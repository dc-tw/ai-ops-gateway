"""Real Redis admission/controller invariants; no external model/provider calls."""

import asyncio
import json

import pytest
import test_planning_api as planning_fixtures
from test_planning_api import path, save, session

from app.core.goal_execution import GoalExecutionStore
from app.core.goal_protocol import LEGACY_QUEUE, WORKFLOW_QUEUE, goal_key
from app.core.workflow_execution import WorkflowExecutionStore, node_id

planning_environment = planning_fixtures.environment


async def test_tenant_capacity_wait_preserves_node_and_requeues_only_its_protocol(environment):
    env = environment
    env.settings.tenant_max_concurrent_runs = 1
    saved, _ = await accepted(env)
    value = await root(env, saved)
    await WorkflowExecutionStore(env.runtime).reconcile(env.tenant, value["goal_id"], env.settings)
    a = await child(env, value, "a")
    assert await env.runtime.acquire_run_slot(env.tenant, "synthetic-existing", 1)

    class NoModel(Router):
        def get(self, selection):
            raise AssertionError("Capacity waiting must not instantiate a model")

    await run_node(env, a, NoModel(ReportModel()))
    waiting = await child(env, value, "a")
    assert waiting["status"] == "queued" and waiting["execution_waiting_capacity"]
    assert waiting["agent_admitted"] and not waiting.get("usage")
    await env.runtime.release_run_slot(env.tenant, "synthetic-existing")
    await GoalExecutionStore(env.runtime).recover_expired()
    await run_node(env, waiting, Router(ReportModel()))
    assert (await child(env, value, "a"))["status"] == "completed"
    assert (await env.runtime.get_goal(value["goal_id"], env.tenant))["status"] == "completed"
    legacy = await env.runtime._client.xrange(LEGACY_QUEUE)
    assert not any(fields.get("tenant_id") == env.tenant for _, fields in legacy)


@pytest.mark.parametrize("interrupted_action", ["pause", "resume"])
async def test_controller_pause_resume_intent_survives_failure_after_root_cas(
    environment, monkeypatch, interrupted_action
):
    env = environment
    saved, _ = await accepted(env)
    value = await root(env, saved)
    controller = WorkflowExecutionStore(env.runtime)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    execution = GoalExecutionStore(env.runtime)
    await execution.control(env.tenant, value["goal_id"], "pause", "operator")
    if interrupted_action == "resume":
        await controller.reconcile(env.tenant, value["goal_id"], env.settings)
        await controller.reconcile(env.tenant, value["goal_id"], env.settings)
        await execution.control(env.tenant, value["goal_id"], "resume", "operator")
    original = GoalExecutionStore.control

    async def interrupted(self, tenant, goal_id, action, subject):
        if goal_id != value["goal_id"] and action == interrupted_action:
            raise RuntimeError("synthetic process exit after root CAS")
        return await original(self, tenant, goal_id, action, subject)

    monkeypatch.setattr(GoalExecutionStore, "control", interrupted)
    with pytest.raises(RuntimeError, match="synthetic process exit"):
        await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    monkeypatch.setattr(GoalExecutionStore, "control", original)
    for _ in range(2):
        await WorkflowExecutionStore(env.runtime).reconcile(
            env.tenant, value["goal_id"], env.settings
        )
    assert (await child(env, value, "a"))["status"] == (
        "paused" if interrupted_action == "pause" else "queued"
    )


async def test_acl_revocation_between_receipt_checks_and_multi_aborts_write_intent(
    environment, monkeypatch
):
    from test_goal_execution import Writes

    from app.automations import AutomationPolicy
    from app.core.execution_policy import PolicyToolExecutor

    env = environment
    writer = await env.agent(policy=AutomationPolicy(allowed_tools=["browser_click"]).model_dump())
    saved, _ = await accepted(env, agent=writer)
    env.settings.resource_acl_enabled = True
    access = f"/v1/access/agent/{writer['agent_id']}"
    response = await env.client.put(
        access,
        headers={"x-api-key": "admin"},
        json={"expected_version": 1, "grants": [{"subject": "operator", "permission": "operate"}]},
    )
    assert response.status_code == 200
    value = await root(env, saved)
    await WorkflowExecutionStore(env.runtime).reconcile(env.tenant, value["goal_id"], env.settings)
    a = await child(env, value, "a")
    assert await GoalExecutionStore(env.runtime).claim(
        a["goal_id"], env.tenant, "race-owner", 60000
    )
    tools = Writes()
    policy = PolicyToolExecutor(
        tools,
        a["policy"],
        runtime=env.runtime,
        tenant_id=env.tenant,
        goal_id=a["goal_id"],
        execution_owner="race-owner",
    )
    original, checks = policy._check_owner, 0

    async def revoke_after_authority_read():
        nonlocal checks
        await original()
        checks += 1
        if checks == 2:
            reply = await env.client.put(
                access, headers={"x-api-key": "admin"}, json={"expected_version": 2, "grants": []}
            )
            assert reply.status_code == 200

    monkeypatch.setattr(policy, "_check_owner", revoke_after_authority_read)
    with pytest.raises(PermissionError):
        await policy.invoke_action("browser_click", {"selector": "#one"}, "raced-action")
    assert checks == 2 and tools.calls == []
    assert not await env.runtime._client.exists(
        f"tenant:{env.tenant}:goal:{a['goal_id']}:tool-receipt:raced-action"
    )


@pytest.mark.parametrize("bad_states", [[], None, "invalid-private-state"])
async def test_ill_typed_workflow_states_quarantine_only_the_corrupt_root(environment, bad_states):
    env = environment
    saved, _ = await accepted(env)
    value = await root(env, saved)
    value["workflow"]["states"] = bad_states
    await env.runtime._client.set(goal_key(env.tenant, value["goal_id"]), json.dumps(value))
    await WorkflowExecutionStore(env.runtime).reconcile_all(env.settings)
    result = await env.runtime.get_goal(value["goal_id"], env.tenant)
    assert result["status"] == "failed" and result["workflow"]["phase"] == "quarantined"


async def test_protocol_namespaces_are_invisible_to_legacy_queue_and_record_readers(environment):
    from uuid import uuid4

    env = environment
    group = "wf-protocol-" + uuid4().hex
    for queue in (LEGACY_QUEUE, WORKFLOW_QUEUE):
        await env.runtime._client.xgroup_create(queue, group, id="$", mkstream=True)
    saved, _ = await accepted(env)
    value = await root(env, saved)
    assert value["workflow_protocol"] == 1 and value["goal_id"].startswith("wfv1-")
    assert (
        await env.runtime._client.get(f"tenant:{env.tenant}:agent:goal:{value['goal_id']}") is None
    )
    assert value["goal_id"] not in await env.runtime._client.smembers(
        f"tenant:{env.tenant}:agent:goals"
    )
    assert (
        await env.runtime.claim_goal(
            group=group, consumer="old", queue=LEGACY_QUEUE, block_seconds=0
        )
        is None
    )
    delivery = await env.runtime.claim_goal(
        group=group, consumer="new", queue=WORKFLOW_QUEUE, block_seconds=0
    )
    assert delivery[1:] == (env.tenant, value["goal_id"])
    await env.runtime.acknowledge_goal(
        delivery[0],
        group=group,
        tenant_id=env.tenant,
        goal_id=value["goal_id"],
        queue=WORKFLOW_QUEUE,
    )
    assert (await env.runtime._client.xpending(WORKFLOW_QUEUE, group))["pending"] == 0
    await WorkflowExecutionStore(env.runtime).reconcile(env.tenant, value["goal_id"], env.settings)
    a = await child(env, value, "a")
    assert a["workflow_protocol"] == 1 and a["goal_id"].startswith("wfv1-")
    assert await env.runtime._client.get(f"tenant:{env.tenant}:agent:goal:{a['goal_id']}") is None


async def test_protocol_missing_marker_and_unversioned_prototype_never_infer(environment):
    from copy import deepcopy
    from uuid import uuid4

    env = environment
    saved, _ = await accepted(env)
    value = await root(env, saved)
    broken = deepcopy(value)
    broken.pop("workflow_protocol")
    await env.runtime._client.set(goal_key(env.tenant, value["goal_id"]), json.dumps(broken))
    await run_node(env, value, Router(ReportModel()))
    assert (await env.runtime.get_goal(value["goal_id"], env.tenant))["workflow"][
        "phase"
    ] == "quarantined"
    prototype = deepcopy(value) | {"goal_id": str(uuid4())}
    prototype.pop("workflow_protocol")
    await env.runtime.save_goal(prototype["goal_id"], prototype, env.tenant)
    await run_node(env, prototype, Router(ReportModel()))
    assert (await env.runtime.get_goal(prototype["goal_id"], env.tenant))["status"] == "failed"


async def test_admission_acl_posture_cannot_be_downgraded_by_worker_or_scheduler(environment):
    from test_goal_execution import Writes

    from app.automations import AutomationPolicy
    from app.core.execution_policy import PolicyToolExecutor

    env = environment
    writer = await env.agent(policy=AutomationPolicy(allowed_tools=["browser_click"]).model_dump())
    saved, _ = await accepted(env, agent=writer)
    env.settings.resource_acl_enabled = True
    access = f"/v1/access/agent/{writer['agent_id']}"
    granted = await env.client.put(
        access,
        headers={"x-api-key": "admin"},
        json={"expected_version": 1, "grants": [{"subject": "operator", "permission": "operate"}]},
    )
    assert granted.status_code == 200, granted.text
    value = await root(env, saved)
    assert value["workflow"]["acl_enforced"] is True
    loose = env.settings.model_copy(update={"resource_acl_enabled": False})
    controller = WorkflowExecutionStore(env.runtime)
    await controller.reconcile(env.tenant, value["goal_id"], loose)
    a = await child(env, value, "a")
    execution = GoalExecutionStore(env.runtime)
    active = await execution.claim(a["goal_id"], env.tenant, "acl-owner", 60000)
    revoked = await env.client.put(
        access, headers={"x-api-key": "admin"}, json={"expected_version": 2, "grants": []}
    )
    assert revoked.status_code == 200, revoked.text
    tools = Writes()
    policy = PolicyToolExecutor(
        tools,
        a["policy"],
        runtime=env.runtime,
        tenant_id=env.tenant,
        goal_id=a["goal_id"],
        execution_owner="acl-owner",
        current_acl=False,
    )
    with pytest.raises(PermissionError):
        await policy.invoke_action("browser_click", {"selector": "#one"}, "exact-action")
    with pytest.raises(PermissionError):
        await execution.reserve_budget(active, "acl-owner", 1000000, current_acl=False)
    assert tools.calls == []
    assert not await env.runtime._client.exists(
        f"tenant:{env.tenant}:goal:{a['goal_id']}:tool-receipt:exact-action"
    )
    active["status"] = "queued"
    assert await execution.finish(active, "acl-owner", {"type": "goal_queued"})
    # Model admission also keeps the captured strict posture, even on a worker
    # whose local ACL setting is accidentally false.
    from app.worker import execute_goal

    class NoModel(Router):
        def get(self, selection):
            raise AssertionError("Revoked ACL must deny before model initialization")

    router = NoModel(ReportModel())
    router.routes = env.app.state.model_router
    await execute_goal(
        a["goal_id"],
        env.runtime,
        router,
        object(),
        tenant_id=env.tenant,
        settings=loose,
        conversation_max_turns=20,
        conversation_ttl_seconds=3600,
    )
    assert (await child(env, value, "a"))["error_type"] == "PermissionError"


@pytest.fixture
async def environment(planning_environment):
    env = planning_environment
    try:
        yield env
    finally:
        # No background workers survive these tests. Delete only this fixture's
        # unique synthetic tenant, never flush the shared disposable server.
        keys = [key async for key in env.runtime._client.scan_iter(f"tenant:{env.tenant}:*")]
        if keys:
            await env.runtime._client.delete(*keys)
        await env.runtime._client.srem("agent:goal:tenants", env.tenant)
        await env.runtime._client.srem("workflow:v1:tenants", env.tenant)


async def run_node(env, value, router, **kwargs):
    from app.core.threat_detection import ThreatDetector
    from app.worker import execute_goal

    if isinstance(router, Router):
        router.routes = env.app.state.model_router

    await execute_goal(
        value["goal_id"],
        env.runtime,
        router,
        object(),
        tenant_id=env.tenant,
        conversation_max_turns=20,
        conversation_ttl_seconds=3600,
        settings=env.settings,
        threat_detector=ThreatDetector.from_settings(env.settings),
        **kwargs,
    )


class ReportModel:
    async def complete(self, system, prompt):
        raise AssertionError("Read-only fixed nodes must not re-plan or delegate")

    async def stream(self, system, prompt):
        yield (
            '{"n":4}'
            if "<OUTPUT_SCHEMA>" in prompt
            else "The supplied evidence is incomplete; further verification remains necessary."
        )


class Router:
    def __init__(self, model):
        self.model = model

    def get(self, selection):
        return self.model

    def planning_route(self, selection):
        return self.routes.planning_route(selection)


async def accepted(env, *, nodes=None, edges=None, agent=None, **changes):
    env.settings.workflow_enabled = True
    env.settings.workflow_execution_enabled = True
    configured = agent or await env.agent()
    fresh = await session(env, configured)
    draft = {
        "schema_version": 2,
        "objective": "Review evidence",
        "nodes": nodes
        or [{"node_id": "a", "agent_id": configured["agent_id"], "query": "Analyze evidence"}],
        "edges": edges or [],
        **changes,
    }
    for node in draft["nodes"]:
        if node.get("agent_id") == "$worker":
            node["agent_id"] = configured["agent_id"]
    saved = await save(env, fresh, draft)
    response = await env.client.post(
        path(saved) + "/mode",
        json={"expected_version": saved["version"], "mode": "execute", "plan_revision": 1},
    )
    assert response.status_code == 200, response.text
    return response.json(), configured


async def admit(env, saved, key="attempt-1", **changes):
    return await env.client.post(
        path(saved) + "/execute",
        json={
            "expected_version": saved["version"],
            "accepted_hash": saved["accepted_hash"],
            "idempotency_key": key,
            **changes,
        },
    )


async def root(env, saved):
    response = await admit(env, saved)
    assert response.status_code == 200, response.text
    return response.json()["goal"]


async def child(env, root, name):
    return await env.runtime.get_goal(node_id(env.tenant, root["goal_id"], name), env.tenant)


async def complete(env, value, *, output=None, usage=100, status="completed"):
    execution = GoalExecutionStore(env.runtime)
    claimed = await execution.claim(value["goal_id"], env.tenant, "test-worker", 60000)
    assert claimed is not None
    await execution.reserve_budget(
        claimed,
        "test-worker",
        env.settings.tenant_daily_token_budget,
        {"policy_charged_tokens": usage},
    )
    claimed["usage"] = {"policy_charged_tokens": usage}
    claimed["workflow_output"] = {"output": output or {}, "output_validated": True}
    claimed["status"] = status
    assert await execution.finish(claimed, "test-worker", {"type": "goal_completed"})
    return claimed


async def test_admission_is_explicit_atomic_and_concurrent_retries_return_one_goal(environment):
    env = environment
    saved, _ = await accepted(env)
    assert await env.runtime.list_goal_ids(env.tenant) == []
    env.settings.workflow_execution_enabled = False
    assert (await admit(env, saved)).status_code == 409
    env.settings.workflow_execution_enabled = True
    replies = await asyncio.gather(*(admit(env, saved) for _ in range(4)))
    assert [r.status_code for r in replies] == [200] * 4, [r.text for r in replies]
    roots = [r.json()["goal"] for r in replies]
    assert len({g["goal_id"] for g in roots}) == 1
    assert await env.runtime.list_goal_ids(env.tenant) == [roots[0]["goal_id"]]
    current = (await env.client.get(path(saved))).json()
    assert current["active_goal_id"] == roots[0]["goal_id"]
    quota = roots[0]["token_reservation"]
    assert (
        int(await env.runtime._client.get(f"tenant:{env.tenant}:quota:tokens:{quota['day']}"))
        == 20000
    )
    queue = await env.runtime._client.xrange(WORKFLOW_QUEUE)
    assert len([fields for _, fields in queue if fields.get("tenant_id") == env.tenant]) == 1
    assert (await admit(env, saved, accepted_hash="f" * 64)).status_code == 409
    assert (await admit(env, saved, key="different-attempt")).status_code == 409


async def test_admission_stale_epoch_atr_quota_and_wrongtype_leave_no_partial_goal(environment):
    env = environment
    saved, agent = await accepted(env)
    env.settings.tenant_daily_token_budget = 1000
    assert (await admit(env, saved)).status_code == 409
    assert await env.runtime.list_goal_ids(env.tenant) == []
    env.settings.tenant_daily_token_budget = 1000000
    env.detector.blocked = True
    assert (await admit(env, saved)).status_code == 403
    env.detector.blocked = False
    key = f"tenant:{env.tenant}:agents:record:{agent['agent_id']}"
    changed = json.loads(await env.runtime._client.get(key))
    changed["version"] += 1
    await env.runtime._client.set(key, json.dumps(changed))
    assert (await admit(env, saved)).status_code == 409
    assert await env.runtime.list_goal_ids(env.tenant) == []
    changed["version"] -= 1
    await env.runtime._client.set(key, json.dumps(changed))
    index = f"tenant:{env.tenant}:workflow:v1:goals"
    await env.runtime._client.set(index, "WRONGTYPE")
    assert (await admit(env, saved)).status_code == 503
    deterministic = WorkflowExecutionStore.root_id(env.tenant, saved["session_id"], "attempt-1")
    assert await env.runtime.get_goal(deterministic, env.tenant) is None
    await env.runtime._client.delete(index)
    result = await root(env, saved)
    assert result["status"] == "queued"


async def test_controller_replicas_enqueue_each_node_once_and_reconcile_shared_budget(environment):
    env = environment
    saved, _ = await accepted(
        env,
        nodes=[
            {
                "node_id": "a",
                "agent_id": "$worker",
                "query": "First",
                "output_schema": {"type": "object"},
            },
            {
                "node_id": "b",
                "agent_id": "$worker",
                "query": "Second",
                "inputs": {"source": {"node_id": "a", "path": "/n"}},
            },
        ],
        edges=[{"source": "a", "target": "b"}],
    )
    value = await root(env, saved)
    controller = WorkflowExecutionStore(env.runtime)
    await asyncio.gather(
        *(controller.reconcile(env.tenant, value["goal_id"], env.settings) for _ in range(3))
    )
    a = await child(env, value, "a")
    assert a is not None and await child(env, value, "b") is None
    assert a["agent_config"]["version"] == 1
    await complete(env, a, output={"n": 4}, usage=101)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    b = await child(env, value, "b")
    assert '"source":4' in b["steps"][0]["query"]
    await complete(env, b, usage=203)
    for _ in range(3):
        await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    finished = await env.runtime.get_goal(value["goal_id"], env.tenant)
    assert finished["status"] == "completed"
    assert finished["usage"]["policy_charged_tokens"] == 304
    assert all("output" not in state for state in finished["workflow"]["states"].values())
    reservation = finished["token_reservation"]
    assert reservation["reconciled"] is True
    assert (
        int(await env.runtime._client.get(f"tenant:{env.tenant}:quota:tokens:{reservation['day']}"))
        == 304
    )
    queue = await env.runtime._client.xrange(WORKFLOW_QUEUE)
    ids = [fields["goal_id"] for _, fields in queue if fields.get("tenant_id") == env.tenant]
    assert len(ids) == len(set(ids)) == 3


async def test_same_agent_execution_lease_serializes_goals_and_approval_releases_it(environment):
    env = environment
    agent = await env.agent()
    execution = GoalExecutionStore(env.runtime)
    original = {
        "tenant_id": env.tenant,
        "created_by": "operator",
        "status": "queued",
        "objective": "test",
        "steps": [],
        "agent_id": agent["agent_id"],
        "agent_config": agent,
        "policy": agent["policy"],
    }
    for name in ("one", "two"):
        await execution.submit_manual(original | {"goal_id": name})
    first = await execution.claim("one", env.tenant, "owner-one", 60000)
    assert first
    assert await execution.claim("two", env.tenant, "owner-two", 60000) is None
    waiting = await env.runtime.get_goal("two", env.tenant)
    assert waiting["execution_waiting_agent"] and waiting["status"] == "queued"
    first["status"] = "waiting_approval"
    assert await execution.finish(first, "owner-one", {"type": "goal_waiting_approval"})
    await execution.recover_expired()
    second = await execution.claim("two", env.tenant, "owner-two", 60000)
    assert second
    assert not await execution.save_owned(first, "owner-one")
    slot = json.loads(
        await env.runtime._client.get(f"tenant:{env.tenant}:agents:execution:{agent['agent_id']}")
    )
    assert slot["goal_id"] == "two"


async def test_required_admission_failure_does_not_enqueue_other_ready_nodes(environment):
    env = environment
    saved, agent = await accepted(
        env,
        nodes=[
            {"node_id": "a", "agent_id": "$worker", "query": "First"},
            {"node_id": "b", "agent_id": "$worker", "query": "Second"},
        ],
    )
    value = await root(env, saved)
    key = f"tenant:{env.tenant}:agents:record:{agent['agent_id']}"
    changed = json.loads(await env.runtime._client.get(key))
    changed["enabled"] = False
    await env.runtime._client.set(key, json.dumps(changed))
    controller = WorkflowExecutionStore(env.runtime)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    current = await env.runtime.get_goal(value["goal_id"], env.tenant)
    assert current["status"] == "failed"
    assert await child(env, value, "a") is None
    assert await child(env, value, "b") is None
    assert current["token_reservation"]["reconciled"]


async def test_required_failure_cancels_active_sibling_and_restarts_safely(environment):
    env = environment
    worker_b = await env.agent()
    saved, _ = await accepted(
        env, nodes=[{"node_id": "a", "agent_id": "$worker", "query": "First"}]
    )
    # Use a Chief snapshot to authorize the second Worker.
    worker_a = saved["snapshot"]["root_agent"]["agent_id"]
    chief = await env.agent(role="chief", delegate_agent_ids=[worker_a, worker_b["agent_id"]])
    fresh = await session(env, chief)
    saved = await save(
        env,
        fresh,
        {
            "schema_version": 2,
            "objective": "Parallel",
            "nodes": [
                {"node_id": "a", "agent_id": worker_a, "query": "First"},
                {"node_id": "b", "agent_id": worker_b["agent_id"], "query": "Second"},
            ],
        },
    )
    saved = (
        await env.client.post(
            path(saved) + "/mode",
            json={"expected_version": saved["version"], "mode": "execute", "plan_revision": 1},
        )
    ).json()
    value = await root(env, saved)
    controller = WorkflowExecutionStore(env.runtime)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    a, b = await child(env, value, "a"), await child(env, value, "b")
    execution = GoalExecutionStore(env.runtime)
    running = await execution.claim(b["goal_id"], env.tenant, "slow", 60000)
    await execution.reserve_budget(
        running, "slow", env.settings.tenant_daily_token_budget, {"policy_charged_tokens": 37}
    )
    await complete(env, a, status="failed", usage=17)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    assert (await child(env, value, "b"))["status"] == "cancelled"
    assert not await execution.save_owned(running, "slow")
    # New controller instance simulates a process restart between cancellation
    # and final aggregate accounting.
    await WorkflowExecutionStore(env.runtime).reconcile(env.tenant, value["goal_id"], env.settings)
    current = await env.runtime.get_goal(value["goal_id"], env.tenant)
    assert current["status"] == "failed" and current["usage"]["policy_charged_tokens"] == 54
    assert current["token_reservation"]["reconciled"]


async def test_actual_worker_validates_short_json_before_typed_dag_input(environment):
    env = environment
    saved, _ = await accepted(
        env,
        nodes=[
            {
                "node_id": "a",
                "agent_id": "$worker",
                "query": "Classify",
                "token_budget": 8000,
                "output_schema": {
                    "type": "object",
                    "properties": {"n": {"type": "integer"}},
                    "required": ["n"],
                },
            },
            {
                "node_id": "b",
                "agent_id": "$worker",
                "query": "Summarize",
                "token_budget": 8000,
                "inputs": {"classified": {"node_id": "a", "path": "/n"}},
                "condition": {"op": "eq", "node_id": "a", "path": "/n", "value": 4},
            },
        ],
        edges=[{"source": "a", "target": "b"}],
    )
    value = await root(env, saved)
    router = Router(ReportModel())
    await run_node(env, value, router)  # Root is controller-only, not an inference call.
    await run_node(env, await child(env, value, "a"), router)
    a = await child(env, value, "a")
    assert a["status"] == "completed", a.get("error")
    assert a["workflow_output"] == {"output": {"n": 4}, "output_validated": True}
    b = await child(env, value, "b")
    assert b and '"classified":4' in b["steps"][0]["query"]
    await run_node(env, b, router)
    current = await env.runtime.get_goal(value["goal_id"], env.tenant)
    assert current["status"] == "completed", current
    assert current["usage"]["policy_charged_tokens"] > 0
    assert current["token_reservation"]["reconciled"]
    assert "token_reservation" not in a  # No duplicate child reservation.


async def test_root_pause_resume_and_cancel_recover_across_controller_instances(environment):
    env = environment
    saved, _ = await accepted(env)
    value = await root(env, saved)
    controller = WorkflowExecutionStore(env.runtime)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    execution = GoalExecutionStore(env.runtime)
    await execution.control(env.tenant, value["goal_id"], "pause", "operator")
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    assert (await child(env, value, "a"))["status"] == "paused"
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    assert (await env.runtime.get_goal(value["goal_id"], env.tenant))["status"] == "paused"
    await execution.control(env.tenant, value["goal_id"], "resume", "operator")
    await WorkflowExecutionStore(env.runtime).reconcile(env.tenant, value["goal_id"], env.settings)
    assert (await child(env, value, "a"))["status"] == "queued"
    await execution.control(env.tenant, value["goal_id"], "cancel", "operator")
    await WorkflowExecutionStore(env.runtime).reconcile(env.tenant, value["goal_id"], env.settings)
    assert (await child(env, value, "a"))["status"] == "cancelled"


async def test_uncertain_receipt_is_recorded_and_never_replayed(environment):
    env = environment
    saved, _ = await accepted(env)
    value = await root(env, saved)
    controller = WorkflowExecutionStore(env.runtime)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    a = await child(env, value, "a")
    key = f"tenant:{env.tenant}:goal:{a['goal_id']}:tool-receipt:uncertain-action"
    await env.runtime._client.set(key, json.dumps({"status": "started", "tool": "browser_click"}))
    await env.runtime._client.sadd(f"tenant:{env.tenant}:goal:{a['goal_id']}:tool-receipts", key)
    await complete(env, a, status="failed", usage=13)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    current = await env.runtime.get_goal(value["goal_id"], env.tenant)
    record = current["workflow"]["receipt_reconciliation"]
    assert record["status"] == "review_required" and record["automatic_replay"] is False
    assert record["uncertain"][0]["goal_id"] == a["goal_id"]
    assert record["uncertain"][0]["count"] == 1
    assert json.loads(await env.runtime._client.get(key))["status"] == "started"
    receipts = await env.client.get(f"/v1/goals/{a['goal_id']}/receipts?limit=1")
    assert receipts.status_code == 200, receipts.text
    assert receipts.json()["receipts"] == [
        {
            "action_id": "uncertain-action",
            "tool": "browser_click",
            "status": "started",
            "review_required": True,
        }
    ]
    assert receipts.json()["automatic_replay"] is False
    assert (
        await env.client.get(f"/v1/goals/{a['goal_id']}/receipts", headers={"x-api-key": "foreign"})
    ).status_code == 404


async def test_durable_exact_approval_does_not_block_independent_branch(environment):
    import os

    from langgraph.checkpoint.redis.aio import AsyncRedisSaver
    from test_goal_execution import Writes

    from app.automations import AutomationPolicy

    env = environment
    env.settings.workflow_enabled = env.settings.workflow_execution_enabled = True
    env.settings.llm_max_output_tokens = 512
    writer = await env.agent(policy=AutomationPolicy(allowed_tools=["browser_click"]).model_dump())
    analyst = await env.agent()
    chief = await env.agent(
        role="chief", delegate_agent_ids=[writer["agent_id"], analyst["agent_id"]]
    )
    saved = await save(
        env,
        await session(env, chief),
        {
            "schema_version": 2,
            "objective": "Review and act",
            "nodes": [
                {
                    "node_id": "write",
                    "agent_id": writer["agent_id"],
                    "query": "Click the requested button",
                    "token_budget": 15000,
                },
                {
                    "node_id": "independent",
                    "agent_id": analyst["agent_id"],
                    "query": "Review the supplied context",
                    "token_budget": 5000,
                },
            ],
        },
    )
    saved = (
        await env.client.post(
            path(saved) + "/mode",
            json={"expected_version": saved["version"], "mode": "execute", "plan_revision": 1},
        )
    ).json()
    value = await root(env, saved)
    controller = WorkflowExecutionStore(env.runtime)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    tools = Writes()

    class OneWrite:
        async def complete(self, system, prompt):
            if '"selector": "#approved"' in prompt:
                return '{"tool":null,"arguments":{}}'
            return '{"tool":"browser_click","arguments":{"selector":"#approved"}}'

        async def stream(self, system, prompt):
            yield "The independently approved browser operation completed successfully."

    async with AsyncRedisSaver(redis_url=os.environ["TEST_REDIS_URL"]) as saver:
        await run_node(
            env,
            await child(env, value, "write"),
            Router(OneWrite()),
            tool_executor=tools,
            checkpointer=saver,
        )
    waiting = await child(env, value, "write")
    assert waiting["status"] == "waiting_approval", waiting.get("error")
    assert tools.calls == []
    assert await env.runtime._client.zcard(f"tenant:{env.tenant}:quota:active-runs") == 0
    await run_node(env, await child(env, value, "independent"), Router(ReportModel()))
    assert (await child(env, value, "independent"))["status"] == "completed"
    assert (await child(env, value, "write"))["status"] == "waiting_approval"
    pending = waiting["pending_approval"]
    execution = GoalExecutionStore(env.runtime)
    await execution.decide_approval(
        env.tenant,
        waiting["goal_id"],
        pending["approval_id"],
        True,
        "checked exact selector",
        "operator",
    )
    # New model and Redis checkpoint connection: no process-local state carries approval.
    async with AsyncRedisSaver(redis_url=os.environ["TEST_REDIS_URL"]) as saver:
        await run_node(
            env,
            await child(env, value, "write"),
            Router(OneWrite()),
            tool_executor=tools,
            checkpointer=saver,
        )
    completed = await child(env, value, "write")
    assert completed["status"] == "completed", completed.get("error")
    assert tools.calls == [("browser_click", {"selector": "#approved"})]
    current = await env.runtime.get_goal(value["goal_id"], env.tenant)
    assert current["status"] == "completed"
    assert current["workflow"]["receipt_reconciliation"]["total"] == 1
    assert current["workflow"]["receipt_reconciliation"]["status"] == "recorded"


async def test_team_reservation_crossing_midnight_corrects_both_days(environment, monkeypatch):
    from datetime import UTC, datetime

    import app.core.workflow_execution as workflow_module

    class Day(datetime):
        current = 1

        @classmethod
        def now(cls, tz=None):
            return datetime(2040, 2, cls.current, tzinfo=UTC)

    monkeypatch.setattr(workflow_module, "datetime", Day)
    env = environment
    saved, _ = await accepted(env)
    value = await root(env, saved)
    controller = WorkflowExecutionStore(env.runtime)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    a = await child(env, value, "a")
    execution = GoalExecutionStore(env.runtime)
    active = await execution.claim(a["goal_id"], env.tenant, "cross-midnight", 60000)
    await execution.reserve_budget(
        active, "cross-midnight", 1000000, {"policy_charged_tokens": 400}
    )
    Day.current = 2
    await execution.reserve_budget(
        active, "cross-midnight", 1000000, {"policy_charged_tokens": 100}
    )
    first, second = (f"tenant:{env.tenant}:quota:tokens:2040-02-0{n}" for n in (1, 2))
    assert int(await env.runtime._client.get(first)) == 100
    assert int(await env.runtime._client.get(second)) == 19900
    await execution.reserve_budget(
        active, "cross-midnight", 1000000, {"policy_charged_tokens": 450}
    )
    active.update(
        status="completed",
        usage={"policy_charged_tokens": 450},
        workflow_output={"output": "done", "output_validated": False},
    )
    assert await execution.finish(active, "cross-midnight", {"type": "goal_completed"})
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    assert int(await env.runtime._client.get(first)) == 100
    assert int(await env.runtime._client.get(second)) == 350


async def test_current_acl_required_at_admission_and_revocation_stops_root(environment):
    env = environment
    saved, agent = await accepted(env)
    env.settings.resource_acl_enabled = True
    assert (await admit(env, saved)).status_code == 403  # Agent belongs to admin, not operator.
    access = f"/v1/access/agent/{agent['agent_id']}"
    grant = await env.client.put(
        access,
        headers={"x-api-key": "admin"},
        json={"expected_version": 1, "grants": [{"subject": "operator", "permission": "operate"}]},
    )
    assert grant.status_code == 200, grant.text
    value = await root(env, saved)
    controller = WorkflowExecutionStore(env.runtime)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    plan_access = f"/v1/access/planning_session/{saved['session_id']}"
    # The ACL API only permits owner/admin updates; revoke the resource using
    # its server authority record to model the existing revoke management path.
    from app.core.resource_access import acl_key

    key = acl_key(env.tenant, "planning_session", saved["session_id"])
    record = json.loads(await env.runtime._client.get(key))
    record.update(revoked=True, version=record["version"] + 1)
    await env.runtime._client.set(key, json.dumps(record))
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    assert (await child(env, value, "a"))["status"] == "cancelled"
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    assert (await env.runtime.get_goal(value["goal_id"], env.tenant))["status"] == "failed"
    assert (await env.client.get(plan_access)).status_code == 200  # Owner may inspect governance.


async def test_cancelled_root_cannot_resume_a_stale_child_approval(environment):
    from datetime import UTC, datetime, timedelta

    env = environment
    saved, _ = await accepted(env)
    value = await root(env, saved)
    controller = WorkflowExecutionStore(env.runtime)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    a = await child(env, value, "a")
    execution = GoalExecutionStore(env.runtime)
    active = await execution.claim(a["goal_id"], env.tenant, "approval-worker", 60000)
    active.update(
        status="waiting_approval",
        pending_approval={
            "approval_id": "exact-approval",
            "action_id": "one-action",
            "tool": "browser_click",
            "arguments": {"selector": "#one"},
            "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        },
    )
    assert await execution.finish(active, "approval-worker", {"type": "goal_waiting_approval"})
    await execution.control(env.tenant, value["goal_id"], "cancel", "operator")
    decision = await execution.decide_approval(
        env.tenant, a["goal_id"], "exact-approval", True, "", "operator"
    )
    assert decision["status"] == "cancelled"
    assert decision["approval_decision"]["approved"] is False
    assert decision["approval_decision"]["reason"] == "parent_stopped"


async def test_worker_rejects_local_route_drift_before_initializing_a_model(environment):
    env = environment
    saved, _ = await accepted(env)
    value = await root(env, saved)
    controller = WorkflowExecutionStore(env.runtime)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)

    class ChangedRoute(Router):
        def planning_route(self, selection):
            value = super().planning_route(selection)
            return value | {"route_fingerprint": "sha256:" + "f" * 64}

        def get(self, selection):
            raise AssertionError("Route drift must be rejected before constructing a model client")

    await run_node(env, await child(env, value, "a"), ChangedRoute(ReportModel()))
    a = await child(env, value, "a")
    assert a["status"] == "failed" and a["error_type"] == "PermissionError"
    assert not a.get("usage")


async def test_receipt_replay_revalidates_compiled_integrity_without_reenqueue(environment):
    env = environment
    saved, _ = await accepted(env)
    value = await root(env, saved)
    value["workflow"]["compiled"]["nodes"][0]["query"] = "PRIVATE_CORRUPT_COMPILED"
    await env.runtime._client.set(goal_key(env.tenant, value["goal_id"]), json.dumps(value))
    response = await admit(env, saved)
    assert response.status_code == 409
    assert "PRIVATE_CORRUPT_COMPILED" not in response.text
    queue = await env.runtime._client.xrange(WORKFLOW_QUEUE)
    assert len([fields for _, fields in queue if fields.get("tenant_id") == env.tenant]) == 1
    healthy_saved, _ = await accepted(env)
    healthy = await root(env, healthy_saved)
    controller = WorkflowExecutionStore(env.runtime)
    await controller.reconcile_all(env.settings)
    rejected = await env.runtime.get_goal(value["goal_id"], env.tenant)
    assert rejected["status"] == "failed"
    assert rejected["workflow"]["phase"] == "quarantined"
    assert rejected["token_reservation"]["reconciled"]
    assert (await child(env, healthy, "a"))["status"] == "queued"


async def test_return_to_plan_waits_for_cancellation_accounting_then_allows_explicit_new_run(
    environment,
):
    env = environment
    saved, _ = await accepted(env)
    value = await root(env, saved)
    controller = WorkflowExecutionStore(env.runtime)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    await GoalExecutionStore(env.runtime).control(
        env.tenant, value["goal_id"], "cancel", "operator"
    )
    current = (await env.client.get(path(saved))).json()
    payload = {"expected_version": current["version"], "mode": "plan"}
    assert (await env.client.post(path(saved) + "/mode", json=payload)).status_code == 409
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    await controller.reconcile(env.tenant, value["goal_id"], env.settings)
    response = await env.client.post(path(saved) + "/mode", json=payload)
    assert response.status_code == 200, response.text
    assert response.json()["active_goal_id"] is None
    reaccepted = await env.client.post(
        path(saved) + "/mode",
        json={
            "expected_version": response.json()["version"],
            "mode": "execute",
            "plan_revision": 1,
        },
    )
    assert reaccepted.status_code == 200, reaccepted.text
    second = await admit(env, reaccepted.json(), key="explicit-new-attempt")
    assert second.status_code == 200, second.text
    assert second.json()["goal"]["goal_id"] != value["goal_id"]
