"""Planning APIs preserve trusted configuration and immutable drafts without execution."""

import asyncio
import hashlib
import json
import os
import re
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from app.agents import AgentStore
from app.automations import AutomationPolicy
from app.core.config import Settings
from app.core.redis_runtime import RedisRuntime
from app.core.skill_store import SkillContent, SkillStore
from app.core.threat_detection import ThreatScan
from app.llm.client import ModelRouter
from app.main import create_app


class Detector:
    def __init__(self):
        self.scans = []
        self.blocked = False
        self.findings = ()

    async def ascan(self, text, **kwargs):
        self.scans.append((text, kwargs))
        return ThreatScan(kwargs["event_type"], kwargs["location"], self.findings, self.blocked)


def digest(value):
    canonical = json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def plan(**changes):
    return {
        "objective": "Review the supplied incident evidence",
        "context": "Only the supplied source material is in scope.",
        "steps": [{"query": "Summarize the evidence and its limitations", "target_url": None}],
        "assignments": [],
        "max_retries": 2,
        "max_tool_rounds": 8,
        "step_deadline_seconds": 300.0,
        **changes,
    }


@pytest.fixture
async def environment():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Planning APIs require TEST_REDIS_URL pointing to a dedicated Redis server")
    runtime = RedisRuntime(url)
    await runtime.ping()
    tenant, foreign = f"planning-{uuid4().hex}", f"foreign-{uuid4().hex}"
    keys = {
        key: {"subject": key, "tenant_id": scope, "role": role}
        for key, scope, role in (
            ("admin", tenant, "admin"),
            ("operator", tenant, "operator"),
            ("viewer", tenant, "viewer"),
            ("foreign", foreign, "admin"),
        )
    }
    configured = Settings(
        api_keys_json=json.dumps(keys),
        pii_redaction=False,
        tenant_requests_per_minute=10_000,
        llm_api_key="PRIVATE_PLANNING_API_KEY_SENTINEL",
    )
    app = create_app(settings=configured, redis_runtime=runtime)
    detector = Detector()
    app.state.settings, app.state.redis = configured, runtime
    app.state.model_router = ModelRouter(configured)
    app.state.threat_detector = detector

    async def agent(**changes):
        now = datetime.now(UTC).isoformat()
        value = {
            "agent_id": str(uuid4()),
            "tenant_id": tenant,
            "name": "Planning analyst",
            "description": "Reviews provided incident evidence",
            "persona": "Use evidence, distinguish uncertainty, and preserve approval boundaries.",
            "role": "worker",
            "model": configured.llm_model,
            "enabled": True,
            "version": 1,
            "delegate_agent_ids": [],
            "skills": [],
            "policy": AutomationPolicy(allowed_tools=[]).model_dump(),
            "created_by": "admin",
            "created_at": now,
            "updated_at": now,
            **changes,
        }
        return await AgentStore(runtime).create(value)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"x-api-key": "operator"},
    ) as client:
        yield SimpleNamespace(
            runtime=runtime,
            app=app,
            settings=configured,
            tenant=tenant,
            foreign=foreign,
            client=client,
            agent=agent,
            detector=detector,
        )
    await runtime.close()


async def session(env, agent=None):
    configured = agent or await env.agent()
    response = await env.client.post(f"/v1/agents/{configured['agent_id']}/planning-sessions")
    assert response.status_code == 200, response.text
    return response.json()


def path(value):
    return f"/v1/planning-sessions/{value['session_id']}"


async def save(env, value, draft=None):
    response = await env.client.post(
        path(value) + "/plans",
        json={"expected_version": value["version"], "plan": draft or plan()},
    )
    assert response.status_code == 200, response.text
    return response.json()


async def test_workflow_flag_yaml_compiled_artifact_and_no_dispatch(environment, monkeypatch):
    env = environment
    configured = await env.agent()
    fresh = await session(env, configured)
    draft = {
        "schema_version": 2,
        "objective": "分析部署證據",
        "nodes": [{"node_id": "review", "agent_id": configured["agent_id"], "query": "只讀分析"}],
    }
    body = {"expected_version": fresh["version"], "plan": draft}
    assert (await env.client.post(path(fresh) + "/plans", json=body)).status_code == 409
    env.settings.workflow_enabled = True

    def forbidden(*args, **kwargs):
        pytest.fail("Compilation must never infer, dispatch, or reserve execution")

    monkeypatch.setattr(env.app.state.model_router, "get", forbidden)
    monkeypatch.setattr(env.runtime, "consume_daily_tokens", forbidden)
    monkeypatch.setattr(env.runtime, "acquire_run_slot", forbidden)
    import yaml

    response = await env.client.post(
        path(fresh) + "/workflow-yaml",
        json={
            "expected_version": fresh["version"],
            "yaml": yaml.safe_dump(draft, allow_unicode=True),
        },
    )
    assert response.status_code == 200, response.text
    saved = response.json()
    artifact = (await env.client.get(path(saved) + "/plans/1")).json()
    view = await env.client.get(path(saved) + "/compiled", headers={"x-api-key": "viewer"})
    assert view.status_code == 200, view.text
    assert view.json()["compiled"] == artifact["compiled"]
    assert view.json()["source_kind"] == "native"
    assert view.json()["execution_started"] is False
    assert (
        await env.client.get(path(saved) + "/compiled", headers={"x-api-key": "foreign"})
    ).status_code == 404
    assert (
        await env.client.post(
            path(saved) + "/workflow-yaml",
            json={"expected_version": saved["version"], "yaml": yaml.safe_dump(draft)},
            headers={"x-api-key": "viewer"},
        )
    ).status_code == 403
    accepted = await env.client.post(
        path(saved) + "/mode",
        json={"expected_version": saved["version"], "mode": "execute", "plan_revision": 1},
    )
    assert accepted.status_code == 200, accepted.text
    assert await env.runtime.list_goal_ids(env.tenant) == []
    assert (
        await env.runtime._client.get(env.runtime._tenant_key(env.tenant, "quota:active-runs"))
        is None
    )
    env.settings.workflow_enabled = False
    assert (await env.client.get(path(saved) + "/compiled")).status_code == 200


async def test_legacy_compile_is_explicit_and_does_not_rewrite_revision(environment):
    env = environment
    saved = await save(env, await session(env))
    before = (await env.client.get(path(saved) + "/plans/1")).json()
    assert (await env.client.get(path(saved) + "/compiled")).status_code == 409
    env.settings.workflow_enabled = True
    response = await env.client.get(path(saved) + "/compiled")
    assert response.status_code == 200, response.text
    assert response.json()["source_kind"] == "legacy_adapter"
    assert (await env.client.get(path(saved) + "/plans/1")).json() == before
    assert await env.runtime.list_goal_ids(env.tenant) == []


async def test_workflow_literals_scanned_limits_and_compiled_tamper_fails_closed(environment):
    env = environment
    env.settings.workflow_enabled = True
    configured = await env.agent()
    fresh = await session(env, configured)
    draft = {
        "schema_version": 2,
        "objective": "Review",
        "nodes": [
            {
                "node_id": "a",
                "agent_id": configured["agent_id"],
                "query": "Evidence",
                "output_schema": {"type": "object", "description": "SCHEMA_LITERAL\nSECOND_LINE"},
            }
        ],
    }
    saved = await save(env, fresh, draft)
    assert any("SCHEMA_LITERAL\nSECOND_LINE" in text for text, _ in env.detector.scans)
    env.settings.workflow_max_edges = 0
    draft["nodes"].append({"node_id": "b", "agent_id": configured["agent_id"], "query": "Next"})
    draft["edges"] = [{"source": "a", "target": "b"}]
    rejected = await env.client.post(
        path(saved) + "/plans", json={"expected_version": saved["version"], "plan": draft}
    )
    assert rejected.status_code == 422
    assert (await env.client.get(path(saved))).json()["plan_revision"] == 1
    from app.core.planning_store import PlanningStore

    store = PlanningStore(env.runtime)
    key = store.revision_key(env.tenant, saved["session_id"], 1)
    record = json.loads(await env.runtime._client.get(key))
    record["compiled"]["nodes"][0]["query"] = "PRIVATE_TAMPER"
    await env.runtime._client.set(key, json.dumps(record))
    for suffix in ("", "/compiled", "/plans/1"):
        response = await env.client.get(path(saved) + suffix)
        assert response.status_code == 409
        assert "PRIVATE_TAMPER" not in response.text


async def test_workflow_compiled_reads_recheck_current_owner_sharing(environment):
    env = environment
    env.settings.workflow_enabled = True
    configured = await env.agent()
    fresh = await session(env, configured)
    saved = await save(
        env,
        fresh,
        {
            "schema_version": 2,
            "objective": "Review",
            "nodes": [{"node_id": "a", "agent_id": configured["agent_id"], "query": "Evidence"}],
        },
    )
    env.settings.resource_acl_enabled = True
    url = path(saved) + "/compiled"
    assert (await env.client.get(url)).status_code == 200
    assert (await env.client.get(url, headers={"x-api-key": "viewer"})).status_code == 404
    access = f"/v1/access/planning_session/{saved['session_id']}"
    granted = await env.client.put(
        access,
        json={
            "expected_version": 1,
            "grants": [{"actor_type": "human", "subject": "viewer", "permission": "read"}],
        },
    )
    assert granted.status_code == 200, granted.text
    assert (await env.client.get(url, headers={"x-api-key": "viewer"})).status_code == 200
    revoked = await env.client.put(access, json={"expected_version": 2, "grants": []})
    assert revoked.status_code == 200, revoked.text
    assert (await env.client.get(url, headers={"x-api-key": "viewer"})).status_code == 404


async def test_fresh_sessions_and_immutable_revisions_never_execute(environment, monkeypatch):
    env = environment

    def forbidden(*args, **kwargs):
        pytest.fail("Planning attempted to initialize a model, invoke tools, or reserve execution")

    monkeypatch.setattr(env.app.state.model_router, "get", forbidden)
    monkeypatch.setattr(env.runtime, "consume_daily_tokens", forbidden)
    monkeypatch.setattr(env.runtime, "acquire_run_slot", forbidden)
    fresh = await session(env)
    assert fresh["mode"] == "plan" and fresh["version"] == 1 and fresh["plan_revision"] == 0
    snapshot = fresh["snapshot"]
    assert snapshot["tenant_id"] == env.tenant
    assert snapshot["root_agent"]["model"] == env.settings.llm_model
    assert (
        snapshot["root_agent"]["model_route"]["version_disclosure"]["actual_model_version"]
        == "unknown"
    )
    assert "PRIVATE_PLANNING_API_KEY_SENTINEL" not in json.dumps(fresh)
    fetched = await env.client.get(path(fresh), headers={"x-api-key": "viewer"})
    assert fetched.json()["configuration_status"] == "current"
    first = await save(env, fresh)
    assert first["version"] == 2 and first["plan_revision"] == 1
    original = (await env.client.get(path(first) + "/plans/1")).json()
    assert original["revision"] == 1 and original["plan"] == plan()
    assert original["snapshot"] == snapshot
    assert original["snapshot_sha256"] == digest(snapshot)
    assert original["content_sha256"] == digest(
        {"schema_version": 1, "plan": plan(), "complete": True, "snapshot": snapshot}
    )
    second = await save(env, first, plan(objective="Prepare a handoff summary"))
    assert second["version"] == 3 and second["plan_revision"] == 2
    assert (await env.client.get(path(second) + "/plans/1")).json() == original
    page = (await env.client.get(path(second) + "/plans?limit=1")).json()
    assert [item["revision"] for item in page["plans"]] == [1]
    assert page["next_after"] == 1
    final_page = (await env.client.get(path(second) + "/plans?after=1&limit=1")).json()
    assert [item["revision"] for item in final_page["plans"]] == [2]
    assert final_page["next_after"] is None
    assert await env.runtime.list_goal_ids(env.tenant) == []
    queue = await env.runtime._client.xrange("agent:goal:queue")
    assert all(fields.get("tenant_id") != env.tenant for _, fields in queue)
    for suffix in ("quota:tokens:*", "quota:active-runs", "agents:daily-runs:*"):
        assert [
            key async for key in env.runtime._client.scan_iter(f"tenant:{env.tenant}:{suffix}")
        ] == []


async def test_roles_tenant_isolation_and_anonymous_access(environment):
    env, configured = environment, await environment.agent()
    create = f"/v1/agents/{configured['agent_id']}/planning-sessions"
    assert (
        await env.client.post(create, json={}, headers={"x-api-key": "viewer"})
    ).status_code == 403
    assert (
        await env.client.post(create, json={}, headers={"x-api-key": "foreign"})
    ).status_code == 404
    assert (
        await env.client.post(create, json={}, headers={"x-api-key": "invalid"})
    ).status_code == 401
    current = await session(env, configured)
    saved = await save(env, current)
    for suffix in ("", "/plans", "/plans/1"):
        assert (
            await env.client.get(path(saved) + suffix, headers={"x-api-key": "viewer"})
        ).status_code == 200
        assert (
            await env.client.get(path(saved) + suffix, headers={"x-api-key": "foreign"})
        ).status_code == 404
    for suffix, body in (
        ("/plans", {"expected_version": saved["version"], "plan": plan()}),
        ("/refresh", {"expected_version": saved["version"]}),
        ("/mode", {"expected_version": saved["version"], "mode": "execute", "plan_revision": 1}),
    ):
        assert (
            await env.client.post(path(saved) + suffix, json=body, headers={"x-api-key": "viewer"})
        ).status_code == 403
        assert (
            await env.client.post(path(saved) + suffix, json=body, headers={"x-api-key": "foreign"})
        ).status_code == 404


async def test_missing_sessions_and_revisions_return_not_found(environment):
    env, missing = environment, str(uuid4())
    for suffix in ("", "/plans", "/plans/1"):
        assert (
            await env.client.get(f"/v1/planning-sessions/{missing}" + suffix)
        ).status_code == 404
    fresh = await session(env)
    assert (await env.client.get(path(fresh) + "/plans/1")).status_code == 404


async def test_disabled_agent_preserves_readable_history_and_marks_configuration_stale(environment):
    env, configured = environment, await environment.agent()
    saved = await save(env, await session(env, configured))
    original = (await env.client.get(path(saved) + "/plans/1")).json()
    await AgentStore(env.runtime).set_enabled(env.tenant, configured["agent_id"], False)
    response = await env.client.get(path(saved), headers={"x-api-key": "viewer"})
    assert response.status_code == 200 and response.json()["configuration_status"] == "stale"
    assert response.json()["snapshot"] == original["snapshot"]
    assert (await env.client.get(path(saved) + "/plans/1")).json() == original
    assert (
        await env.client.post(path(saved) + "/refresh", json={"expected_version": saved["version"]})
    ).status_code == 409


async def test_mode_switch_requires_complete_current_revision_and_never_starts_a_goal(environment):
    env, fresh = environment, await session(environment)
    incomplete = await save(env, fresh, plan(steps=[]))
    assert (
        await env.client.post(
            path(incomplete) + "/mode",
            json={"expected_version": incomplete["version"], "mode": "execute", "plan_revision": 1},
        )
    ).status_code == 409
    complete = await save(env, incomplete)
    for bad_revision in (None, 1, 3):
        body = {"expected_version": complete["version"], "mode": "execute"}
        if bad_revision is not None:
            body["plan_revision"] = bad_revision
        assert (await env.client.post(path(complete) + "/mode", json=body)).status_code == 409
    accepted = await env.client.post(
        path(complete) + "/mode",
        json={"expected_version": complete["version"], "mode": "execute", "plan_revision": 2},
    )
    assert accepted.status_code == 200, accepted.text
    accepted = accepted.json()
    assert accepted["mode"] == "execute" and accepted["plan_revision"] == 2
    assert (
        await env.client.post(
            path(accepted) + "/refresh", json={"expected_version": accepted["version"]}
        )
    ).status_code == 409
    assert (
        await env.client.post(
            path(accepted) + "/plans",
            json={"expected_version": accepted["version"], "plan": plan()},
        )
    ).status_code == 409
    returned = await env.client.post(
        path(accepted) + "/mode", json={"expected_version": accepted["version"], "mode": "plan"}
    )
    assert returned.status_code == 200 and returned.json()["mode"] == "plan"
    assert returned.json()["plan_revision"] == 2
    assert await env.runtime.list_goal_ids(env.tenant) == []
    assert not await env.runtime._client.exists(f"tenant:{env.tenant}:quota:active-runs")


async def test_configuration_changes_are_stale_until_explicit_refresh(environment):
    env, configured = environment, await environment.agent()
    saved = await save(env, await session(env, configured))
    old = (await env.client.get(path(saved) + "/plans/1")).json()
    updated = await AgentStore(env.runtime).update(
        {**configured, "persona": "New persona: describe the source limitations first."}, 1
    )
    assert updated["version"] == 2
    stale = await env.client.get(path(saved))
    assert stale.status_code == 200 and stale.json()["configuration_status"] == "stale"
    assert stale.json()["snapshot"] == saved["snapshot"]
    assert (
        await env.client.post(
            path(saved) + "/plans", json={"expected_version": saved["version"], "plan": plan()}
        )
    ).status_code == 409
    assert (
        await env.client.post(
            path(saved) + "/mode",
            json={"expected_version": saved["version"], "mode": "execute", "plan_revision": 1},
        )
    ).status_code == 409
    response = await env.client.post(
        path(saved) + "/refresh", json={"expected_version": saved["version"]}
    )
    assert response.status_code == 200, response.text
    refreshed = response.json()
    assert refreshed["version"] == saved["version"] + 1
    assert refreshed["plan_revision"] == 2
    assert refreshed["snapshot"]["root_agent"]["persona"] == updated["persona"]
    assert (
        refreshed["snapshot"]["snapshot_fingerprint"] != saved["snapshot"]["snapshot_fingerprint"]
    )
    assert (await env.client.get(path(refreshed))).json()["configuration_status"] == "current"
    assert (await env.client.get(path(refreshed) + "/plans/1")).json() == old
    assert (
        await env.client.post(path(saved) + "/refresh", json={"expected_version": saved["version"]})
    ).status_code == 409


async def test_model_route_replacement_requires_refresh_and_preserves_old_revision(
    environment, monkeypatch
):
    env = environment
    alias = "planning-routed-model"
    env.settings.llm_routes_json = json.dumps(
        {alias: {"base_url": "http://private-backend-one:8000/v1", "model": "weights-one"}}
    )
    env.app.state.model_router = ModelRouter(env.settings)

    def forbidden(*args, **kwargs):
        pytest.fail("Model route snapshot attempted to initialize an inference client")

    monkeypatch.setattr(env.app.state.model_router, "get", forbidden)
    configured = await env.agent(model=alias)
    saved = await save(env, await session(env, configured))
    old = (await env.client.get(path(saved) + "/plans/1")).json()
    old_route = saved["snapshot"]["root_agent"]["model_route"]
    assert old_route["resolved_model"] == "weights-one"
    assert (await env.client.get(path(saved))).json()["configuration_status"] == "current"

    replacement = env.settings.model_copy(
        update={
            "llm_routes_json": json.dumps(
                {
                    alias: {
                        "base_url": "http://private-backend-two:8000/v1",
                        "model": "weights-two",
                    }
                }
            )
        }
    )
    env.app.state.model_router = ModelRouter(replacement)
    monkeypatch.setattr(env.app.state.model_router, "get", forbidden)
    stale = (await env.client.get(path(saved))).json()
    assert stale["configuration_status"] == "stale"
    assert stale["snapshot"] == saved["snapshot"]
    response = await env.client.post(
        path(saved) + "/refresh", json={"expected_version": saved["version"]}
    )
    assert response.status_code == 200, response.text
    refreshed = response.json()
    assert refreshed["configuration_status"] == "current" and refreshed["plan_revision"] == 2
    new_route = refreshed["snapshot"]["root_agent"]["model_route"]
    assert new_route["resolved_model"] == "weights-two"
    assert new_route["route_fingerprint"] != old_route["route_fingerprint"]
    assert "private-backend" not in json.dumps(refreshed)
    assert (await env.client.get(path(saved) + "/plans/1")).json() == old


async def test_skill_revocation_cannot_be_undone_without_refreshing_the_snapshot(environment):
    env = environment
    skills = SkillStore(env.runtime)
    skill = await skills.create(
        env.tenant,
        SkillContent(
            name="planning-evidence",
            display_name="Planning evidence",
            description="Keep a source for each planned investigation step.",
            instructions="Cite available evidence and identify uncertainty.",
        ).model_dump(),
        "admin",
    )
    configured = await env.agent(skills=[{"skill_id": skill["skill_id"], "revision": 1}])
    saved = await save(env, await session(env, configured))
    old = (await env.client.get(path(saved) + "/plans/1")).json()
    disabled = await skills.set_enabled(env.tenant, skill["skill_id"], False, 1, "admin")
    assert (await env.client.get(path(saved))).json()["configuration_status"] == "stale"
    assert (
        await env.client.post(path(saved) + "/refresh", json={"expected_version": saved["version"]})
    ).status_code == 409
    await skills.set_enabled(env.tenant, skill["skill_id"], True, disabled["version"], "admin")
    assert (await env.client.get(path(saved))).json()["configuration_status"] == "stale"
    refreshed = await env.client.post(
        path(saved) + "/refresh", json={"expected_version": saved["version"]}
    )
    assert refreshed.status_code == 200, refreshed.text
    snapshot = refreshed.json()["snapshot"]["root_agent"]["skill_snapshots"][0]
    assert snapshot["revision"] == 1 and snapshot["revocation_epoch"] == 1
    assert (await env.client.get(path(saved) + "/plans/1")).json() == old


async def test_concurrent_plan_edits_have_one_cas_winner(environment):
    env, fresh = environment, await session(environment)
    responses = await asyncio.gather(
        *[
            env.client.post(
                path(fresh) + "/plans",
                json={"expected_version": 1, "plan": plan(objective=f"Revision candidate {index}")},
            )
            for index in range(8)
        ]
    )
    assert [response.status_code for response in responses].count(200) == 1
    assert [response.status_code for response in responses].count(409) == 7
    final = (await env.client.get(path(fresh))).json()
    assert final["version"] == 2 and final["plan_revision"] == 1
    revisions = (await env.client.get(path(fresh) + "/plans")).json()["plans"]
    assert len(revisions) == 1
    assert re.fullmatch(r"[0-9a-f]{64}", revisions[0]["content_sha256"])


async def test_chief_assignment_scope_and_worker_linear_plan_rules(environment):
    env = environment
    worker, other = await env.agent(), await env.agent()
    chief = await env.agent(role="chief", delegate_agent_ids=[worker["agent_id"]])
    current = await session(env, chief)
    assignment = {
        "agent_id": worker["agent_id"],
        "objective": "Review the incident evidence",
        "steps": [{"query": "Extract the confirmed evidence", "target_url": None}],
    }
    for invalid in (
        plan(),
        plan(steps=[], assignments=[{**assignment, "agent_id": other["agent_id"]}]),
        plan(steps=[], assignments=[assignment, assignment]),
    ):
        assert (
            await env.client.post(
                path(current) + "/plans", json={"expected_version": 1, "plan": invalid}
            )
        ).status_code == 422
    saved = await save(env, current, plan(steps=[], assignments=[assignment]))
    assert [item["agent_id"] for item in saved["snapshot"]["workers"]] == [worker["agent_id"]]
    worker_session = await session(env, worker)
    assert (
        await env.client.post(
            path(worker_session) + "/plans",
            json={"expected_version": 1, "plan": plan(assignments=[assignment])},
        )
    ).status_code == 422


@pytest.mark.parametrize("version", [True, "1", 1.0, 0])
async def test_versions_are_strict_positive_integers(environment, version):
    env, fresh = environment, await session(environment)
    for suffix, body in (
        ("/plans", {"expected_version": version, "plan": plan()}),
        ("/refresh", {"expected_version": version}),
        ("/mode", {"expected_version": version, "mode": "plan"}),
    ):
        assert (await env.client.post(path(fresh) + suffix, json=body)).status_code == 422


async def test_caller_cannot_forge_configuration_or_execution_fields(environment):
    env, configured = environment, await environment.agent()
    create = f"/v1/agents/{configured['agent_id']}/planning-sessions"
    for body in ({"tenant_id": env.foreign}, {"snapshot": {}}, {"mode": "execute"}):
        assert (await env.client.post(create, json=body)).status_code == 422
    current = await session(env, configured)
    for draft in (
        plan(policy={"allowed_tools": ["browser_click"]}),
        plan(model="unapproved-model"),
        plan(snapshot={"tenant_id": env.foreign}),
        plan(steps=[{"query": "Read", "approved_tools": ["browser_click"]}]),
    ):
        assert (
            await env.client.post(
                path(current) + "/plans", json={"expected_version": 1, "plan": draft}
            )
        ).status_code == 422
    assert (
        await env.client.post(
            path(current) + "/mode", json={"expected_version": 1, "mode": "plan", "goal_id": "fake"}
        )
    ).status_code == 422
    assert (await env.client.get(path(current))).json()["plan_revision"] == 0


async def test_plan_text_is_redacted_and_content_checks_run_before_persistence(environment):
    env, fresh = environment, await session(environment)
    env.settings.pii_redaction = True
    private = "private.person@example.com"
    saved = await save(
        env,
        fresh,
        plan(
            objective=f"Review for {private}",
            context=f"Contact {private}",
            steps=[{"query": private}],
        ),
    )
    revision = (await env.client.get(path(saved) + "/plans/1")).json()
    assert private not in json.dumps(revision["plan"])
    assert "[EMAIL_REDACTED]" in revision["plan"]["objective"]
    assert env.detector.scans
    env.settings.content_blocked_patterns = ["BLOCK_PATTERN"]
    blocked = await env.client.post(
        path(saved) + "/plans",
        json={"expected_version": saved["version"], "plan": plan(context="BLOCK_PATTERN")},
    )
    assert blocked.status_code == 403
    env.settings.content_blocked_patterns = []
    env.detector.blocked = True
    blocked = await env.client.post(
        path(saved) + "/plans", json={"expected_version": saved["version"], "plan": plan()}
    )
    assert blocked.status_code == 403
    current = (await env.client.get(path(saved))).json()
    assert current["version"] == saved["version"] and current["plan_revision"] == 1


@pytest.mark.parametrize("blocked", [False, True])
async def test_planning_findings_persist_metadata_without_triggering_background_automation(
    environment, blocked
):
    env, fresh = environment, await session(environment)
    private = "PRIVATE_MATCH_MUST_NOT_ENTER_FINDING_METADATA"
    env.detector.findings = (
        {
            "finding_id": str(uuid4()),
            "rule_id": "ATR-PLANNING-TEST",
            "title": "Planning input finding",
            "severity": "high",
            "confidence": "high",
            "enforcement_action": "detected",
            "matched_text": private,
        },
    )
    env.detector.blocked = blocked
    response = await env.client.post(
        path(fresh) + "/plans",
        json={"expected_version": 1, "plan": plan(context=private)},
    )
    assert response.status_code == (403 if blocked else 200)
    findings = await env.runtime.list_threat_findings(env.tenant)
    assert len(findings) == 1
    assert findings[0]["rule_id"] == "ATR-PLANNING-TEST"
    assert findings[0]["context_id"] == f"planning:{fresh['session_id']}"
    assert private not in json.dumps(findings)
    events = await env.runtime._client.xrange("automation:internal-events")
    assert all(fields.get("tenant_id") != env.tenant for _, fields in events)
    assert await env.runtime.list_goal_ids(env.tenant) == []
    current = (await env.client.get(path(fresh))).json()
    assert current["plan_revision"] == (0 if blocked else 1)


async def test_full_plan_serialization_has_a_bounded_size(environment):
    env, fresh = environment, await session(environment)
    oversized = plan(steps=[{"query": f"{index}:" + "x" * 7_000} for index in range(10)])
    assert len(json.dumps(oversized).encode("utf-8")) > 65_536
    response = await env.client.post(
        path(fresh) + "/plans", json={"expected_version": 1, "plan": oversized}
    )
    assert response.status_code in {413, 422}
    assert (await env.client.get(path(fresh))).json()["plan_revision"] == 0


async def test_plan_over_full_atr_scan_limit_is_rejected_without_a_revision(environment):
    env, fresh = environment, await session(environment)
    env.settings.atr_enabled = True
    env.settings.atr_max_content_chars = 1_000
    draft = plan(context="Evidence context. " * 80)
    assert len(draft["context"]) > 1_000
    assert len(json.dumps(draft).encode("utf-8")) < 65_536
    before_scans = len(env.detector.scans)
    response = await env.client.post(
        path(fresh) + "/plans", json={"expected_version": 1, "plan": draft}
    )
    assert response.status_code == 413
    assert len(env.detector.scans) == before_scans
    current = (await env.client.get(path(fresh))).json()
    assert current["version"] == 1 and current["plan_revision"] == 0
    assert (await env.client.get(path(fresh) + "/plans")).json()["plans"] == []


async def test_atr_receives_literal_newlines_and_target_urls_in_plan_text(environment):
    env, fresh = environment, await session(environment)
    objective = "Review incident evidence\nPreserve each source"
    context = "First line of context\nSecond line of context"
    query = "Read the source\nIdentify its limitations"
    url = "https://example.com/evidence"
    await save(
        env,
        fresh,
        plan(objective=objective, context=context, steps=[{"query": query, "target_url": url}]),
    )
    scanned, kwargs = env.detector.scans[-1]
    for original in (objective, context, query, url):
        assert original in scanned
    assert "\\n" not in scanned
    assert kwargs["fields"]["user_input"] == scanned
    assert kwargs["location"] == "planning_plan"


async def test_disabled_and_unresolvable_configuration_are_conflicts(environment):
    env = environment
    disabled = await env.agent(enabled=False)
    invalid = await env.agent(model="not-allowlisted-for-planning")
    for configured in (disabled, invalid):
        response = await env.client.post(f"/v1/agents/{configured['agent_id']}/planning-sessions")
        assert response.status_code == 409
        assert "PRIVATE_PLANNING_API_KEY_SENTINEL" not in response.text


async def test_request_rate_and_redis_availability_are_enforced(environment):
    env, configured = environment, await environment.agent()
    env.settings.tenant_requests_per_minute = 1
    first = await session(env, configured)
    response = await env.client.post(f"/v1/agents/{configured['agent_id']}/planning-sessions")
    assert response.status_code == 429
    env.app.state.redis = None
    assert (await env.client.get(path(first))).status_code == 503
    assert (
        await env.client.post(f"/v1/agents/{configured['agent_id']}/planning-sessions")
    ).status_code == 503
