"""Chief queue fan-out/fan-in checks against an isolated real Redis instance."""

import asyncio
import json
import os
import time
from copy import deepcopy
from uuid import uuid4

import pytest

from app.core.delegation import DelegationStore
from app.core.goal_execution import GoalExecutionStore
from app.core.redis_runtime import RedisRuntime


@pytest.fixture
async def delegation():
    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL for real Redis Chief delegation checks")
    runtime = RedisRuntime(url)
    await runtime.ping()
    tenant, parent_id = f"delegation-test-{uuid4().hex}", str(uuid4())
    parent = {
        "goal_id": parent_id,
        "tenant_id": tenant,
        "agent_id": "chief",
        "agent_config": {
            "agent_id": "chief",
            "role": "chief",
            "delegate_agent_ids": ["analyst", "writer"],
        },
        "objective": "Analyze and produce a report",
        "status": "running",
        "model": "chief-model",
        "steps": [],
        "execution_owner": "owner",
        "execution_fence": 1,
        "execution_lease_until": time.time() + 60,
        "team_token_budget": 20_000,
        "policy": {"token_budget": 20_000},
    }
    await runtime.save_goal(parent_id, parent, tenant)
    store = DelegationStore(runtime)
    try:
        yield runtime, store, tenant, parent
    finally:
        for stream in ("agent:goal:queue", "automation:internal-events"):
            rows = await runtime._client.xrange(stream)
            ids = [entry_id for entry_id, fields in rows if fields.get("tenant_id") == tenant]
            if ids:
                await runtime._client.xdel(stream, *ids)
        keys = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}:*")]
        if keys:
            await runtime._client.delete(*keys)
        await runtime._client.srem("agent:goal:tenants", tenant)
        await runtime.close()


def children(parent):
    result = []
    for agent_id, model in (("analyst", "analysis-model"), ("writer", "writing-model")):
        policy = {
            "allowed_tools": ["scrape_dynamic_page"],
            "allowed_domains": ["example.com"],
            "token_budget": 8_000,
            "approval_timeout_seconds": 86_400,
        }
        result.append(
            {
                "goal_id": str(uuid4()),
                "tenant_id": parent["tenant_id"],
                "agent_id": agent_id,
                "agent_config": {
                    "agent_id": agent_id,
                    "role": "worker",
                    "model": model,
                    "persona": f"private persona for {agent_id}",
                    "delegate_agent_ids": [],
                    "policy": deepcopy(policy),
                },
                "model": model,
                "policy": deepcopy(policy),
                "token_budget": 8_000,
                "objective": f"secret assignment for {agent_id}",
                "steps": [
                    {
                        "id": str(uuid4()),
                        "query": "Complete your assignment",
                        "status": "pending",
                        "answer": "",
                    }
                ],
            }
        )
    return result


def planned(parent):
    return deepcopy(parent) | {"chief_budget": 4_000, "policy": {"token_budget": 4_000}}


async def create(store, parent):
    assert await store.create_children(
        planned(parent),
        "owner",
        children(parent),
        {
            "total_tokens": 500,
            "policy_charged_tokens": 1_000,
        },
    )


async def finish_children(runtime, store, tenant, parent, statuses=("completed", "completed")):
    current = await runtime.get_goal(parent["goal_id"], tenant)
    for child_id, status in zip(current["children_goal_ids"], statuses, strict=True):
        child = await runtime.get_goal(child_id, tenant)
        child["status"] = status
        child["steps"][0]["answer"] = f"Completed result from {child['agent_id']}"
        await runtime.save_goal(child_id, child, tenant)


@pytest.mark.asyncio
async def test_two_creators_atomically_persist_one_plan_and_child_queue(delegation):
    runtime, store, tenant, parent = delegation
    results = await asyncio.gather(
        *[
            store.create_children(planned(parent), "owner", children(parent), {"total_tokens": 100})
            for _ in range(2)
        ]
    )
    assert results.count(True) == 1
    current = await runtime.get_goal(parent["goal_id"], tenant)
    assert current["status"] == "waiting_children"
    assert current["execution_owner"] is None
    assert current["execution_lease_until"] == 0
    ids = current["children_goal_ids"]
    assert ids == [store.child_id(tenant, parent["goal_id"], index) for index in range(2)]
    rows = await runtime._client.xrange("agent:goal:queue")
    assert len([f for _, f in rows if f.get("tenant_id") == tenant]) == 2
    audit = await runtime._client.xrange(f"tenant:{tenant}:audit")
    assert "secret assignment" not in json.dumps(audit)
    assert "private persona" not in json.dumps(audit)
    assert (await store.children_ready(parent["goal_id"], tenant))["status"] == "pending"


@pytest.mark.parametrize(
    "escape", ["tenant", "unapproved", "recursive", "tools", "domains", "model", "team_budget"]
)
@pytest.mark.asyncio
async def test_child_cannot_escape_identity_scope_or_shared_budget(delegation, escape):
    runtime, store, tenant, parent = delegation
    tasks = children(parent)
    if escape == "tenant":
        tasks[0]["tenant_id"] = "other"
    elif escape == "unapproved":
        tasks[0]["agent_id"] = "admin"
    elif escape == "recursive":
        tasks[0]["agent_config"]["delegate_agent_ids"] = ["writer"]
    elif escape == "tools":
        tasks[0]["policy"]["allowed_tools"] = ["browser_type"]
    elif escape == "domains":
        tasks[0]["policy"]["allowed_domains"] = []
    elif escape == "model":
        tasks[0]["model"] = "unauthorized-model"
    else:
        tasks[0]["token_budget"] = 9_000
        tasks[0]["policy"]["token_budget"] = 9_000
        tasks[0]["agent_config"]["policy"]["token_budget"] = 9_000
    with pytest.raises(ValueError):
        await store.create_children(planned(parent), "owner", tasks, {})
    assert (await runtime.get_goal(parent["goal_id"], tenant))["status"] == "running"
    assert not await runtime.get_goal(store.child_id(tenant, parent["goal_id"], 0), tenant)


@pytest.mark.asyncio
async def test_wrong_owner_expired_owner_and_cancelled_parent_are_fenced(delegation):
    runtime, store, tenant, parent = delegation
    assert not await store.create_children(planned(parent), "other-owner", children(parent), {})
    parent["execution_lease_until"] = time.time() - 1
    await runtime.save_goal(parent["goal_id"], parent, tenant)
    assert not await store.create_children(planned(parent), "owner", children(parent), {})
    parent.update(status="cancelled", execution_lease_until=time.time() + 60)
    await runtime.save_goal(parent["goal_id"], parent, tenant)
    assert not await store.create_children(planned(parent), "owner", children(parent), {})


@pytest.mark.asyncio
async def test_waiting_parent_releases_slot_so_single_worker_can_claim_child(delegation):
    runtime, store, tenant, parent = delegation
    await runtime.acquire_run_slot(tenant, f"goal:{parent['goal_id']}:owner", 1, 60)
    await create(store, parent)
    current = await runtime.get_goal(parent["goal_id"], tenant)
    child_id = current["children_goal_ids"][0]
    child = await GoalExecutionStore(runtime).claim(child_id, tenant, "single-worker", 60_000)
    assert child and child["status"] == "running"
    assert await runtime.acquire_run_slot(tenant, f"goal:{child_id}:single-worker", 1, 60)


@pytest.mark.asyncio
async def test_successful_children_requeue_parent_once_with_agent_provenance(delegation):
    runtime, store, tenant, parent = delegation
    await create(store, parent)
    await finish_children(runtime, store, tenant, parent)
    results = await asyncio.gather(
        *[store.requeue_parent(tenant, parent["goal_id"]) for _ in range(3)]
    )
    assert results.count(True) == 1
    current = await runtime.get_goal(parent["goal_id"], tenant)
    assert current["status"] == "queued"
    assert {result["agent_id"] for result in current["delegation_results"]} == {"analyst", "writer"}
    assert all("Completed result" in result["answer"] for result in current["delegation_results"])
    assert current["delegation_planned"] is True


@pytest.mark.asyncio
async def test_failed_child_fails_parent_and_automatic_failure_does_not_chain(delegation):
    runtime, store, tenant, parent = delegation
    parent["automation_id"] = "scheduled-chief"
    await runtime.save_goal(parent["goal_id"], parent, tenant)
    await create(store, parent)
    await finish_children(runtime, store, tenant, parent, ("failed", "completed"))
    assert await store.requeue_parent(tenant, parent["goal_id"])
    current = await runtime.get_goal(parent["goal_id"], tenant)
    assert current["status"] == "failed"
    assert "Worker Goals failed" in current["error"]
    rows = await runtime._client.xrange("automation:internal-events")
    assert not [fields for _, fields in rows if fields.get("tenant_id") == tenant]


@pytest.mark.asyncio
async def test_cancel_and_pause_parent_cannot_be_overwritten_by_completed_children(delegation):
    runtime, store, tenant, parent = delegation
    await create(store, parent)
    current = await runtime.get_goal(parent["goal_id"], tenant)
    current["status"] = "paused"
    await runtime.save_goal(parent["goal_id"], current, tenant)
    assert not await store.requeue_parent(tenant, parent["goal_id"])
    current["status"] = "cancelled"
    await runtime.save_goal(parent["goal_id"], current, tenant)
    assert await store.cancel_children(tenant, parent["goal_id"], "operator") == 2
    assert await store.cancel_children(tenant, parent["goal_id"], "operator") == 0
    assert not await store.requeue_parent(tenant, parent["goal_id"])
    assert (await runtime.get_goal(parent["goal_id"], tenant))["status"] == "cancelled"
    assert (await store.children_ready(parent["goal_id"], tenant))["status"] == "failed"


@pytest.mark.parametrize("crash_after_cascade", [False, True])
@pytest.mark.asyncio
async def test_scheduler_recovers_parent_cancel_before_child_cascade_and_refunds_quota(
    delegation, monkeypatch, crash_after_cascade
):
    runtime, store, tenant, parent = delegation
    await create(store, parent)
    current = await runtime.get_goal(parent["goal_id"], tenant)
    child_id = current["children_goal_ids"][0]
    execution = GoalExecutionStore(runtime)
    child = await execution.claim(child_id, tenant, "child-owner", 60_000)
    await execution.reserve_budget(child, "child-owner", 1_000_000)
    child.update(
        status="waiting_approval",
        pending_approval={
            "approval_id": "future-approval",
            "expires_at": time.time() + 86_400,
        },
    )
    assert await execution.finish(child, "child-owner", {"type": "goal_waiting_approval"})
    reservation = child["token_reservation"]
    quota_key = f"tenant:{tenant}:quota:tokens:{reservation['day']}"
    assert int(await runtime._client.get(quota_key)) == 8_000
    # Simulate the API committing parent cancellation and dying before cascade.
    current["status"] = "cancelled"
    await runtime.save_goal(parent["goal_id"], current, tenant)
    assert (await runtime.get_goal(child_id, tenant))["status"] == "waiting_approval"
    if crash_after_cascade:
        original = GoalExecutionStore.reconcile_budget

        async def crash_before_refund(*args, **kwargs):
            raise RuntimeError("simulated cascade committed before quota refund")

        monkeypatch.setattr(GoalExecutionStore, "reconcile_budget", crash_before_refund)
        with pytest.raises(RuntimeError, match="before quota refund"):
            await store.cancel_children(tenant, parent["goal_id"], "operator")
        assert int(await runtime._client.get(quota_key)) == 8_000
        monkeypatch.setattr(GoalExecutionStore, "reconcile_budget", original)
    recovered = await store.reconcile_parents()
    assert recovered == (0 if crash_after_cascade else 2)
    assert (await runtime.get_goal(child_id, tenant))["status"] == "cancelled"
    assert int(await runtime._client.get(quota_key)) == 0
    assert await store.reconcile_parents() == 0
