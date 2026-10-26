"""Concurrency and durable approval assertions against a real isolated Redis DB.

Run with TEST_REDIS_URL pointed at a disposable test database. Tests use unique
tenant/goal identifiers and never flush a database or delete another test's keys.
"""

import asyncio
import json
import os
import time
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import pytest_asyncio
from langgraph.checkpoint.memory import MemorySaver

from app.core.config import Settings
from app.core.goal_execution import GoalExecutionStore
from app.core.redis_runtime import RedisRuntime
from app.core.threat_detection import ThreatScan
from app.tools.registry import ToolSpec
from app.worker import execute_goal


@pytest_asyncio.fixture
async def state():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL to run real Redis Goal reliability checks")
    runtime = RedisRuntime(url)
    await runtime.ping()
    tenant = f"goaltest-{uuid4().hex}"
    goal = {
        "goal_id": str(uuid4()),
        "tenant_id": tenant,
        "status": "queued",
        "objective": "bounded test",
        "created_by": "test",
        "steps": [],
        "policy": {"token_budget": 1_000},
    }
    store = GoalExecutionStore(runtime)
    await store.submit_manual(goal)
    try:
        yield runtime, store, tenant, goal
    finally:
        # Keys are retained for audit/debug in the dedicated disposable test DB.
        await runtime.close()


@pytest.mark.asyncio
async def test_only_one_of_two_workers_can_claim_and_renew(state):
    runtime, store, tenant, goal = state
    first, second = await asyncio.gather(
        store.claim(goal["goal_id"], tenant, "one", 30_000),
        store.claim(goal["goal_id"], tenant, "two", 30_000),
    )
    winner = first or second
    assert sum(value is not None for value in (first, second)) == 1
    assert await store.renew(goal["goal_id"], tenant, winner["execution_owner"], 60_000)
    assert not await store.renew(goal["goal_id"], tenant, "stale-owner", 60_000)
    await store.recover_expired()
    current = await runtime.get_goal(goal["goal_id"], tenant)
    assert current["execution_owner"] == winner["execution_owner"]


@pytest.mark.asyncio
async def test_expired_worker_is_fenced_from_writing_and_recovery_requeues(state):
    runtime, store, tenant, goal = state
    claimed = await store.claim(goal["goal_id"], tenant, "dead", 1_000, now=time.time() - 5)
    assert await store.recover_expired() >= 1
    live = await store.claim(goal["goal_id"], tenant, "live", 60_000)
    assert live["execution_fence"] == claimed["execution_fence"] + 1
    claimed["status"] = "completed"
    assert not await store.save_owned(claimed, "dead")
    assert not await store.finish(claimed, "dead", {"type": "goal_completed"})
    assert (await runtime.get_goal(goal["goal_id"], tenant))["execution_owner"] == "live"


@pytest.mark.asyncio
async def test_cancel_cannot_be_overwritten_by_an_active_worker(state):
    runtime, store, tenant, goal = state
    active = await store.claim(goal["goal_id"], tenant, "worker", 60_000)
    await store.control(tenant, goal["goal_id"], "cancel", "operator")
    assert not await store.save_owned(active, "worker")
    assert not await store.finish(
        {**active, "status": "completed"}, "worker", {"type": "goal_completed"}
    )
    assert (await runtime.get_goal(goal["goal_id"], tenant))["status"] == "cancelled"


async def waiting(store, tenant, goal, expires=None):
    active = await store.claim(goal["goal_id"], tenant, "worker", 60_000)
    active.update(
        status="waiting_approval",
        pending_approval={
            "approval_id": "a-1",
            "action_id": "exact-action",
            "tool": "browser_type",
            "arguments": {"selector": "#name"},
            "expires_at": datetime.fromtimestamp(expires or time.time() + 300, UTC).isoformat(),
        },
    )
    assert await store.finish(active, "worker", {"type": "goal_waiting_approval"})
    return active


@pytest.mark.asyncio
async def test_approval_is_exact_tenant_scoped_and_decided_once(state):
    runtime, store, tenant, goal = state
    await waiting(store, tenant, goal)
    assert (
        await store.decide_approval("other-tenant", goal["goal_id"], "a-1", True, "", "u") is None
    )
    with pytest.raises(ValueError, match="this approval"):
        await store.decide_approval(tenant, goal["goal_id"], "wrong-id", True, "", "u")
    result = await store.decide_approval(tenant, goal["goal_id"], "a-1", True, "reviewed", "u")
    assert result["status"] == "queued"
    assert result["pending_approval"]["action_id"] == "exact-action"
    assert result["approval_decision"]["subject"] == "u"
    with pytest.raises(ValueError):
        await store.decide_approval(tenant, goal["goal_id"], "a-1", True, "", "u")
    saved = await runtime.get_goal(goal["goal_id"], tenant)
    assert saved["status"] == "queued"


@pytest.mark.asyncio
async def test_expired_approval_cancels_instead_of_resuming(state):
    runtime, store, tenant, goal = state
    await waiting(store, tenant, goal, time.time() - 10)
    result = await store.decide_approval(tenant, goal["goal_id"], "a-1", True, "", "late")
    assert result["status"] == "cancelled"
    assert result["approval_decision"]["reason"] == "approval_expired"
    assert (await runtime.get_goal(goal["goal_id"], tenant))["status"] == "cancelled"


@pytest.mark.asyncio
async def test_budget_reserved_once_across_approval_and_reconciled_once(state):
    runtime, store, tenant, goal = state
    active = await store.claim(goal["goal_id"], tenant, "one", 60_000)
    await store.reserve_budget(active, "one", 1_500)
    day = active["token_reservation"]["day"]
    quota = f"tenant:{tenant}:quota:tokens:{day}"
    assert int(await runtime._client.get(quota)) == 1_000
    active.update(
        status="waiting_approval",
        pending_approval={
            "approval_id": "a",
            "action_id": "action",
            "expires_at": time.time() + 60,
        },
        usage={"total_tokens": 50, "policy_charged_tokens": 400},
    )
    assert await store.finish(active, "one", {"type": "goal_waiting_approval"})
    await store.decide_approval(tenant, goal["goal_id"], "a", True, "", "u")
    resumed = await store.claim(goal["goal_id"], tenant, "two", 60_000)
    await store.reserve_budget(resumed, "two", 1_500)
    assert int(await runtime._client.get(quota)) == 1_000
    resumed["status"] = "completed"
    assert await store.finish(resumed, "two", {"type": "goal_completed"})
    await asyncio.gather(store.reconcile_budget(resumed), store.reconcile_budget(resumed))
    # Failed/interrupted calls retain conservative charges, not only reported usage.
    assert int(await runtime._client.get(quota)) == 400


@pytest.mark.asyncio
async def test_quota_reservation_fails_before_any_model_or_tool_call(state):
    runtime, store, tenant, goal = state
    active = await store.claim(goal["goal_id"], tenant, "one", 60_000)
    with pytest.raises(RuntimeError, match="daily token"):
        await store.reserve_budget(active, "one", 999)
    assert "token_reservation" not in await runtime.get_goal(goal["goal_id"], tenant)


class BrowserModel:
    def __init__(self):
        self.plans = iter(
            [
                '{"tool":"browser_click","arguments":{"selector":"#first"}}',
                '{"tool":"browser_click","arguments":{"selector":"#second"}}',
                '{"tool":null,"arguments":{}}',
            ]
        )

    async def complete(self, system, prompt):
        return next(self.plans)

    async def stream(self, system, prompt):
        yield "The requested browser operations completed successfully."


class Router:
    def __init__(self, model):
        self.model = model

    def get(self, selection):
        return self.model


class Writes:
    def __init__(self):
        self.calls = []

    def specs(self):
        return [ToolSpec("browser_click", "click a button", requires_approval=True)]

    async def invoke(self, name, arguments):
        self.calls.append((name, arguments))
        return {"clicked": arguments["selector"]}


def runnable(goal):
    return {
        **goal,
        "model": "test",
        "max_retries": 0,
        "max_tool_rounds": 5,
        "current_step": 0,
        "step_deadline_seconds": 60,
        "steps": [
            {
                "id": "s",
                "query": "Use browser to click both requested buttons",
                "status": "pending",
                "answer": "",
                "error": "",
                "target_url": None,
            }
        ],
        "policy": {"token_budget": 20_000, "allowed_tools": ["browser_click"]},
    }


@pytest.mark.asyncio
async def test_real_graph_requires_separate_approval_for_each_exact_action(state):
    runtime, store, tenant, goal = state
    await runtime.save_goal(goal["goal_id"], runnable(goal), tenant)
    model, tools, checkpointer = BrowserModel(), Writes(), MemorySaver()

    async def run():
        await execute_goal(
            goal["goal_id"],
            runtime,
            Router(model),
            object(),
            tenant_id=tenant,
            conversation_max_turns=20,
            conversation_ttl_seconds=3_600,
            tool_executor=tools,
            checkpointer=checkpointer,
            settings=Settings(),
        )

    await run()
    first = await runtime.get_goal(goal["goal_id"], tenant)
    assert first["status"] == "waiting_approval", first.get("error")
    assert tools.calls == []
    assert await runtime._client.zcard(f"tenant:{tenant}:quota:active-runs") == 0
    pending = first["pending_approval"]
    assert pending["arguments"]["selector"] == "#first"
    await store.decide_approval(tenant, goal["goal_id"], pending["approval_id"], True, "", "u")
    await run()
    second = await runtime.get_goal(goal["goal_id"], tenant)
    assert second["status"] == "waiting_approval", second.get("error")
    assert tools.calls == [("browser_click", {"selector": "#first"})]
    assert second["pending_approval"]["action_id"] != pending["action_id"]
    assert second["pending_approval"]["approval_id"] != pending["approval_id"]
    await store.decide_approval(
        tenant, goal["goal_id"], second["pending_approval"]["approval_id"], True, "", "u"
    )
    await run()
    completed = await runtime.get_goal(goal["goal_id"], tenant)
    assert completed["status"] == "completed", completed.get("error")
    assert tools.calls == [
        ("browser_click", {"selector": "#first"}),
        ("browser_click", {"selector": "#second"}),
    ]
    assert completed["usage"]["model_calls"] >= 4
    assert completed["usage"]["policy_charged_tokens"] > 0


@pytest.mark.asyncio
async def test_worker_heartbeat_renews_and_cancel_stops_without_killing_worker(state):
    runtime, store, tenant, goal = state
    ready, release = asyncio.Event(), asyncio.Event()

    class SlowModel:
        async def stream(self, system, prompt):
            ready.set()
            await release.wait()
            yield "A sufficiently detailed result if the run is allowed to continue."

    value = runnable(goal)
    value["steps"][0]["query"] = "Write a report from existing context"
    value["policy"]["allowed_tools"] = []
    await runtime.save_goal(goal["goal_id"], value, tenant)
    task = asyncio.create_task(
        execute_goal(
            goal["goal_id"],
            runtime,
            Router(SlowModel()),
            object(),
            tenant_id=tenant,
            conversation_max_turns=20,
            conversation_ttl_seconds=3_600,
            settings=Settings(goal_lease_ms=5_000),
        )
    )
    try:
        await asyncio.wait_for(ready.wait(), 3)
        first = await runtime.get_goal(goal["goal_id"], tenant)
        await asyncio.sleep(1.2)
        later = await runtime.get_goal(goal["goal_id"], tenant)
        assert later["execution_lease_until"] > first["execution_lease_until"]
        assert await store.claim(goal["goal_id"], tenant, "competitor", 5_000) is None
        await store.control(tenant, goal["goal_id"], "cancel", "operator")
        await asyncio.wait_for(task, 3)
        assert (await runtime.get_goal(goal["goal_id"], tenant))["status"] == "cancelled"
        assert await runtime._client.zcard(f"tenant:{tenant}:quota:active-runs") == 0
    finally:
        release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_cross_day_approval_moves_only_remaining_budget_to_new_day(state, monkeypatch):
    import app.core.goal_execution as execution

    class Day(datetime):
        current = 1

        @classmethod
        def now(cls, tz=None):
            return datetime(2040, 1, cls.current, tzinfo=UTC)

    monkeypatch.setattr(execution, "datetime", Day)
    runtime, store, tenant, goal = state
    active = await store.claim(goal["goal_id"], tenant, "one", 60_000)
    await store.reserve_budget(active, "one", 1_500)
    active.update(
        status="waiting_approval",
        usage={"policy_charged_tokens": 400},
        pending_approval={"approval_id": "a", "action_id": "x", "expires_at": time.time() + 60},
    )
    await store.finish(active, "one", {"type": "goal_waiting_approval"})
    await store.decide_approval(tenant, goal["goal_id"], "a", True, "", "u")
    Day.current = 2
    resumed = await store.claim(goal["goal_id"], tenant, "two", 60_000)
    await store.reserve_budget(resumed, "two", 1_500)
    first_key = f"tenant:{tenant}:quota:tokens:2040-01-01"
    next_key = f"tenant:{tenant}:quota:tokens:2040-01-02"
    assert int(await runtime._client.get(first_key)) == 400
    assert int(await runtime._client.get(next_key)) == 600
    resumed.update(status="completed", usage={"policy_charged_tokens": 700})
    await store.finish(resumed, "two", {"type": "goal_completed"})
    await store.reconcile_budget(resumed)
    assert int(await runtime._client.get(first_key)) == 400
    assert int(await runtime._client.get(next_key)) == 300


@pytest.mark.asyncio
async def test_call_crossing_midnight_reconciles_both_day_reservations(state, monkeypatch):
    import app.core.goal_execution as execution

    class Day(datetime):
        current = 1

        @classmethod
        def now(cls, tz=None):
            return datetime(2040, 2, cls.current, tzinfo=UTC)

    monkeypatch.setattr(execution, "datetime", Day)
    runtime, store, tenant, goal = state
    active = await store.claim(goal["goal_id"], tenant, "one", 60_000)
    await store.reserve_budget(active, "one", 1_500)
    active["usage"] = {"policy_charged_tokens": 400}
    await store.save_owned(active, "one")
    Day.current = 2
    # Successful response lowers the old day's pending conservative call charge.
    await store.reserve_budget(active, "one", 1_500, usage={"policy_charged_tokens": 100})
    active["usage"] = {"policy_charged_tokens": 100}
    await store.save_owned(active, "one")
    first_key = f"tenant:{tenant}:quota:tokens:2040-02-01"
    next_key = f"tenant:{tenant}:quota:tokens:2040-02-02"
    assert int(await runtime._client.get(first_key)) == 100
    assert int(await runtime._client.get(next_key)) == 900
    active.update(status="completed", usage={"policy_charged_tokens": 450})
    await store.finish(active, "one", {"type": "goal_completed"})
    await store.reconcile_budget(active)
    assert int(await runtime._client.get(next_key)) == 350


@pytest.mark.asyncio
async def test_queued_goal_is_rescanned_under_current_atr_before_model_execution(state):
    runtime, store, tenant, goal = state
    value = runnable(goal)
    value["steps"][0]["query"] = "previously accepted content now blocked by policy"
    await runtime.save_goal(goal["goal_id"], value, tenant)

    class Detector:
        async def ascan(self, content, *, event_type, location, **kwargs):
            return ThreatScan(
                event_type,
                location,
                (
                    {
                        "finding_id": str(uuid4()),
                        "rule_id": "TEST-POLICY",
                        "severity": "high",
                        "title": "Current rule",
                        "enforcement_action": "blocked",
                    },
                ),
                True,
            )

    class NoModel:
        async def stream(self, system, prompt):
            raise AssertionError("blocked queued work must not reach the model")
            yield "unreachable"

    await execute_goal(
        goal["goal_id"],
        runtime,
        Router(NoModel()),
        object(),
        tenant_id=tenant,
        conversation_max_turns=20,
        conversation_ttl_seconds=3_600,
        threat_detector=Detector(),
        settings=Settings(),
    )
    result = await runtime.get_goal(goal["goal_id"], tenant)
    assert result["status"] == "failed"
    assert "ThreatBlockedError" in result["error"]
    records = await runtime.read_goal_events(goal["goal_id"], tenant_id=tenant, block_ms=1)
    threat = [event for _, event in records if event.get("type") == "threat_detection"]
    assert threat
    assert value["steps"][0]["query"] not in str(threat)


@pytest.mark.asyncio
async def test_atomic_tenant_budget_rejects_one_of_two_concurrent_goal_reservations(state):
    runtime, store, tenant, goal = state
    other = {**goal, "goal_id": str(uuid4())}
    await store.submit_manual(other)
    one, two = await asyncio.gather(
        store.claim(goal["goal_id"], tenant, "one", 60_000),
        store.claim(other["goal_id"], tenant, "two", 60_000),
    )
    outcomes = await asyncio.gather(
        store.reserve_budget(one, "one", 1_500),
        store.reserve_budget(two, "two", 1_500),
        return_exceptions=True,
    )
    assert sum(result is None for result in outcomes) == 1
    assert sum(isinstance(result, RuntimeError) for result in outcomes) == 1
    day = datetime.now(UTC).strftime("%Y-%m-%d")
    assert int(await runtime._client.get(f"tenant:{tenant}:quota:tokens:{day}")) == 1_000


@pytest.mark.asyncio
async def test_automation_lifecycle_is_durable_without_raw_goal_content(state):
    runtime, store, tenant, goal = state
    automation = str(uuid4())
    await runtime.save_goal(goal["goal_id"], {**goal, "automation_id": automation}, tenant)
    active = await store.claim(goal["goal_id"], tenant, "worker", 60_000)
    active["status"] = "completed"
    await store.finish(active, "worker", {"type": "goal_completed", "goal": active})
    rows = await runtime._client.xrange(f"tenant:{tenant}:automation:{automation}:events")
    assert len(rows) == 2
    encoded = str(rows)
    assert "goal_completed" in encoded
    assert goal["objective"] not in encoded
    assert "goal_id" in encoded


@pytest.mark.asyncio
async def test_slot_heartbeat_preserves_concurrency_beyond_the_original_key_ttl(state):
    runtime, store, tenant, goal = state
    assert await runtime.acquire_run_slot(tenant, "live", 1, ttl_seconds=2)
    # Renew at production's minimum 60-second heartbeat horizon. This proves
    # the key TTL is refreshed without depending on sub-second scheduler timing.
    assert await store.renew_run_slot(tenant, "live", 60)
    await asyncio.sleep(2.1)
    # More time elapsed than the original Redis key TTL of two seconds.
    key = f"tenant:{tenant}:quota:active-runs"
    assert await runtime._client.exists(key)
    assert await runtime._client.ttl(key) >= 1
    assert not await runtime.acquire_run_slot(tenant, "competitor", 1, ttl_seconds=2)


@pytest.mark.asyncio
async def test_short_slot_heartbeat_keeps_longer_slots_and_cannot_recreate_a_revoked_slot(state):
    runtime, store, tenant, goal = state
    assert await runtime.acquire_run_slot(tenant, "long", 2, ttl_seconds=120)
    assert await runtime.acquire_run_slot(tenant, "short", 2, ttl_seconds=2)
    assert await store.renew_run_slot(tenant, "short", 2)
    key = f"tenant:{tenant}:quota:active-runs"
    assert await runtime._client.ttl(key) >= 110
    await runtime.release_run_slot(tenant, "short")
    assert not await store.renew_run_slot(tenant, "short", 2)
    assert await runtime._client.zscore(key, "short") is None


@pytest.mark.asyncio
async def test_failed_goal_and_safe_internal_event_commit_together_without_a_later_emit(state):
    runtime, store, tenant, goal = state
    active = await store.claim(goal["goal_id"], tenant, "worker", 60_000)
    active.update(
        status="failed",
        error_type="DeploymentFailure",
        error="DeploymentFailure: private deployment output",
    )
    assert await store.finish(active, "worker", {"type": "goal_failed", "error": active["error"]})
    # Simulate process exit immediately after the finish transaction returns:
    # there is deliberately no worker emit_internal_event call here.
    assert not await store.finish(active, "worker", {"type": "goal_failed"})
    rows = await runtime._client.xrange("automation:internal-events")
    own = [
        json.loads(fields["payload"])
        for _, fields in rows
        if fields["tenant_id"] == tenant
        and json.loads(fields["payload"]).get("data", {}).get("goal_id") == goal["goal_id"]
    ]
    assert len(own) == 1
    assert own[0]["type"] == "goal.failed"
    assert own[0]["data"]["error_type"] == "DeploymentFailure"
    assert "private deployment output" not in json.dumps(own)


@pytest.mark.asyncio
async def test_failed_automation_goal_cannot_chain_another_internal_trigger(state):
    runtime, store, tenant, goal = state
    await runtime.save_goal(goal["goal_id"], {**goal, "automation_id": "source-job"}, tenant)
    active = await store.claim(goal["goal_id"], tenant, "worker", 60_000)
    active.update(status="failed", error_type="DeploymentFailure")
    assert await store.finish(active, "worker", {"type": "goal_failed"})
    rows = await runtime._client.xrange("automation:internal-events")
    assert not [fields for _, fields in rows if fields["tenant_id"] == tenant]


@pytest.mark.asyncio
async def test_terminal_accounting_recovers_a_crash_immediately_after_finish(state):
    runtime, store, tenant, goal = state
    active = await store.claim(goal["goal_id"], tenant, "worker", 60_000)
    await store.reserve_budget(active, "worker", 2_000)
    active["usage"] = {"policy_charged_tokens": 350}
    await store.save_owned(active, "worker")
    active["status"] = "completed"
    assert await store.finish(active, "worker", {"type": "goal_completed"})
    key = f"tenant:{tenant}:quota:tokens:{active['token_reservation']['day']}"
    assert int(await runtime._client.get(key)) == 1_000
    assert not (await runtime.get_goal(goal["goal_id"], tenant))["token_reservation"]["reconciled"]
    # No worker finally block ran; the scheduler's normal recovery path repairs it.
    recovery = GoalExecutionStore(runtime)
    await recovery.recover_expired()
    assert int(await runtime._client.get(key)) == 350
    assert (await runtime.get_goal(goal["goal_id"], tenant))["token_reservation"]["reconciled"]
    await asyncio.gather(recovery.recover_expired(), recovery.reconcile_terminals())
    assert int(await runtime._client.get(key)) == 350


@pytest.mark.asyncio
async def test_terminal_accounting_recovers_crash_between_cancel_and_refund(state, monkeypatch):
    runtime, store, tenant, goal = state
    active = await store.claim(goal["goal_id"], tenant, "worker", 60_000)
    await store.reserve_budget(active, "worker", 2_000)
    active["usage"] = {"policy_charged_tokens": 250}
    await store.save_owned(active, "worker")

    async def crash_before_refund(*args, **kwargs):
        raise RuntimeError("simulated process exit after terminal cancel commit")

    monkeypatch.setattr(store, "reconcile_budget", crash_before_refund)
    with pytest.raises(RuntimeError, match="simulated process exit"):
        await store.control(tenant, goal["goal_id"], "cancel", "operator")
    cancelled = await runtime.get_goal(goal["goal_id"], tenant)
    assert cancelled["status"] == "cancelled"
    assert not cancelled["token_reservation"]["reconciled"]
    key = f"tenant:{tenant}:quota:tokens:{cancelled['token_reservation']['day']}"
    assert int(await runtime._client.get(key)) == 1_000
    recovery = GoalExecutionStore(runtime)
    await asyncio.gather(recovery.recover_expired(), recovery.reconcile_terminals())
    assert int(await runtime._client.get(key)) == 250
    assert (await runtime.get_goal(goal["goal_id"], tenant))["token_reservation"]["reconciled"]
    await recovery.recover_expired()
    assert int(await runtime._client.get(key)) == 250
