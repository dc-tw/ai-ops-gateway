import asyncio
import json
import os
import time
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.agents import AgentStore
from app.core.chief import allocate_budget, prepare_chief, prepare_summary
from app.core.goal_execution import GoalExecutionStore
from app.core.redis_runtime import RedisRuntime

SETTINGS = SimpleNamespace(
    pii_redaction=False, request_token_budget=16_000, content_blocked_patterns=[]
)


def worker_records(tenant="t"):
    return [
        {
            "agent_id": agent_id,
            "tenant_id": tenant,
            "name": name,
            "description": description,
            "persona": persona,
            "role": "worker",
            "model": model,
            "enabled": True,
            "delegate_agent_ids": [],
            "version": 1,
            "policy": {
                "allowed_tools": tools,
                "allowed_domains": ["example.com"],
                "token_budget": 20_000,
                "max_runs_per_day": 24,
                "approval_timeout_seconds": 86_400,
            },
            "created_by": "admin",
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
        }
        for agent_id, name, description, persona, model, tools in [
            (
                "analyst",
                "Analyst",
                "Investigates deployment failures",
                "分析失敗證據，區分觀察與推測。",
                "analysis-model",
                ["scrape_dynamic_page"],
            ),
            (
                "editor",
                "Editor",
                "Writes concise incident reports",
                "整理簡明的中文事件摘要。",
                "writing-model",
                ["browser_start"],
            ),
        ]
    ]


def parent(tenant="t"):
    return {
        "goal_id": str(uuid4()),
        "tenant_id": tenant,
        "agent_id": "chief",
        "objective": "Analyze the failed deployment and produce an incident report",
        "model": "chief-model",
        "status": "running",
        "created_by": "operator",
        "execution_owner": "owner",
        "execution_lease_until": time.time() + 300,
        "steps": [],
        "current_step": 0,
        "max_retries": 0,
        "usage": {},
        "policy": {"token_budget": 20_000, "allowed_tools": []},
        "agent_config": {
            "agent_id": "chief",
            "role": "chief",
            "delegate_agent_ids": ["analyst", "editor"],
        },
        "token_reservation": {
            "reserved": 20_000,
            "base_usage": 0,
            "day": "2040-01-01",
            "reconciled": False,
        },
    }


class PlanModel:
    def __init__(self, response=None):
        self.response = response or json.dumps(
            {
                "assignments": [
                    {"agent_id": "analyst", "query": "Analyze deployment evidence"},
                    {"agent_id": "editor", "query": "Prepare an incident report draft"},
                ]
            }
        )
        self.usage = {"policy_charged_tokens": 2_000, "model_calls": 1}
        self.calls = []

    async def complete(self, system, prompt):
        self.calls.append((system, prompt))
        return self.response


class FakeAgents:
    def __init__(self, records):
        self.records = {record["agent_id"]: deepcopy(record) for record in records}

    async def get(self, tenant, agent_id):
        result = self.records.get(agent_id)
        return deepcopy(result) if result and result["tenant_id"] == tenant else None


class FakeDelegation:
    def __init__(self, runtime):
        self.runtime = runtime
        self.children = []
        self.crash_once = False
        self.ready = {"status": "pending", "results": []}

    async def create_children(self, goal, owner, children, plan_usage):
        if self.crash_once:
            self.crash_once = False
            raise RuntimeError("simulated crash before child transaction")
        self.children = deepcopy(children)
        self.runtime.goal.update(status="waiting_children", delegation_planned=True)
        return True

    async def children_ready(self, goal_id, tenant):
        return deepcopy(self.ready)


class FakeRuntime:
    def __init__(self, goal=None):
        self.goal = deepcopy(goal or parent())
        self.agent_store = FakeAgents(worker_records(self.goal["tenant_id"]))
        self.delegation_store = FakeDelegation(self)
        self.lock = asyncio.Lock()

    async def goal_execution_mutate(self, tenant, goal_id, change):
        async with self.lock:
            result = change(deepcopy(self.goal))
            if result.goal is not None:
                self.goal = deepcopy(result.goal)
            return deepcopy(result.result)

    async def goal_execution_resize_budget(self, goal, owner, new_budget):
        assert self.goal["execution_owner"] == owner
        assert new_budget <= self.goal["policy"]["token_budget"]
        self.goal["policy"]["token_budget"] = new_budget
        self.goal["token_reservation"]["reserved"] = new_budget
        goal["policy"]["token_budget"] = new_budget
        goal["token_budget"] = new_budget
        goal["token_reservation"] = deepcopy(self.goal["token_reservation"])

    async def get_goal(self, goal_id, tenant):
        return deepcopy(self.goal)


@pytest.mark.parametrize("count", range(1, 9))
def test_shared_budget_includes_chief_planning_summary_and_all_children(count):
    chief, children = allocate_budget(20_000, 2_000, [20_000] * count)
    assert chief + sum(children) == 20_000
    assert chief >= 2_000 + 6_000
    assert all(256 <= cap <= 20_000 for cap in children)


def test_small_worker_cap_does_not_expand_and_unusable_team_budget_rejects():
    chief, children = allocate_budget(20_000, 2_000, [500, 1_000])
    assert children == [500, 1_000]
    assert chief == 18_500
    with pytest.raises(ValueError):
        allocate_budget(1_000, 900, [20_000])
    with pytest.raises(ValueError):
        allocate_budget(20_000, 0, [255])


@pytest.mark.asyncio
async def test_chief_assigns_approved_workers_with_independent_persona_model_and_tools():
    runtime, model = FakeRuntime(), PlanModel()
    goal = deepcopy(runtime.goal)
    assert await prepare_chief(goal, model, runtime, "owner", SETTINGS, None)
    children = runtime.delegation_store.children
    assert [child["model"] for child in children] == ["analysis-model", "writing-model"]
    assert children[0]["policy"]["allowed_tools"] == ["scrape_dynamic_page"]
    assert children[1]["policy"]["allowed_tools"] == ["browser_start"]
    assert children[0]["agent_config"]["persona"] != children[1]["agent_config"]["persona"]
    assert goal["policy"]["allowed_tools"] == []
    assert goal["chief_budget"] + sum(child["token_budget"] for child in children) == 20_000
    assert len(model.calls) == 1
    assert "approved_workers" in model.calls[0][1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        '{"assignments":[{"agent_id":"unapproved","query":"do work"}]}',
        '{"assignments":[]}',
        '{"assignments":[{"agent_id":"analyst","query":"task","model":"secret"}]}',
        "not JSON containing a private planner fragment",
    ],
)
async def test_unapproved_or_invalid_plans_never_create_children(response):
    runtime = FakeRuntime()
    with pytest.raises(ValueError) as error:
        await prepare_chief(
            deepcopy(runtime.goal), PlanModel(response), runtime, "owner", SETTINGS, None
        )
    assert runtime.delegation_store.children == []
    assert "private planner fragment" not in str(error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("enabled", False), ("role", "chief")])
async def test_disabled_or_recursive_worker_is_rejected_before_any_planning_call(field, value):
    runtime, model = FakeRuntime(), PlanModel()
    runtime.agent_store.records["analyst"][field] = value
    with pytest.raises(ValueError, match="enabled Worker"):
        await prepare_chief(deepcopy(runtime.goal), model, runtime, "owner", SETTINGS, None)
    assert model.calls == []


@pytest.mark.asyncio
async def test_prepared_plan_survives_retry_without_replanning_or_changing_child_scopes():
    runtime, first_model = FakeRuntime(), PlanModel()
    runtime.delegation_store.crash_once = True
    with pytest.raises(RuntimeError, match="simulated crash"):
        await prepare_chief(deepcopy(runtime.goal), first_model, runtime, "owner", SETTINGS, None)
    saved = deepcopy(runtime.goal)
    assert saved["chief_plan_state"] == "prepared"
    retry_model = PlanModel()
    assert await prepare_chief(saved, retry_model, runtime, "owner", SETTINGS, None)
    assert retry_model.calls == []
    assert len(runtime.delegation_store.children) == 2


@pytest.mark.asyncio
async def test_planner_retries_are_bounded_without_resetting_the_team_budget():
    runtime, model = FakeRuntime(), PlanModel()
    runtime.goal["chief_plan_state"] = "planning"
    runtime.goal["chief_plan_attempts"] = 3
    with pytest.raises(RuntimeError, match="three bounded attempts"):
        await prepare_chief(deepcopy(runtime.goal), model, runtime, "owner", SETTINGS, None)
    assert model.calls == []


@pytest.mark.asyncio
async def test_interrupted_planning_can_resume_with_prior_usage_and_one_shared_cap():
    runtime, model = FakeRuntime(), PlanModel()
    runtime.goal.update(
        chief_plan_state="planning",
        chief_plan_attempts=1,
        team_token_budget=20_000,
        usage={"policy_charged_tokens": 3_000, "model_calls": 1},
    )
    model.usage = {"policy_charged_tokens": 5_000, "model_calls": 2}
    goal = deepcopy(runtime.goal)
    assert await prepare_chief(goal, model, runtime, "owner", SETTINGS, None)
    assert goal["chief_plan_attempts"] == 2
    assert goal["team_token_budget"] == 20_000
    assert len(model.calls) == 1
    children = runtime.delegation_store.children
    assert len(children) == 2
    assert goal["chief_budget"] >= 5_000
    assert goal["chief_budget"] + sum(child["token_budget"] for child in children) == 20_000


@pytest.mark.asyncio
async def test_resumed_paused_parent_waits_for_unfinished_children_without_replanning():
    runtime, model = FakeRuntime(), PlanModel()
    runtime.goal["delegation_planned"] = True
    goal = deepcopy(runtime.goal)
    assert await prepare_chief(goal, model, runtime, "owner", SETTINGS, None)
    assert runtime.goal["status"] == "waiting_children"
    assert runtime.goal["execution_owner"] is None
    assert model.calls == []


def test_summary_bounds_untrusted_results_and_preserves_each_source_id():
    goal = parent()
    goal["policy"]["token_budget"] = 8_000
    goal["usage"] = {"policy_charged_tokens": 2_000}
    goal["delegation_results"] = [
        {
            "goal_id": str(uuid4()),
            "agent_id": agent_id,
            "status": "completed",
            "answer": "不可信工作資料" * 10_000,
        }
        for agent_id in ("analyst", "editor")
    ]
    updates = prepare_summary(goal)
    query = updates["steps"][0]["query"]
    assert len(query.encode("utf-8")) < 8_000
    assert all(result["goal_id"] in query for result in goal["delegation_results"])
    assert all(item["answer_truncated"] for item in updates["delegation_summary_sources"])
    assert "<DELEGATED_RESULTS>" in query
    assert updates["synthesis_prepared"]
    assert prepare_summary({**goal, **updates}) == {}


def test_summary_cannot_turn_a_failed_child_into_success():
    goal = parent()
    goal["delegation_results"] = [{"goal_id": "failed", "status": "failed"}]
    with pytest.raises(ValueError, match="every delegated Worker"):
        prepare_summary(goal)


@pytest.mark.asyncio
async def test_real_redis_resize_refunds_once_and_never_raises_chief_cap():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL for atomic Chief quota tests")
    runtime = RedisRuntime(url)
    tenant = f"chief-resize-{uuid4().hex}"
    goal = parent(tenant)
    goal.update(status="queued", execution_owner=None, execution_lease_until=0)
    goal.pop("token_reservation")
    store = GoalExecutionStore(runtime)
    try:
        await store.submit_manual(goal)
        goal = await store.claim(goal["goal_id"], tenant, "owner", 60_000)
        await store.reserve_budget(goal, "owner", 100_000)
        goal["usage"] = {"policy_charged_tokens": 2_000}
        await store.save_owned(goal, "owner")
        await store.resize_budget(goal, "owner", 8_000)
        quota = f"tenant:{tenant}:quota:tokens:{goal['token_reservation']['day']}"
        assert int(await runtime._client.get(quota)) == 8_000
        await store.resize_budget(goal, "owner", 8_000)
        assert int(await runtime._client.get(quota)) == 8_000
        with pytest.raises(ValueError):
            await store.resize_budget(goal, "owner", 9_000)
        with pytest.raises(ValueError):
            await store.resize_budget(goal, "owner", 1_000)
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_real_redis_chief_enqueues_scoped_children_inside_one_team_budget():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL for durable Chief delegation tests")
    runtime = RedisRuntime(url)
    tenant = f"chief-team-{uuid4().hex}"
    store, agent_store = GoalExecutionStore(runtime), AgentStore(runtime)
    try:
        for record in worker_records(tenant):
            await agent_store.create(record)
        goal = parent(tenant)
        goal.update(status="queued", execution_owner=None, execution_lease_until=0)
        goal.pop("token_reservation")
        await store.submit_manual(goal)
        active = await store.claim(goal["goal_id"], tenant, "owner", 60_000)
        await store.reserve_budget(active, "owner", 100_000)
        assert await prepare_chief(active, PlanModel(), runtime, "owner", SETTINGS, None)
        saved = await runtime.get_goal(goal["goal_id"], tenant)
        assert saved["status"] == "waiting_children"
        assert len(saved["children_goal_ids"]) == 2
        children = [
            await runtime.get_goal(child_id, tenant) for child_id in saved["children_goal_ids"]
        ]
        assert saved["chief_budget"] + sum(c["policy"]["token_budget"] for c in children) == 20_000
        assert [child["agent_config"]["role"] for child in children] == ["worker", "worker"]
        assert [child["model"] for child in children] == ["analysis-model", "writing-model"]
    finally:
        await runtime.close()
