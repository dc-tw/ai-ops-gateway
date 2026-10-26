"""Durable human approvals survive closing and reopening the real Redis saver."""

import os
from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from langgraph.checkpoint.redis.aio import AsyncRedisSaver

from app.agents import AgentStore, apply_agent
from app.automations import AutomationPolicy
from app.core.config import Settings
from app.core.goal_execution import GoalExecutionStore
from app.core.redis_runtime import RedisRuntime
from app.core.skill_store import SkillContent, SkillStore
from app.tools.registry import ToolSpec
from app.worker import execute_goal


class StatelessBrowserModel:
    def __init__(self, systems):
        self.systems = systems

    async def complete(self, system, prompt):
        self.systems.append(system)
        # Derive the plan from saved observations, never an in-memory call counter.
        if '"selector": "#second"' in prompt:
            return '{"tool":null,"arguments":{}}'
        selector = "#second" if '"selector": "#first"' in prompt else "#first"
        return '{"tool":"browser_click","arguments":{"selector":"' + selector + '"}}'

    async def stream(self, system, prompt):
        self.systems.append(system)
        yield "Both independently approved browser operations completed successfully."


class Router:
    def __init__(self, systems):
        self.systems = systems

    def get(self, selection):
        # Recreate the model too; only the Redis graph state carries observations.
        return StatelessBrowserModel(self.systems)


class BrowserWrites:
    def __init__(self, calls):
        self.calls = calls

    def specs(self):
        return [ToolSpec("browser_click", "click a button", requires_approval=True)]

    async def invoke(self, name, arguments):
        self.calls.append((name, dict(arguments)))
        return {"clicked": arguments["selector"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("with_skills", [False, True], ids=["legacy", "pinned-skills"])
async def test_redis_checkpoint_reopens_between_each_independent_action_approval(with_skills):
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL to a Redis JSON/Search test server")
    runtime = RedisRuntime(url)
    tenant, goal_id = f"checkpoint-{uuid4().hex}", str(uuid4())
    store, calls, systems = GoalExecutionStore(runtime), [], []
    goal = {
        "goal_id": goal_id,
        "tenant_id": tenant,
        "status": "queued",
        "objective": "Use browser to click two requested buttons",
        "created_by": "reviewer",
        "model": "test",
        "max_retries": 0,
        "max_tool_rounds": 5,
        "current_step": 0,
        "step_deadline_seconds": 60,
        "policy": {"token_budget": 20_000, "allowed_tools": ["browser_click"]},
        "steps": [
            {
                "id": str(uuid4()),
                "query": "Use browser to click two requested buttons",
                "status": "pending",
                "answer": "",
                "error": "",
                "target_url": None,
            }
        ],
    }
    pinned_snapshots = []
    old_instructions = "Pinned revision one: retain each independently approved action boundary."
    new_instructions = "Replacement revision two must never enter the in-flight Goal."
    if with_skills:
        skill_store = SkillStore(runtime)
        content = SkillContent(
            name="checkpoint-procedure",
            display_name="Checkpoint procedure",
            description="Preserve the independently approved browser action sequence.",
            instructions=old_instructions,
            required_tools=["browser_click"],
            agent_roles=["worker"],
        ).model_dump()
        skill = await skill_store.create(tenant, content, "reviewer")
        agent_id = str(uuid4())
        now = datetime.now(UTC).isoformat()
        await AgentStore(runtime).create(
            {
                "agent_id": agent_id,
                "tenant_id": tenant,
                "name": "Checkpoint Worker",
                "description": "Performs independently approved browser operations.",
                "persona": "Preserve exact approval boundaries and report the result.",
                "role": "worker",
                "model": "test",
                "enabled": True,
                "version": 1,
                "delegate_agent_ids": [],
                "skills": [{"skill_id": skill["skill_id"], "revision": 1}],
                "policy": AutomationPolicy(
                    allowed_tools=["browser_click"], token_budget=20_000, max_runs_per_day=1
                ).model_dump(),
                "created_by": "reviewer",
                "created_at": now,
                "updated_at": now,
            }
        )
        goal = await apply_agent(runtime, tenant, agent_id, goal)
        pinned_snapshots = deepcopy(goal["agent_config"]["skill_snapshots"])
        assert pinned_snapshots[0]["revision"] == 1
        assert pinned_snapshots[0]["content_sha256"] == skill["content_sha256"]
    await store.submit_manual(goal)

    async def run_with_fresh_checkpoint_connection():
        prior_calls = len(systems)
        async with AsyncRedisSaver(redis_url=url) as saver:
            await execute_goal(
                goal_id,
                runtime,
                Router(systems),
                object(),
                tenant_id=tenant,
                conversation_max_turns=20,
                conversation_ttl_seconds=3_600,
                tool_executor=BrowserWrites(calls),
                checkpointer=saver,
                settings=Settings(),
            )
        if with_skills:
            assert len(systems) > prior_calls
            for system in systems[prior_calls:]:
                assert old_instructions in system
                assert new_instructions not in system
            saved = await runtime.get_goal(goal_id, tenant)
            assert saved["agent_config"]["skill_snapshots"] == pinned_snapshots
            assert saved["policy"]["token_budget"] == 20_000

    try:
        await run_with_fresh_checkpoint_connection()
        first = await runtime.get_goal(goal_id, tenant)
        assert first["status"] == "waiting_approval", first.get("error")
        assert calls == []
        reservation = deepcopy(first["token_reservation"])
        quota_key = f"tenant:{tenant}:quota:tokens:{reservation['day']}"
        assert reservation["reserved"] == 20_000
        assert int(await runtime._client.get(quota_key)) == 20_000
        if with_skills:
            published = await skill_store.publish(
                tenant,
                skill["skill_id"],
                {**content, "instructions": new_instructions},
                skill["version"],
                "reviewer",
            )
            assert published["revision"] == 2
            assert published["content_sha256"] != pinned_snapshots[0]["content_sha256"]
        pending_one = first["pending_approval"]
        assert pending_one["arguments"]["selector"] == "#first"
        await store.decide_approval(
            tenant, goal_id, pending_one["approval_id"], True, "first action reviewed", "reviewer"
        )

        # Prior saver, model and executor are all closed or discarded.
        await run_with_fresh_checkpoint_connection()
        second = await runtime.get_goal(goal_id, tenant)
        assert second["status"] == "waiting_approval", second.get("error")
        assert calls == [("browser_click", {"selector": "#first"})]
        assert second["token_reservation"] == reservation
        assert second["usage"]["model_calls"] > first["usage"]["model_calls"]
        assert second["usage"]["policy_charged_tokens"] > first["usage"]["policy_charged_tokens"]
        assert int(await runtime._client.get(quota_key)) == 20_000
        pending_two = second["pending_approval"]
        assert pending_two["arguments"]["selector"] == "#second"
        assert pending_two["approval_id"] != pending_one["approval_id"]
        assert pending_two["action_id"] != pending_one["action_id"]
        await store.decide_approval(
            tenant, goal_id, pending_two["approval_id"], True, "second action reviewed", "reviewer"
        )

        await run_with_fresh_checkpoint_connection()
        finished = await runtime.get_goal(goal_id, tenant)
        assert finished["status"] == "completed", finished.get("error")
        assert calls == [
            ("browser_click", {"selector": "#first"}),
            ("browser_click", {"selector": "#second"}),
        ]
        assert finished["usage"]["model_calls"] >= 4
        assert finished["usage"]["model_calls"] > second["usage"]["model_calls"]
        charged = finished["usage"]["policy_charged_tokens"]
        assert second["usage"]["policy_charged_tokens"] < charged <= 20_000
        assert finished["token_reservation"]["reserved"] == reservation["reserved"]
        assert finished["token_reservation"]["base_usage"] == reservation["base_usage"]
        assert finished["token_reservation"]["reconciled"]
        assert int(await runtime._client.get(quota_key)) == charged
        if with_skills:
            counter = runtime._tenant_key(
                tenant,
                f"agents:daily-runs:{agent_id}:{finished['agent_admitted_day']}",
            )
            assert int(await runtime._client.get(counter)) == 1
    finally:
        await runtime.close()
