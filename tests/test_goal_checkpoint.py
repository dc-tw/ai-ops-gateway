"""Durable human approvals survive closing and reopening the real Redis saver."""

import os
from uuid import uuid4

import pytest
from langgraph.checkpoint.redis.aio import AsyncRedisSaver

from app.core.config import Settings
from app.core.goal_execution import GoalExecutionStore
from app.core.redis_runtime import RedisRuntime
from app.tools.registry import ToolSpec
from app.worker import execute_goal


class StatelessBrowserModel:
    async def complete(self, system, prompt):
        # Derive the plan from saved observations, never an in-memory call counter.
        if '"selector": "#second"' in prompt:
            return '{"tool":null,"arguments":{}}'
        selector = "#second" if '"selector": "#first"' in prompt else "#first"
        return '{"tool":"browser_click","arguments":{"selector":"' + selector + '"}}'

    async def stream(self, system, prompt):
        yield "Both independently approved browser operations completed successfully."


class Router:
    def get(self, selection):
        # Recreate the model too; only the Redis graph state carries observations.
        return StatelessBrowserModel()


class BrowserWrites:
    def __init__(self, calls):
        self.calls = calls

    def specs(self):
        return [ToolSpec("browser_click", "click a button", requires_approval=True)]

    async def invoke(self, name, arguments):
        self.calls.append((name, dict(arguments)))
        return {"clicked": arguments["selector"]}


@pytest.mark.asyncio
async def test_redis_checkpoint_reopens_between_each_independent_action_approval():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL to a Redis JSON/Search test server")
    runtime = RedisRuntime(url)
    tenant, goal_id = f"checkpoint-{uuid4().hex}", str(uuid4())
    store, calls = GoalExecutionStore(runtime), []
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
    await store.submit_manual(goal)

    async def run_with_fresh_checkpoint_connection():
        async with AsyncRedisSaver(redis_url=url) as saver:
            await execute_goal(
                goal_id,
                runtime,
                Router(),
                object(),
                tenant_id=tenant,
                conversation_max_turns=20,
                conversation_ttl_seconds=3_600,
                tool_executor=BrowserWrites(calls),
                checkpointer=saver,
                settings=Settings(),
            )

    try:
        await run_with_fresh_checkpoint_connection()
        first = await runtime.get_goal(goal_id, tenant)
        assert first["status"] == "waiting_approval", first.get("error")
        assert calls == []
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
        assert finished["token_reservation"]["reconciled"]
    finally:
        await runtime.close()
