import hashlib
import time

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import Command

from app.core.execution_policy import BudgetedModel, PolicyToolExecutor, TokenBudgetExceeded
from app.graph.workflow import build_agent_graph, initial_state
from app.llm.client import ModelResult
from app.tools.registry import ToolSpec


class MeteredModel:
    def __init__(self):
        self.calls = 0

    async def complete_result(self, system, prompt):
        self.calls += 1
        return ModelResult(
            "result",
            {
                "input_tokens": 10,
                "output_tokens": 2,
                "total_tokens": 12,
                "provider_reported": True,
            },
        )


async def test_budget_tracks_planning_and_resumes_without_resetting():
    saved = []

    async def persist(usage):
        saved.append(usage)

    model = MeteredModel()
    budgeted = BudgetedModel(model, 500, max_output_tokens=100, on_usage=persist)
    await budgeted.complete("system", "plan")
    assert budgeted.usage["total_tokens"] == 12
    assert saved[0]["policy_charged_tokens"] > saved[-1]["policy_charged_tokens"]
    resumed = BudgetedModel(model, 500, existing_usage=budgeted.usage, max_output_tokens=100)
    await resumed.complete("system", "answer")
    assert resumed.usage["total_tokens"] == 24
    assert resumed.usage["model_calls"] == 2
    with pytest.raises(TokenBudgetExceeded):
        await resumed.complete("system", "x" * 500)
    assert model.calls == 2


async def test_failed_model_call_retains_reservation():
    class Failing:
        async def complete(self, system, prompt):
            raise ConnectionError("provider disconnected")

    model = BudgetedModel(Failing(), 500, max_output_tokens=100)
    with pytest.raises(ConnectionError):
        await model.complete("system", "plan")
    assert model.usage["policy_charged_tokens"] > 350
    with pytest.raises(TokenBudgetExceeded):
        await model.complete("system", "retry")


@pytest.mark.parametrize(
    "usage",
    [
        {"input_tokens": -50, "output_tokens": -50, "total_tokens": -100},
        {"total_tokens": 1},
        {"input_tokens": 10, "output_tokens": 8, "total_tokens": 1},
        {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
    ],
)
async def test_malformed_provider_usage_keeps_conservative_charge_and_cannot_reset_budget(usage):
    class Malformed:
        calls = 0

        async def complete_result(self, system, prompt):
            self.calls += 1
            return ModelResult("result", {**usage, "provider_reported": True})

    provider = Malformed()
    model = BudgetedModel(provider, 500, max_output_tokens=100)
    await model.complete("system", "plan")
    assert model.usage["provider_reported"] is False
    assert model.usage["provider_reported_calls"] == 0
    assert model.usage["policy_charged_tokens"] == len(b"systemplan") + 256 + 100
    with pytest.raises(TokenBudgetExceeded):
        await model.complete("system", "retry")
    assert provider.calls == 1


class Tools:
    def __init__(self):
        self.calls = []

    def specs(self):
        return [
            ToolSpec("browser_click", "click", requires_approval=True),
            ToolSpec("scrape_dynamic_page", "read"),
        ]

    async def invoke(self, name, args):
        self.calls.append((name, args))
        text = "The approved operation succeeded."
        return {
            "url": "https://example.com/result",
            "text": text,
            "characters": len(text),
            "retrieved_at": "2026-10-25T00:00:00+00:00",
            "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
        }


async def test_delegated_scope_and_revoked_owner_prevent_tool_invocation():
    tools = Tools()
    restricted = PolicyToolExecutor(
        tools, {"allowed_tools": ["scrape_dynamic_page"], "allowed_domains": ["example.com"]}
    )
    with pytest.raises(PermissionError):
        await restricted.invoke("browser_click", {})
    with pytest.raises(PermissionError):
        await restricted.invoke("scrape_dynamic_page", {"url": "https://elsewhere.example"})

    class Revoked:
        async def get_goal(self, goal_id, tenant):
            return {
                "status": "running",
                "execution_owner": "new-owner",
                "execution_lease_until": time.time() + 60,
            }

    stale = PolicyToolExecutor(
        tools,
        {"allowed_tools": ["scrape_dynamic_page"]},
        runtime=Revoked(),
        execution_owner="old-owner",
    )
    with pytest.raises(PermissionError):
        await stale.invoke("scrape_dynamic_page", {"url": "https://example.com"})
    assert tools.calls == []


class TwoActions:
    def __init__(self):
        self.responses = iter(
            [
                '{"tool":"browser_click","arguments":{"session_id":"s1","selector":"#one"}}',
                '{"tool":"browser_click","arguments":{"session_id":"s1","selector":"#two"}}',
                '{"tool":null,"arguments":{}}',
            ]
        )

    async def complete(self, system, prompt):
        return next(self.responses)

    async def stream(self, system, prompt):
        yield "The approved operation succeeded [source:1]."


async def test_each_write_requires_exact_approval_without_replaying_previous_write():
    tools = Tools()
    graph = build_agent_graph(
        TwoActions(), object(), tool_executor=tools, checkpointer=MemorySaver()
    )
    config = {"configurable": {"thread_id": "two-exact-actions"}}
    events = [
        event
        async for event in graph.astream(
            initial_state(
                "run",
                "Click buttons",
                None,
                0,
                interactive_approval=True,
                require_bound_approval=True,
            ),
            config=config,
            stream_mode="custom",
        )
    ]
    first = next(e for e in events if e.get("type") == "approval_required")
    assert tools.calls == []
    events = [
        event
        async for event in graph.astream(
            Command(resume={"approved": True, "action_id": first["action_id"]}),
            config=config,
            stream_mode="custom",
        )
    ]
    second = next(
        e
        for e in events
        if e.get("type") == "approval_required" and e["action_id"] != first["action_id"]
    )
    assert len(tools.calls) == 1
    awaitable = graph.astream(
        Command(resume={"approved": True, "action_id": second["action_id"]}),
        config=config,
        stream_mode="custom",
    )
    final_events = [event async for event in awaitable]
    assert [args["selector"] for _, args in tools.calls] == ["#one", "#two"]
    assert any(e.get("type") == "completed" for e in final_events)


async def test_approval_for_different_parameters_cannot_authorize_write():
    tools = Tools()
    graph = build_agent_graph(
        TwoActions(), object(), tool_executor=tools, checkpointer=MemorySaver()
    )
    config = {"configurable": {"thread_id": "wrong-action"}}
    events = [
        e
        async for e in graph.astream(
            initial_state(
                "run",
                "Click button",
                None,
                0,
                interactive_approval=True,
                require_bound_approval=True,
            ),
            config=config,
            stream_mode="custom",
        )
    ]
    assert any(e.get("type") == "approval_required" for e in events)
    events = [
        e
        async for e in graph.astream(
            Command(resume={"approved": True, "action_id": "different-arguments"}),
            config=config,
            stream_mode="custom",
        )
    ]
    assert tools.calls == []
    assert any(e.get("status") == "failed" for e in events)
