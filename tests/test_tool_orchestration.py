import hashlib
from collections.abc import AsyncIterator

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import Command

from app.graph.workflow import build_agent_graph, initial_state
from app.tools.registry import MCPBrowserToolExecutor, ToolSpec


class PlanningModel:
    def __init__(self, responses: list[str]) -> None:
        self.responses = iter(responses)

    async def complete(self, system: str, prompt: str) -> str:
        return next(self.responses)

    async def stream(self, system: str, prompt: str) -> AsyncIterator[str]:
        yield next(self.responses)


class RecordingTools:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def specs(self) -> list[ToolSpec]:
        return [ToolSpec("scrape_dynamic_page", "scrape")]

    async def invoke(self, name: str, arguments: dict[str, object]) -> dict[str, object]:
        self.calls.append((name, arguments))
        text = "The announcement says availability increased by 12%."
        return {
            "url": str(arguments["url"]),
            "title": "Announcement",
            "text": text,
            "characters": len(text),
            "retrieved_at": "2026-01-01T00:00:00+00:00",
            "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
        }


@pytest.mark.asyncio
async def test_model_can_select_and_execute_registered_browser_tool() -> None:
    model = PlanningModel(
        [
            '{"tool":"scrape_dynamic_page","arguments":{"url":"https://example.com"}}',
            '{"tool":null,"arguments":{}}',
            "Availability increased by 12% [source:1].",
        ]
    )
    tools = RecordingTools()
    graph = build_agent_graph(model, object(), tool_executor=tools)
    events = [
        event
        async for event in graph.astream(
            initial_state(
                "autonomous-1",
                "Use the website https://example.com and summarize its announcement",
                None,
                0,
            ),
            stream_mode="custom",
        )
    ]

    assert tools.calls == [("scrape_dynamic_page", {"url": "https://example.com"})]
    completed = next(event for event in events if event.get("type") == "completed")
    assert completed["evidence_validated"] is True
    assert completed["answer"] == "Availability increased by 12% [source:1]."


@pytest.mark.asyncio
async def test_real_mcp_stdio_transport_can_invoke_browser_contract_without_navigation() -> None:
    executor = MCPBrowserToolExecutor()
    try:
        result = await executor.invoke("browser_close", {"session_id": "not-present"})
        assert result == {"session_id": "not-present", "closed": False}
        session = executor._session
        await executor.invoke("browser_close", {"session_id": "still-not-present"})
        assert executor._session is session
    finally:
        await executor.close()


class MutatingTools(RecordingTools):
    def specs(self) -> list[ToolSpec]:
        return [ToolSpec("browser_click", "click", requires_approval=True)]

    async def invoke(self, name: str, arguments: dict[str, object]) -> dict[str, object]:
        self.calls.append((name, arguments))
        text = "The approved button click completed successfully."
        return {
            "url": "https://example.com/result",
            "text": text,
            "characters": len(text),
            "retrieved_at": "2026-01-01T00:00:00+00:00",
            "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
        }


@pytest.mark.asyncio
async def test_mutating_tool_is_blocked_without_explicit_approval() -> None:
    model = PlanningModel(
        [
            '{"tool":"browser_click","arguments":{"session_id":"s1","selector":"#buy"}}',
            '{"tool":null,"arguments":{}}',
            "The click could not be completed safely.",
        ]
    )
    tools = MutatingTools()
    graph = build_agent_graph(model, object(), tool_executor=tools)
    events = [
        event
        async for event in graph.astream(
            initial_state("approval-1", "Use the browser to click #buy", None, 0),
            stream_mode="custom",
        )
    ]

    assert tools.calls == []
    failed = next(
        event
        for event in events
        if event.get("type") == "tool_status" and event.get("status") == "failed"
    )
    assert "requires explicit approval" in str(failed["error"])


@pytest.mark.asyncio
async def test_react_loop_uses_observation_to_select_multiple_tools() -> None:
    model = PlanningModel(
        [
            '{"tool":"scrape_dynamic_page","arguments":{"url":"https://one.example"}}',
            '{"tool":"scrape_dynamic_page","arguments":{"url":"https://two.example"}}',
            '{"tool":null,"arguments":{}}',
            "Availability increased by 12% [source:1].",
        ]
    )
    tools = RecordingTools()
    graph = build_agent_graph(model, object(), tool_executor=tools)
    events = [
        event
        async for event in graph.astream(
            initial_state(
                "react-1", "Use the browser to compare two websites", None, 0, max_tool_rounds=4
            ),
            stream_mode="custom",
        )
    ]

    assert [arguments["url"] for _, arguments in tools.calls] == [
        "https://one.example",
        "https://two.example",
    ]
    decisions = [
        event["decision"]
        for event in events
        if event.get("type") == "node_status"
        and event.get("node") == "tool_decision"
        and event.get("status") == "completed"
    ]
    assert decisions == ["scrape_dynamic_page", "finish"]


@pytest.mark.asyncio
async def test_dangerous_tool_pauses_and_resumes_from_human_approval() -> None:
    model = PlanningModel(
        [
            '{"tool":"browser_click","arguments":{"session_id":"s1","selector":"#buy"}}',
            '{"tool":null,"arguments":{}}',
            "The approved button click completed successfully [source:1].",
        ]
    )
    tools = MutatingTools()
    graph = build_agent_graph(
        model, object(), tool_executor=tools, checkpointer=MemorySaver()
    )
    config = {"configurable": {"thread_id": "approval-thread"}}
    initial_events = [
        event
        async for event in graph.astream(
            initial_state(
                "approval-2",
                "Click the purchase button",
                None,
                0,
                interactive_approval=True,
            ),
            config=config,
            stream_mode="custom",
        )
    ]
    assert any(event.get("type") == "approval_required" for event in initial_events)
    assert tools.calls == []

    resumed_events = [
        event
        async for event in graph.astream(
            Command(resume={"approved": True}), config=config, stream_mode="custom"
        )
    ]
    assert tools.calls == [("browser_click", {"session_id": "s1", "selector": "#buy"})]
    completed = next(event for event in resumed_events if event.get("type") == "completed")
    assert completed["evidence_validated"] is True
