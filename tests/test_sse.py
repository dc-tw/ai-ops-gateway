import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi.testclient import TestClient

from app.api.routes import _sse
from app.core.config import Settings
from app.main import create_app


def test_sse_frame_is_valid_and_json_encoded() -> None:
    frame = _sse("token", {"type": "token", "content": "第一行\n第二行"}, 7)
    assert frame.startswith("id: 7\nevent: token\ndata: ")
    assert frame.endswith("\n\n")
    payload = frame.split("data: ", 1)[1].strip()
    assert json.loads(payload)["content"] == "第一行\n第二行"


class FakeGraph:
    async def astream(self, *args: Any, **kwargs: Any) -> AsyncIterator[dict[str, object]]:
        yield {"type": "token", "content": "hello"}
        yield {"type": "completed", "answer": "hello", "evidence_validated": True}


class SlowGraph:
    async def astream(self, *args: Any, **kwargs: Any) -> AsyncIterator[dict[str, object]]:
        await asyncio.sleep(2)
        yield {"type": "completed", "answer": "too late", "evidence_validated": True}


class FakeRedis:
    def __init__(self) -> None:
        self.messages: dict[str, list[dict[str, str]]] = {}
        self.goals: dict[str, dict[str, object]] = {}
        self.goal_queue: list[str] = []
        self.events: dict[str, list[tuple[str, dict[str, object]]]] = {}
        self.run_states: dict[str, dict[str, object]] = {}

    async def append_event(
        self, request_id: str, event: dict[str, object], tenant_id: str = "default"
    ) -> str:
        values = self.events.setdefault(request_id, [])
        event_id = f"{len(values) + 1}-0"
        values.append((event_id, event.copy()))
        return event_id

    async def save_run_state(
        self, request_id: str, state: dict[str, object], tenant_id: str = "default"
    ) -> None:
        self.run_states[request_id] = state.copy()

    async def get_run_state(self, request_id: str, tenant_id: str = "default"):
        return self.run_states.get(request_id)

    async def read_agent_events(
        self,
        request_id: str,
        last_id: str = "0-0",
        block_ms: int = 15_000,
        tenant_id: str = "default",
    ):
        last_number = int(last_id.split("-", 1)[0])
        return [
            (event_id, event)
            for event_id, event in self.events.get(request_id, [])
            if int(event_id.split("-", 1)[0]) > last_number
        ]

    async def enforce_rate_limit(self, tenant_id: str, limit: int) -> int:
        return 1

    async def consume_daily_tokens(self, tenant_id: str, tokens: int, limit: int) -> int:
        return tokens

    async def acquire_run_slot(
        self, tenant_id: str, request_id: str, limit: int, ttl_seconds: int = 600
    ) -> bool:
        return True

    async def release_run_slot(self, tenant_id: str, request_id: str) -> None:
        return None

    async def append_audit(self, tenant_id: str, event: dict[str, object]) -> str:
        return "1-0"

    async def get_conversation(
        self, conversation_id: str, limit: int = 40, tenant_id: str = "default"
    ):
        return self.messages.get(conversation_id, [])[-limit:]

    async def append_conversation_turn(
        self,
        conversation_id: str,
        *,
        query: str,
        answer: str,
        model: str,
        request_id: str,
        tenant_id: str = "default",
        **kwargs: object,
    ) -> None:
        self.messages.setdefault(conversation_id, []).extend(
            [
                {"role": "user", "content": query},
                {"role": "assistant", "content": answer, "model": model},
            ]
        )

    async def save_goal(
        self, goal_id: str, goal: dict[str, object], tenant_id: str = "default"
    ) -> None:
        self.goals[goal_id] = goal.copy()

    async def get_goal(self, goal_id: str, tenant_id: str = "default"):
        goal = self.goals.get(goal_id)
        return goal.copy() if goal else None

    async def list_goal_ids(self, tenant_id: str = "default") -> list[str]:
        return list(self.goals)

    async def enqueue_goal(self, goal_id: str, tenant_id: str = "default") -> None:
        self.goal_queue.append(goal_id)

    async def append_goal_event(
        self, goal_id: str, event: dict[str, object], tenant_id: str = "default"
    ) -> str:
        return "1-0"

    async def close(self) -> None:
        return None


def test_stream_endpoint_returns_ordered_sse_events() -> None:
    app = create_app(graph=FakeGraph(), settings=Settings(), redis_runtime=FakeRedis())
    with TestClient(app) as client:
        response = client.post("/v1/agent/stream", json={"query": "say hello"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["x-accel-buffering"] == "no"
    assert "event: accepted" in response.text
    assert "event: token" in response.text
    assert "event: completed" in response.text
    assert response.text.index("event: accepted") < response.text.index("event: completed")


def test_same_conversation_can_change_model_and_reuse_history() -> None:
    redis = FakeRedis()
    app = create_app(
        graph=FakeGraph(),
        settings=Settings(llm_allow_arbitrary_models=True),
        redis_runtime=redis,
    )
    with TestClient(app) as client:
        first = client.post(
            "/v1/agent/stream",
            json={"conversation_id": "conversation-1", "model": "model-a", "query": "one"},
        )
        second = client.post(
            "/v1/agent/stream",
            json={"conversation_id": "conversation-1", "model": "model-b", "query": "two"},
        )

    assert first.status_code == 200
    assert second.status_code == 200
    assert '"model":"model-a"' in first.text
    assert '"model":"model-b"' in second.text
    assert '"history_messages":2' in second.text
    assert [message.get("model") for message in redis.messages["conversation-1"]] == [
        None,
        "model-a",
        None,
        "model-b",
    ]


def test_goal_can_be_persisted_paused_and_resumed() -> None:
    redis = FakeRedis()
    app = create_app(
        graph=FakeGraph(),
        settings=Settings(llm_allow_arbitrary_models=True),
        redis_runtime=redis,
    )
    with TestClient(app) as client:
        created = client.post(
            "/v1/goals",
            json={
                "objective": "produce a durable report",
                "model": "model-a",
                "steps": [{"query": "collect data"}, {"query": "write report"}],
            },
        )
        goal_id = created.json()["goal_id"]
        paused = client.post(f"/v1/goals/{goal_id}/pause")
        resumed = client.post(f"/v1/goals/{goal_id}/resume")

    assert created.status_code == 200
    assert paused.json()["status"] == "paused"
    assert resumed.json()["status"] == "queued"
    assert redis.goal_queue == [goal_id, goal_id]


def test_goal_and_waiting_agent_run_can_be_cancelled() -> None:
    redis = FakeRedis()
    app = create_app(
        graph=FakeGraph(),
        settings=Settings(llm_allow_arbitrary_models=True),
        redis_runtime=redis,
    )
    with TestClient(app) as client:
        created = client.post(
            "/v1/goals",
            json={
                "objective": "cancel this goal",
                "model": "model-a",
                "steps": [{"query": "wait"}],
            },
        )
        goal_id = created.json()["goal_id"]
        goal_cancelled = client.post(f"/v1/goals/{goal_id}/cancel")
        redis.run_states["waiting-run"] = {
            "status": "waiting_approval",
            "model": "model-a",
            "conversation_id": "cancel-test",
            "query": "wait",
        }
        run_cancelled = client.post("/v1/agent/runs/waiting-run/cancel")

    assert goal_cancelled.json()["status"] == "cancelled"
    assert run_cancelled.json()["status"] == "cancelled"
    assert redis.run_states["waiting-run"]["status"] == "cancelled"


def test_agent_events_can_be_replayed_after_completion() -> None:
    redis = FakeRedis()
    app = create_app(graph=FakeGraph(), settings=Settings(), redis_runtime=redis)
    with TestClient(app) as client:
        response = client.post("/v1/agent/stream", json={"query": "say hello"})
        request_id = response.headers["x-request-id"]
        replay = client.get(f"/v1/agent/runs/{request_id}/events")

    assert replay.status_code == 200
    assert "event: accepted" in replay.text
    assert "event: completed" in replay.text
    assert "id: 1-0" in replay.text


def test_metrics_endpoint_reports_requests_and_agent_events() -> None:
    app = create_app(graph=FakeGraph(), settings=Settings(), redis_runtime=FakeRedis())
    with TestClient(app) as client:
        client.post("/v1/agent/stream", json={"query": "say hello"})
        response = client.get("/metrics")

    assert response.status_code == 200
    assert "ai_gateway_http_requests_total" in response.text
    assert "ai_gateway_agent_events_total" in response.text


def test_agent_deadline_stops_slow_graph_and_persists_terminal_state() -> None:
    redis = FakeRedis()
    app = create_app(graph=SlowGraph(), settings=Settings(), redis_runtime=redis)
    with TestClient(app) as client:
        response = client.post(
            "/v1/agent/stream", json={"query": "slow request", "deadline_seconds": 1}
        )

    request_id = response.headers["x-request-id"]
    assert '"error":"deadline_exceeded"' in response.text
    assert redis.run_states[request_id]["status"] == "timed_out"
