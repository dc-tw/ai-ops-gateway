import hashlib
from collections.abc import AsyncIterator
from copy import deepcopy
from typing import Any

import pytest

from app.worker import execute_goal, recover_goals


class FakeModel:
    async def complete(self, system: str, prompt: str) -> str:
        return "A sufficiently detailed result for the durable goal step."

    async def stream(self, system: str, prompt: str) -> AsyncIterator[str]:
        for token in ("A sufficiently detailed result ", "for the durable goal step."):
            yield token


class FakeRouter:
    def get(self, model: str):
        return FakeModel()


class FakeScraper:
    async def scrape(self, url: str) -> dict[str, object]:
        return {
            "url": url,
            "title": "Page",
            "text": "content",
            "characters": 7,
            "retrieved_at": "2026-01-01T00:00:00+00:00",
            "content_sha256": hashlib.sha256(b"content").hexdigest(),
        }


class FakeRuntime:
    def __init__(self, goal: dict[str, Any]) -> None:
        self.goals = {goal["goal_id"]: deepcopy(goal)}
        self.events: list[dict[str, Any]] = []
        self.queue: list[str] = []
        self.messages: list[dict[str, str]] = []

    async def get_goal(self, goal_id: str, tenant_id: str = "default"):
        goal = self.goals.get(goal_id)
        return deepcopy(goal) if goal else None

    async def save_goal(
        self, goal_id: str, goal: dict[str, Any], tenant_id: str = "default"
    ) -> None:
        self.goals[goal_id] = deepcopy(goal)

    async def append_goal_event(
        self, goal_id: str, event: dict[str, Any], tenant_id: str = "default"
    ) -> str:
        self.events.append(deepcopy(event))
        return str(len(self.events))

    async def get_conversation(
        self, conversation_id: str, limit: int = 40, tenant_id: str = "default"
    ):
        return self.messages[-limit:]

    async def append_conversation_turn(
        self,
        conversation_id: str,
        *,
        query: str,
        answer: str,
        model: str,
        request_id: str,
        **kwargs: object,
    ) -> None:
        self.messages.extend(
            [
                {"role": "user", "content": query},
                {"role": "assistant", "content": answer, "model": model},
            ]
        )

    async def list_goal_ids(self, tenant_id: str = "default") -> list[str]:
        return list(self.goals)

    async def list_goal_tenants(self) -> list[str]:
        return ["default"]

    async def enqueue_goal(self, goal_id: str, tenant_id: str = "default") -> None:
        self.queue.append(goal_id)


def goal(status: str = "queued") -> dict[str, Any]:
    return {
        "goal_id": "goal-1",
        "objective": "finish a report",
        "model": "model-a",
        "max_retries": 0,
        "status": status,
        "current_step": 0,
        "steps": [
            {
                "id": "step-1",
                "query": "write the report",
                "target_url": None,
                "status": "pending",
                "answer": "",
                "error": "",
            }
        ],
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }


@pytest.mark.asyncio
async def test_goal_worker_completes_and_persists_step_memory() -> None:
    runtime = FakeRuntime(goal())
    await execute_goal(
        "goal-1",
        runtime,
        FakeRouter(),
        FakeScraper(),
        conversation_max_turns=20,
        conversation_ttl_seconds=3600,
    )

    saved = runtime.goals["goal-1"]
    assert saved["status"] == "completed"
    assert saved["steps"][0]["status"] == "completed"
    assert (
        saved["steps"][0]["answer"]
        == "A sufficiently detailed result for the durable goal step."
    )
    assert runtime.messages[-1]["model"] == "model-a"
    assert runtime.events[-1]["type"] == "goal_completed"


@pytest.mark.asyncio
async def test_worker_restart_recovers_running_goal() -> None:
    runtime = FakeRuntime(goal(status="running"))
    await recover_goals(runtime)
    assert runtime.goals["goal-1"]["status"] == "queued"
    assert runtime.queue == ["goal-1"]
