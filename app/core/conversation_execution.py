"""Redis authority for per-conversation single-flight execution."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from redis.exceptions import WatchError


def conversation_keys(tenant: str, conversation_id: str) -> tuple[str, str, str, str]:
    prefix = f"tenant:{tenant}:conversation:{conversation_id}:execution"
    return prefix, f"{prefix}:fence", f"{prefix}:fifo", f"{prefix}:members"


@dataclass(frozen=True)
class ConversationClaim:
    owner_id: str
    fence: int
    phase: str
    execution_fence: int


class ConversationExecutionStore:
    """Immediate runs conflict; background Goals are ordered by their Redis FIFO."""

    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self.client = getattr(runtime, "_client", None)
        if self.client is None:
            raise RuntimeError("Conversation execution requires Redis")

    async def acquire_immediate(
        self,
        tenant: str,
        conversation_id: str,
        owner_id: str,
        lease_seconds: float = 300,
    ) -> ConversationClaim | None:
        record_key, counter_key, queue_key, _ = conversation_keys(tenant, conversation_id)
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(record_key, counter_key, queue_key)
                    record = json.loads(await pipe.get(record_key) or "null")
                    queued = await pipe.llen(queue_key)
                    if record or queued:
                        return None
                    fence = int(await pipe.get(counter_key) or 0) + 1
                    record = {
                        "owner_id": owner_id,
                        "owner_kind": "immediate",
                        "fence": fence,
                        "phase": "running",
                        "execution_fence": 1,
                        "lease_until": time.time() + max(1, lease_seconds),
                    }
                    pipe.multi()
                    pipe.set(counter_key, fence)
                    pipe.set(record_key, json.dumps(record, separators=(",", ":")))
                    await pipe.execute()
                    return ConversationClaim(owner_id, fence, "running", 1)
                except WatchError:
                    continue

    async def assert_owner(
        self,
        tenant: str,
        conversation_id: str,
        owner_id: str,
        fence: int,
        execution_fence: int,
    ) -> bool:
        record_key, _, _, _ = conversation_keys(tenant, conversation_id)
        record = json.loads(await self.client.get(record_key) or "null")
        return bool(
            record
            and record.get("owner_id") == owner_id
            and int(record.get("fence", 0)) == fence
            and int(record.get("execution_fence", 0)) == execution_fence
        )

    async def current_claim(
        self, tenant: str, conversation_id: str, owner_id: str, fence: int
    ) -> ConversationClaim | None:
        record_key, _, _, _ = conversation_keys(tenant, conversation_id)
        record = json.loads(await self.client.get(record_key) or "null")
        if (
            not record
            or record.get("owner_id") != owner_id
            or int(record.get("fence", 0)) != int(fence)
        ):
            return None
        return ConversationClaim(
            owner_id,
            int(fence),
            str(record.get("phase", "")),
            int(record.get("execution_fence", 0)),
        )

    async def transition(
        self,
        tenant: str,
        conversation_id: str,
        owner_id: str,
        fence: int,
        execution_fence: int,
        phase: str,
        *,
        release: bool = False,
    ) -> bool:
        from app.core.goal_protocol import goal_queue

        record_key, _, queue_key, _ = conversation_keys(tenant, conversation_id)
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(record_key, queue_key)
                    record = json.loads(await pipe.get(record_key) or "null")
                    if not record or record.get("owner_id") != owner_id or int(
                        record.get("fence", 0)
                    ) != int(fence) or int(record.get("execution_fence", 0)) != int(
                        execution_fence
                    ):
                        return False
                    next_goal = await pipe.lindex(queue_key, 0) if release else None
                    pipe.multi()
                    if release:
                        pipe.delete(record_key)
                        if next_goal:
                            pipe.xadd(
                                goal_queue(next_goal),
                                {"goal_id": next_goal, "tenant_id": tenant},
                            )
                    else:
                        record["phase"] = phase
                        if phase != "running":
                            record["lease_until"] = 0
                        pipe.set(record_key, json.dumps(record, separators=(",", ":")))
                    await pipe.execute()
                    return True
                except WatchError:
                    continue

    async def claim_execution(
        self,
        tenant: str,
        conversation_id: str,
        owner_id: str,
        fence: int,
        lease_seconds: float,
        *,
        expected_phases: set[str],
    ) -> ConversationClaim | None:
        """Atomically start one replica, allowing an expired running attempt to recover."""
        record_key, _, _, _ = conversation_keys(tenant, conversation_id)
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(record_key)
                    record = json.loads(await pipe.get(record_key) or "null")
                    if not record or record.get("owner_id") != owner_id or int(
                        record.get("fence", 0)
                    ) != int(fence):
                        return None
                    phase = str(record.get("phase", ""))
                    live_running = phase == "running" and float(
                        record.get("lease_until", 0)
                    ) > time.time()
                    if live_running or (phase != "running" and phase not in expected_phases):
                        return None
                    execution_fence = int(record.get("execution_fence", 0)) + 1
                    record.update(
                        phase="running",
                        execution_fence=execution_fence,
                        lease_until=time.time() + max(1, lease_seconds),
                    )
                    pipe.multi()
                    pipe.set(record_key, json.dumps(record, separators=(",", ":")))
                    await pipe.execute()
                    return ConversationClaim(
                        owner_id, int(fence), "running", execution_fence
                    )
                except WatchError:
                    continue


class ConversationFencedExecutor:
    """Reject tool effects after a run loses its logical conversation fence."""

    def __init__(
        self,
        delegate: Any,
        store: ConversationExecutionStore,
        tenant: str,
        conversation_id: str,
        claim: ConversationClaim,
    ) -> None:
        self.delegate = delegate
        self.store = store
        self.tenant = tenant
        self.conversation_id = conversation_id
        self.claim = claim

    def specs(self):
        return self.delegate.specs()

    async def invoke(self, name: str, arguments: dict[str, Any]):
        if not await self.store.assert_owner(
            self.tenant,
            self.conversation_id,
            self.claim.owner_id,
            self.claim.fence,
            self.claim.execution_fence,
        ):
            raise PermissionError("Conversation ownership was revoked")
        return await self.delegate.invoke(name, arguments)
