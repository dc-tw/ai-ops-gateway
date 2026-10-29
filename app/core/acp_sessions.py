"""Redis-backed ACP session metadata; execution remains in existing Gateway APIs."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from redis.exceptions import WatchError

from app.core.resource_access import guard_owner, queue_owner

MODES = {"plan", "execute"}


class AcpSessionStore:
    def __init__(self, runtime: Any, ttl_seconds: int, *, max_watch_retries: int = 8) -> None:
        self.runtime = runtime
        self.client = runtime._client
        self.ttl_seconds = ttl_seconds
        self.max_watch_retries = max_watch_retries

    def key(self, tenant: str, session_id: str) -> str:
        return self.runtime._tenant_key(tenant, f"acp:session:{session_id}")

    @staticmethod
    def _decode(raw: Any, tenant: str, session_id: str) -> dict[str, Any] | None:
        if raw is None:
            return None
        try:
            value = json.loads(raw)
        except (TypeError, ValueError):
            return None
        if (
            not isinstance(value, dict)
            or value.get("schema_version") != 1
            or value.get("tenant_id") != tenant
            or value.get("session_id") != session_id
            or value.get("conversation_id") != session_id
            or value.get("mode") not in MODES
            or type(value.get("version")) is not int
            or value["version"] < 1
        ):
            return None
        return value

    async def create(
        self, tenant: str, subject: str, actor_type: str, cwd_sha256: str, client_name: str
    ) -> dict[str, Any]:
        now = datetime.now(UTC).isoformat()
        for _ in range(4):
            session_id = str(uuid4())
            session_key = self.key(tenant, session_id)
            value = {
                "schema_version": 1,
                "tenant_id": tenant,
                "session_id": session_id,
                "conversation_id": session_id,
                "mode": "plan",
                "version": 1,
                "created_by": subject,
                "created_by_type": actor_type,
                "cwd_sha256": cwd_sha256,
                "client_name": client_name,
                "created_at": now,
                "updated_at": now,
            }
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await guard_owner(pipe, tenant, "conversation", session_id, subject, actor_type)
                    await pipe.watch(session_key)
                    if await pipe.exists(session_key):
                        continue
                    pipe.multi()
                    pipe.set(
                        session_key,
                        json.dumps(value, separators=(",", ":")),
                        ex=self.ttl_seconds,
                        nx=True,
                    )
                    queue_owner(pipe, tenant, "conversation", session_id, subject, actor_type)
                    created, *_ = await pipe.execute()
                    if created:
                        return value
                except WatchError:
                    continue
        raise RuntimeError("ACP session identifier unavailable")

    async def get(self, tenant: str, session_id: str) -> dict[str, Any] | None:
        return self._decode(await self.client.get(self.key(tenant, session_id)), tenant, session_id)

    async def set_mode(
        self, tenant: str, session_id: str, mode: str, expected_version: int
    ) -> dict[str, Any] | None:
        if mode not in MODES:
            raise ValueError("Unsupported ACP session mode")
        key = self.key(tenant, session_id)
        for _ in range(self.max_watch_retries):
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    current = self._decode(await pipe.get(key), tenant, session_id)
                    if current is None:
                        return None
                    if current["version"] != expected_version:
                        raise ValueError("ACP session version conflict")
                    updated = {
                        **current,
                        "mode": mode,
                        "version": current["version"] + 1,
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                    pipe.multi()
                    pipe.set(
                        key,
                        json.dumps(updated, separators=(",", ":")),
                        ex=self.ttl_seconds,
                    )
                    await pipe.execute()
                    return updated
                except WatchError:
                    continue
        raise RuntimeError("ACP session update contention")
