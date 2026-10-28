"""Ephemeral human presence and Agent heartbeat; neither grants locks or authority."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, datetime
from typing import Any

from app.core.resource_access import ID, KINDS


def _identity(subject: str) -> str:
    return hashlib.sha256(subject.encode("utf-8")).hexdigest()


def _validate_resource(kind: str, resource_id: str) -> None:
    if kind not in KINDS or not ID.fullmatch(resource_id):
        raise ValueError("Invalid collaboration resource")


class CollaborationStore:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self.client = runtime._client

    def _presence_key(self, tenant: str, kind: str, resource_id: str, subject: str) -> str:
        return self.runtime._tenant_key(
            tenant, f"collaboration:presence:{kind}:{resource_id}:{_identity(subject)}"
        )

    def _presence_index(self, tenant: str, kind: str, resource_id: str) -> str:
        return self.runtime._tenant_key(
            tenant, f"collaboration:presence-index:{kind}:{resource_id}"
        )

    def _heartbeat_key(self, tenant: str, agent_id: str) -> str:
        return self.runtime._tenant_key(tenant, f"collaboration:agent-heartbeat:{agent_id}")

    async def set_presence(
        self, principal: Any, kind: str, resource_id: str, state: str, ttl: int
    ) -> dict[str, Any]:
        _validate_resource(kind, resource_id)
        if principal.actor_type != "human" or state not in {"viewing", "editing"}:
            raise ValueError("Invalid human presence")
        now = datetime.now(UTC).isoformat()
        record = {
            "schema_version": 1,
            "kind": kind,
            "resource_id": resource_id,
            "subject": principal.subject,
            "state": state,
            "updated_at": now,
            "expires_in_seconds": ttl,
        }
        key = self._presence_key(principal.tenant_id, kind, resource_id, principal.subject)
        index = self._presence_index(principal.tenant_id, kind, resource_id)
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.set(key, json.dumps(record, separators=(",", ":")), ex=ttl)
            pipe.zadd(index, {key: time.time() + ttl})
            pipe.expire(index, ttl * 2)
            await pipe.execute()
        return record

    async def clear_presence(self, principal: Any, kind: str, resource_id: str) -> None:
        _validate_resource(kind, resource_id)
        key = self._presence_key(principal.tenant_id, kind, resource_id, principal.subject)
        index = self._presence_index(principal.tenant_id, kind, resource_id)
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.delete(key)
            pipe.zrem(index, key)
            await pipe.execute()

    async def list_presence(self, tenant: str, kind: str, resource_id: str) -> list[dict[str, Any]]:
        _validate_resource(kind, resource_id)
        index = self._presence_index(tenant, kind, resource_id)
        now = time.time()
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.zremrangebyscore(index, "-inf", now)
            pipe.zrangebyscore(index, now, "+inf")
            _, keys = await pipe.execute()
        values = await self.client.mget(keys) if keys else []
        result = []
        stale = []
        for key, raw in zip(keys, values, strict=True):
            try:
                record = json.loads(raw) if raw else None
            except (TypeError, ValueError):
                record = None
            if (
                not isinstance(record, dict)
                or record.get("schema_version") != 1
                or record.get("kind") != kind
                or record.get("resource_id") != resource_id
                or record.get("state") not in {"viewing", "editing"}
                or not isinstance(record.get("subject"), str)
            ):
                stale.append(key)
                continue
            result.append(record)
        if stale:
            await self.client.zrem(index, *stale)
        return sorted(result, key=lambda item: (item["subject"], item["state"]))

    async def heartbeat(
        self, principal: Any, agent_id: str, status: str, instance_id: str | None, ttl: int
    ) -> dict[str, Any]:
        if (
            not ID.fullmatch(agent_id)
            or principal.actor_type != "agent"
            or principal.subject != agent_id
        ):
            raise PermissionError("Agent heartbeat identity mismatch")
        if status not in {"idle", "working", "degraded"}:
            raise ValueError("Invalid Agent heartbeat status")
        record = {
            "schema_version": 1,
            "agent_id": agent_id,
            "status": status,
            "instance_id": instance_id,
            "updated_at": datetime.now(UTC).isoformat(),
            "expires_in_seconds": ttl,
        }
        await self.client.set(
            self._heartbeat_key(principal.tenant_id, agent_id),
            json.dumps(record, separators=(",", ":")),
            ex=ttl,
        )
        return record

    async def get_heartbeat(self, tenant: str, agent_id: str) -> dict[str, Any]:
        if not ID.fullmatch(agent_id):
            raise ValueError("Invalid Agent identity")
        raw = await self.client.get(self._heartbeat_key(tenant, agent_id))
        if raw is None:
            return {"agent_id": agent_id, "status": "offline"}
        try:
            record = json.loads(raw)
        except (TypeError, ValueError):
            return {"agent_id": agent_id, "status": "offline"}
        if (
            not isinstance(record, dict)
            or record.get("schema_version") != 1
            or record.get("agent_id") != agent_id
            or record.get("status") not in {"idle", "working", "degraded"}
        ):
            return {"agent_id": agent_id, "status": "offline"}
        return record
