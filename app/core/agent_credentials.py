"""Rotatable, revocable credentials bound to one server-owned Agent."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from redis.exceptions import WatchError

TOKEN_PATTERN = re.compile(r"^agc_([0-9a-f]{32})_([A-Za-z0-9_-]{43})$")
MAX_CREDENTIALS_PER_AGENT = 10


def _now() -> datetime:
    return datetime.now(UTC)


def _digest(credential_id: str, secret: str) -> str:
    return hashlib.sha256(f"{credential_id}.{secret}".encode()).hexdigest()


def public_credential(record: dict[str, Any]) -> dict[str, Any]:
    now = _now().timestamp()
    if record.get("revoked_at"):
        status = "revoked"
    elif float(record.get("expires_at_epoch", 0)) <= now:
        status = "expired"
    else:
        status = "active"
    return {
        key: record[key]
        for key in (
            "credential_id",
            "agent_id",
            "tenant_id",
            "label",
            "issued_at",
            "issued_by",
            "expires_at",
            "revoked_at",
            "revoked_by",
        )
    } | {"status": status}


class AgentCredentialStore:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self.client = runtime._client

    @staticmethod
    def credential_key(credential_id: str) -> str:
        return f"gateway:agent-credential:{credential_id}"

    def agent_key(self, tenant: str, agent_id: str) -> str:
        return self.runtime._tenant_key(tenant, f"agents:record:{agent_id}")

    def index_key(self, tenant: str, agent_id: str) -> str:
        return self.runtime._tenant_key(tenant, f"agents:credentials:{agent_id}")

    def audit_key(self, tenant: str) -> str:
        # Credential authority must remain available during a PostgreSQL
        # history maintenance fence. This Redis-only security stream is
        # intentionally not classified for durable projection.
        return self.runtime._tenant_key(tenant, "security:credential-audit")

    async def _agent(self, tenant: str, agent_id: str) -> dict[str, Any] | None:
        raw = await self.client.get(self.agent_key(tenant, agent_id))
        return json.loads(raw) if raw else None

    async def list(self, tenant: str, agent_id: str) -> list[dict[str, Any]]:
        agent = await self._agent(tenant, agent_id)
        if agent is None or agent.get("tenant_id") != tenant:
            raise LookupError("Agent not found")
        ids = sorted(await self.client.smembers(self.index_key(tenant, agent_id)))
        rows = await self.client.mget([self.credential_key(item) for item in ids]) if ids else []
        return sorted(
            [public_credential(json.loads(row)) for row in rows if row],
            key=lambda item: item["issued_at"],
            reverse=True,
        )

    async def issue(
        self,
        tenant: str,
        agent_id: str,
        *,
        label: str,
        expires_in_seconds: int,
        issued_by: str,
        rotate: bool = False,
    ) -> tuple[dict[str, Any], str]:
        credential_id = uuid4().hex
        secret = secrets.token_urlsafe(32)
        token = f"agc_{credential_id}_{secret}"
        now = _now()
        record = {
            "credential_id": credential_id,
            "agent_id": agent_id,
            "tenant_id": tenant,
            "label": label,
            "secret_sha256": _digest(credential_id, secret),
            "issued_at": now.isoformat(),
            "issued_by": issued_by,
            "expires_at": (now + timedelta(seconds=expires_in_seconds)).isoformat(),
            "expires_at_epoch": (now + timedelta(seconds=expires_in_seconds)).timestamp(),
            "revoked_at": "",
            "revoked_by": "",
        }
        agent_key = self.agent_key(tenant, agent_id)
        index_key = self.index_key(tenant, agent_id)
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(agent_key, index_key)
                    agent_raw = await pipe.get(agent_key)
                    agent = json.loads(agent_raw) if agent_raw else None
                    if not agent or agent.get("tenant_id") != tenant:
                        raise LookupError("Agent not found")
                    if not agent.get("enabled"):
                        raise PermissionError("Agent is disabled")
                    ids = sorted(await pipe.smembers(index_key))
                    keys = [self.credential_key(item) for item in ids]
                    if keys:
                        await pipe.watch(*keys)
                    rows = await pipe.mget(keys) if keys else []
                    existing = [json.loads(row) for row in rows if row]
                    active = [
                        item
                        for item in existing
                        if not item.get("revoked_at")
                        and float(item.get("expires_at_epoch", 0)) > now.timestamp()
                    ]
                    if active and not rotate:
                        raise FileExistsError("Agent already has an active credential")
                    inactive = [item for item in existing if item not in active]
                    prune_count = max(0, len(existing) + 1 - MAX_CREDENTIALS_PER_AGENT)
                    pruned = sorted(inactive, key=lambda item: item["issued_at"])[:prune_count]
                    present_ids = {item["credential_id"] for item in existing}
                    dangling_ids = [item for item in ids if item not in present_ids]
                    pipe.multi()
                    if rotate:
                        for item in active:
                            item["revoked_at"] = now.isoformat()
                            item["revoked_by"] = issued_by
                            pipe.set(
                                self.credential_key(item["credential_id"]),
                                json.dumps(item, ensure_ascii=False),
                            )
                    for item in pruned:
                        pipe.delete(self.credential_key(item["credential_id"]))
                        pipe.srem(index_key, item["credential_id"])
                    if dangling_ids:
                        pipe.srem(index_key, *dangling_ids)
                    pipe.set(
                        self.credential_key(credential_id),
                        json.dumps(record, ensure_ascii=False),
                    )
                    pipe.sadd(index_key, credential_id)
                    audit_payload = json.dumps(
                        {
                            "action": (
                                "agent.credential.rotate"
                                if rotate
                                else "agent.credential.issue"
                            ),
                            "agent_id": agent_id,
                            "credential_id": credential_id,
                            "subject": issued_by,
                            "expires_at": record["expires_at"],
                            "revoked_credentials": len(active) if rotate else 0,
                            "pruned_metadata": len(pruned) + len(dangling_ids),
                        },
                        ensure_ascii=False,
                    )
                    pipe.xadd(self.audit_key(tenant), {"payload": audit_payload})
                    pipe.xadd(
                        self.runtime._tenant_key(tenant, "audit"),
                        {"payload": audit_payload},
                    )
                    await pipe.execute()
                    return public_credential(record), token
                except WatchError:
                    continue

    async def revoke(
        self,
        tenant: str,
        agent_id: str,
        credential_id: str,
        *,
        revoked_by: str,
    ) -> dict[str, Any] | None:
        key = self.credential_key(credential_id)
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    record = json.loads(raw) if raw else None
                    if (
                        not record
                        or record.get("tenant_id") != tenant
                        or record.get("agent_id") != agent_id
                    ):
                        return None
                    if record.get("revoked_at"):
                        return public_credential(record)
                    record["revoked_at"] = _now().isoformat()
                    record["revoked_by"] = revoked_by
                    pipe.multi()
                    pipe.set(key, json.dumps(record, ensure_ascii=False))
                    audit_payload = json.dumps(
                        {
                            "action": "agent.credential.revoke",
                            "agent_id": agent_id,
                            "credential_id": credential_id,
                            "subject": revoked_by,
                        },
                        ensure_ascii=False,
                    )
                    pipe.xadd(self.audit_key(tenant), {"payload": audit_payload})
                    pipe.xadd(
                        self.runtime._tenant_key(tenant, "audit"),
                        {"payload": audit_payload},
                    )
                    await pipe.execute()
                    return public_credential(record)
                except WatchError:
                    continue

    async def resolve(self, token: str) -> dict[str, Any] | None:
        match = TOKEN_PATTERN.fullmatch(token)
        if not match:
            return None
        credential_id, secret = match.groups()
        raw = await self.client.get(self.credential_key(credential_id))
        if not raw:
            return None
        record = json.loads(raw)
        if not hmac.compare_digest(
            str(record.get("secret_sha256", "")), _digest(credential_id, secret)
        ):
            return None
        if (
            record.get("revoked_at")
            or float(record.get("expires_at_epoch", 0)) <= _now().timestamp()
        ):
            return None
        agent = await self._agent(
            str(record.get("tenant_id", "")), str(record.get("agent_id", ""))
        )
        if (
            not agent
            or not agent.get("enabled")
            or agent.get("tenant_id") != record.get("tenant_id")
        ):
            return None
        return record
