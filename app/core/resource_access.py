"""Current Redis resource ownership, explicit grants, and legacy fail-closed reads.

ACL records are control-plane authority, not historical snapshots. A missing
record never implies tenant-wide visibility when enforcement is enabled.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Literal

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator
from redis.exceptions import WatchError

ResourceKind = Literal[
    "agent", "skill", "planning_session", "goal", "run", "automation", "conversation", "finding"
]
KINDS = frozenset(ResourceKind.__args__)
LEVELS = {"read": 1, "operate": 2}
ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class OwnershipConflict(RuntimeError):
    pass


class Actor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    actor_type: Literal["human", "agent"] = "human"
    subject: str = Field(min_length=1, max_length=256)

    @field_validator("subject")
    @classmethod
    def valid_text(cls, value):
        if not value.strip():
            raise ValueError("Actor identity cannot be blank")
        value.encode("utf-8")
        return value


class Grant(Actor):
    permission: Literal["read", "operate"]


class AccessUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    expected_version: StrictInt = Field(ge=0)
    grants: list[Grant] = Field(default_factory=list, max_length=100)
    # Only admin can explicitly adopt a legacy resource; ownership is otherwise immutable.
    legacy_owner: Actor | None = None


def acl_key(tenant, kind, resource_id):
    if kind not in KINDS or not isinstance(resource_id, str) or not ID.fullmatch(resource_id):
        raise ValueError("Invalid resource identity")
    return f"tenant:{tenant}:governance:acl:{kind}:{resource_id}"


def actor_index(tenant, actor):
    # Lookup index only, not an authenticity hash. Actual authorization always
    # compares the complete typed identity against the authoritative ACL.
    digest = hashlib.sha1((actor["actor_type"] + "\0" + actor["subject"]).encode()).hexdigest()
    return f"tenant:{tenant}:governance:actor:{digest}"


def resource_keys(tenant, kind, resource_id):
    """Live existence only; archived copies never authorize legacy adoption."""
    if kind == "goal":
        from app.core.goal_protocol import goal_key

        return [goal_key(tenant, resource_id)]
    suffixes = {
        "agent": "agents:record",
        "skill": "skills:record",
        "planning_session": "planning:session",
        "goal": "agent:goal",
        "run": "agent:run",
        "automation": "automation",
        "finding": "security:threat-finding",
    }
    if kind == "conversation":
        return [
            f"tenant:{tenant}:conversation:{resource_id}:{suffix}"
            for suffix in ("messages", "summary", "memory")
        ]
    return [f"tenant:{tenant}:{suffixes[kind]}:{resource_id}"]


def queue_owner(pipe, tenant, kind, resource_id, subject, actor_type="human"):
    if not subject:
        return  # Legacy internal adapters without a creator remain admin-only.
    owner = Actor(actor_type=actor_type, subject=subject).model_dump()
    document = {
        "schema_version": 1,
        "version": 1,
        "kind": kind,
        "resource_id": resource_id,
        "owner": owner,
        "grants": [],
        "revoked": False,
    }
    pipe.set(acl_key(tenant, kind, resource_id), json.dumps(document, ensure_ascii=True), nx=True)
    pipe.sadd(actor_index(tenant, owner), f"{kind}/{resource_id}")


async def guard_owner(pipe, tenant, kind, resource_id, subject, actor_type="human"):
    if not subject:
        return
    actor = Actor(actor_type=actor_type, subject=subject).model_dump()
    destinations = {acl_key(tenant, kind, resource_id): "string", actor_index(tenant, actor): "set"}
    await pipe.watch(*destinations)
    for key, expected in destinations.items():
        if await pipe.type(key) not in {"none", expected}:
            raise RuntimeError("Resource ownership destination unavailable")
    raw = await pipe.get(acl_key(tenant, kind, resource_id))
    if raw:
        record = AccessStore.decode(raw, kind, resource_id)
        if record["owner"] != actor or record["revoked"]:
            raise OwnershipConflict("Resource ownership conflict")


def canonical_resource(kind, resource_id):
    aliases = {
        "skill_revision": "skill",
        "planning_revision": "planning_session",
        "tool_receipt": "goal",
    }
    if kind == "planning":
        return "planning_session", resource_id
    if kind in aliases:
        return aliases[kind], resource_id.split(":", 1)[0]
    return kind, resource_id


class AccessStore:
    def __init__(self, runtime):
        self.client = runtime._client

    async def get(self, tenant, kind, resource_id):
        raw = await self.client.get(acl_key(tenant, kind, resource_id))
        if not raw:
            return None
        return self.decode(raw, kind, resource_id)

    @staticmethod
    def decode(raw, kind, resource_id):
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise RuntimeError("Resource access record is invalid")
        if (
            value.get("schema_version") != 1
            or value.get("kind") != kind
            or value.get("resource_id") != resource_id
            or type(value.get("version")) is not int
            or value["version"] < 1
            or type(value.get("revoked")) is not bool
            or not isinstance(value.get("grants"), list)
            or len(value["grants"]) > 100
        ):
            raise RuntimeError("Resource access record is invalid")
        Actor.model_validate(value["owner"])
        for grant in value["grants"]:
            Grant.model_validate(grant)
        return value

    async def can(self, principal, kind, resource_id, permission="read"):
        if principal.role == "admin" and principal.actor_type == "human":
            return True
        kind, resource_id = canonical_resource(kind, resource_id)
        if kind not in KINDS or not isinstance(resource_id, str) or not ID.fullmatch(resource_id):
            return False  # Global audit streams are never implicitly shared.
        record = await self.get(principal.tenant_id, kind, resource_id)
        if not record or record.get("revoked") is not False:
            return False
        identity = {"subject": principal.subject, "actor_type": principal.actor_type}
        if record["owner"] == identity:
            return True
        return any(
            {"subject": grant["subject"], "actor_type": grant["actor_type"]} == identity
            and LEVELS[grant["permission"]] >= LEVELS[permission]
            for grant in record["grants"]
        )

    async def allowed_resources(self, principal):
        if principal.role == "admin" and principal.actor_type == "human":
            return None
        actor = {"subject": principal.subject, "actor_type": principal.actor_type}
        values = await self.client.smembers(actor_index(principal.tenant_id, actor))
        result = []
        for value in sorted(values):
            kind, _, resource_id = value.partition("/")
            if await self.can(principal, kind, resource_id):
                result.append(f"{kind}:{resource_id}")
        return result

    async def update(self, principal, kind, resource_id, body):
        key = acl_key(principal.tenant_id, kind, resource_id)
        identities = [(grant.actor_type, grant.subject) for grant in body.grants]
        if len(set(identities)) != len(identities):
            raise HTTPException(status_code=422, detail="Duplicate resource grant")
        audit_key = f"tenant:{principal.tenant_id}:audit"
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key, audit_key)
                    if await pipe.type(audit_key) not in {"none", "stream"}:
                        raise HTTPException(status_code=503, detail="Access audit unavailable")
                    raw = await pipe.get(key)
                    current = self.decode(raw, kind, resource_id) if raw else None
                    owner = {"subject": principal.subject, "actor_type": principal.actor_type}
                    admin = principal.role == "admin" and principal.actor_type == "human"
                    if not admin and (not current or current["owner"] != owner):
                        raise HTTPException(status_code=404, detail="Resource not found")
                    if (current["version"] if current else 0) != body.expected_version:
                        raise HTTPException(
                            status_code=409, detail="Resource access version conflict"
                        )
                    if current is None:
                        if not admin or body.legacy_owner is None:
                            raise HTTPException(
                                status_code=409, detail="Explicit legacy owner required"
                            )
                        owner = body.legacy_owner.model_dump()
                    else:
                        if body.legacy_owner is not None:
                            raise HTTPException(
                                status_code=422, detail="Resource owner is immutable"
                            )
                        owner = current["owner"]
                        if current["revoked"]:
                            raise HTTPException(
                                status_code=409, detail="Resource access is revoked"
                            )
                    if current is None:
                        sources = resource_keys(principal.tenant_id, kind, resource_id)
                        await pipe.watch(*sources)
                        if not await pipe.exists(*sources):
                            raise HTTPException(status_code=404, detail="Resource not found")
                    document = {
                        "schema_version": 1,
                        "kind": kind,
                        "resource_id": resource_id,
                        "version": body.expected_version + 1,
                        "owner": owner,
                        "grants": [grant.model_dump() for grant in body.grants],
                        "revoked": False,
                    }
                    actors = [owner, *(current or {}).get("grants", []), *document["grants"]]
                    indices = {actor_index(principal.tenant_id, actor) for actor in actors}
                    await pipe.watch(*indices)
                    for index in indices:
                        if await pipe.type(index) not in {"none", "set"}:
                            raise HTTPException(status_code=503, detail="Access index unavailable")
                    pipe.multi()
                    pipe.set(key, json.dumps(document, ensure_ascii=True))
                    member = f"{kind}/{resource_id}"
                    for old in (current or {}).get("grants", []):
                        old_actor = {field: old[field] for field in ("actor_type", "subject")}
                        pipe.srem(actor_index(principal.tenant_id, old_actor), member)
                    for actor in [
                        owner,
                        *[
                            {field: grant[field] for field in ("actor_type", "subject")}
                            for grant in document["grants"]
                        ],
                    ]:
                        pipe.sadd(actor_index(principal.tenant_id, actor), member)
                    pipe.xadd(
                        audit_key,
                        {
                            "payload": json.dumps(
                                {
                                    "action": "resource.access_updated",
                                    "subject": principal.subject,
                                    "actor_type": principal.actor_type,
                                    "kind": kind,
                                    "resource_id": resource_id,
                                    "version": document["version"],
                                    "grant_count": len(body.grants),
                                    "legacy_adopted": current is None,
                                    "time": datetime.now(UTC).isoformat(),
                                }
                            )
                        },
                    )
                    await pipe.execute()
                    return document
                except WatchError:
                    continue


def enforcement(request):
    return bool(getattr(request.app.state.settings, "resource_acl_enabled", False))


def access_store(request):
    runtime = request.app.state.redis
    if runtime is None or not hasattr(runtime, "_client"):
        raise HTTPException(status_code=503, detail="Resource authorization requires Redis")
    return AccessStore(runtime)


async def require_access(request, principal, kind, resource_id, permission="read"):
    if not enforcement(request):
        return
    try:
        store = access_store(request)
        if principal.role == "admin":
            await store.client.ping()
        allowed = await store.can(principal, kind, resource_id, permission)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Resource authorization unavailable") from exc
    if not allowed:
        raise HTTPException(status_code=404, detail="Resource not found")


async def filter_resources(request, principal, kind, rows, id_field):
    if not enforcement(request):
        return rows
    store = access_store(request)
    try:
        return [row for row in rows if await store.can(principal, kind, row[id_field])]
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Resource authorization unavailable") from exc


async def filter_history(request, principal, rows):
    if not enforcement(request):
        return rows
    store = access_store(request)
    try:
        await store.client.ping()
        return [row for row in rows if await store.can(principal, row["kind"], row["resource_id"])]
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Resource authorization unavailable") from exc


async def history_permissions(request, principal):
    if not enforcement(request):
        return {}
    try:
        store = access_store(request)
        await store.client.ping()
        return {"authorized_resources": await store.allowed_resources(principal)}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Resource authorization unavailable") from exc


async def claim_conversation(request, principal, conversation_id):
    if not enforcement(request):
        return
    store = access_store(request)
    client, tenant = store.client, principal.tenant_id
    key = acl_key(tenant, "conversation", conversation_id)
    data_keys = [
        f"tenant:{tenant}:conversation:{conversation_id}:{suffix}"
        for suffix in ("messages", "summary", "memory")
    ]
    while True:
        async with client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(key, *data_keys)
                if await pipe.exists(key):
                    await require_access(
                        request, principal, "conversation", conversation_id, "operate"
                    )
                    return
                if await pipe.exists(*data_keys):
                    if principal.role != "admin":
                        raise HTTPException(status_code=404, detail="Resource not found")
                    return
                await guard_owner(
                    pipe,
                    tenant,
                    "conversation",
                    conversation_id,
                    principal.subject,
                    principal.actor_type,
                )
                pipe.multi()
                queue_owner(
                    pipe,
                    tenant,
                    "conversation",
                    conversation_id,
                    principal.subject,
                    principal.actor_type,
                )
                await pipe.execute()
                return
            except WatchError:
                continue
            except OwnershipConflict:
                raise HTTPException(status_code=404, detail="Resource not found") from None


async def authorize_resource_route(request, principal):
    if not enforcement(request):
        return
    # Run inside authorize(), after routing and role checks. Public signed hooks
    # intentionally use their separate server-owned capability, not this path.
    route = request.scope.get("route")
    template = getattr(route, "path", "")
    if template.startswith("/v1/access/"):
        return  # Management handlers require owner/admin explicitly.
    mapping = {
        "agent_id": "agent",
        "goal_id": "goal",
        "request_id": "run",
        "session_id": "planning_session",
        "skill_id": "skill",
        "automation_id": "automation",
        "conversation_id": "conversation",
    }
    if template.startswith("/v1/acp/sessions/"):
        mapping["session_id"] = "conversation"
    for field, kind in mapping.items():
        if field in request.path_params:
            await require_access(
                request,
                principal,
                kind,
                request.path_params[field],
                "read" if request.method in {"GET", "HEAD"} else "operate",
            )


async def stream_access(request, principal, kind, resource_id):
    """Recheck current identity and ACL immediately before each streamed payload."""
    if not enforcement(request):
        return True
    from app.core.security import authenticate

    try:
        current = await authenticate(request)
        if (current.subject, current.actor_type, current.tenant_id) != (
            principal.subject,
            principal.actor_type,
            principal.tenant_id,
        ):
            return False
        return await access_store(request).can(current, kind, resource_id)
    except Exception:
        return False
