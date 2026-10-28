"""Collaboration visibility APIs. Presence is advisory and never a CAS lock."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Path, Request
from pydantic import BaseModel, ConfigDict, Field

from app.core.collaboration import CollaborationStore
from app.core.resource_access import ID, KINDS, require_access, resource_keys
from app.core.security import authorize

router = APIRouter(prefix="/v1/collaboration", tags=["collaboration"])
RESOURCE_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"


class PresenceUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    state: Literal["viewing", "editing"]


class AgentHeartbeat(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    status: Literal["idle", "working", "degraded"]
    instance_id: str | None = Field(default=None, min_length=1, max_length=128)


def _store(request: Request) -> CollaborationStore:
    runtime = request.app.state.redis
    if runtime is None or not hasattr(runtime, "_client"):
        raise HTTPException(status_code=503, detail="Collaboration state requires Redis")
    return CollaborationStore(runtime)


async def _live(request: Request, tenant: str, kind: str, resource_id: str) -> None:
    if kind not in KINDS or not ID.fullmatch(resource_id):
        raise HTTPException(status_code=404, detail="Resource not found")
    if not await request.app.state.redis._client.exists(
        *resource_keys(tenant, kind, resource_id)
    ):
        raise HTTPException(status_code=404, detail="Resource not found")


@router.get("/presence/{kind}/{resource_id}")
async def list_presence(
    request: Request,
    kind: str,
    resource_id: str = Path(pattern=RESOURCE_PATTERN),
) -> dict[str, object]:
    principal = await authorize(request, "viewer")
    await _live(request, principal.tenant_id, kind, resource_id)
    await require_access(request, principal, kind, resource_id, "read")
    return {
        "presence": await _store(request).list_presence(
            principal.tenant_id, kind, resource_id
        ),
        "advisory_only": True,
    }


@router.put("/presence/{kind}/{resource_id}")
async def update_presence(
    payload: PresenceUpdate,
    request: Request,
    kind: str,
    resource_id: str = Path(pattern=RESOURCE_PATTERN),
) -> dict[str, object]:
    principal = await authorize(request, "viewer")
    if principal.actor_type != "human":
        raise HTTPException(status_code=403, detail="Human identity required")
    await _live(request, principal.tenant_id, kind, resource_id)
    await require_access(
        request,
        principal,
        kind,
        resource_id,
        "operate" if payload.state == "editing" else "read",
    )
    value = await _store(request).set_presence(
        principal,
        kind,
        resource_id,
        payload.state,
        request.app.state.settings.human_presence_ttl_seconds,
    )
    return {"presence": value, "advisory_only": True}


@router.delete("/presence/{kind}/{resource_id}")
async def delete_presence(
    request: Request,
    kind: str,
    resource_id: str = Path(pattern=RESOURCE_PATTERN),
) -> dict[str, bool]:
    principal = await authorize(request, "viewer")
    if principal.actor_type != "human":
        raise HTTPException(status_code=403, detail="Human identity required")
    await require_access(request, principal, kind, resource_id, "read")
    await _store(request).clear_presence(principal, kind, resource_id)
    return {"cleared": True}


@router.post("/agents/{agent_id}/heartbeat")
async def agent_heartbeat(
    payload: AgentHeartbeat,
    request: Request,
    agent_id: str = Path(pattern=RESOURCE_PATTERN),
) -> dict[str, object]:
    principal = await authorize(request, "operator")
    await _live(request, principal.tenant_id, "agent", agent_id)
    try:
        value = await _store(request).heartbeat(
            principal,
            agent_id,
            payload.status,
            payload.instance_id,
            request.app.state.settings.agent_heartbeat_ttl_seconds,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail="Agent may heartbeat only itself") from exc
    return {"heartbeat": value, "lease_or_lock": False}


@router.get("/agents/{agent_id}/heartbeat")
async def get_agent_heartbeat(
    request: Request, agent_id: str = Path(pattern=RESOURCE_PATTERN)
) -> dict[str, object]:
    principal = await authorize(request, "viewer")
    await _live(request, principal.tenant_id, "agent", agent_id)
    return {
        "heartbeat": await _store(request).get_heartbeat(principal.tenant_id, agent_id),
        "lease_or_lock": False,
    }
