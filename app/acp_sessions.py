"""Authenticated remote session registry used by the ACP stdio adapter."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Path, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from app.core.acp_sessions import AcpSessionStore
from app.core.resource_access import require_access
from app.core.security import authorize

router = APIRouter(prefix="/v1/acp/sessions", tags=["acp"])
ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"


class AcpSessionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    cwd_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    client_name: str = Field(default="unknown", min_length=1, max_length=128)


class AcpModeUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    expected_version: int = Field(ge=1)
    mode: Literal["plan", "execute"]


def _store(request: Request) -> AcpSessionStore:
    runtime = request.app.state.redis
    if runtime is None or not hasattr(runtime, "_client"):
        raise HTTPException(status_code=503, detail="ACP sessions require Redis")
    return AcpSessionStore(runtime, request.app.state.settings.conversation_ttl_seconds)


def _public(value: dict) -> dict:
    return {
        name: value[name]
        for name in (
            "schema_version",
            "session_id",
            "conversation_id",
            "mode",
            "version",
            "client_name",
            "created_at",
            "updated_at",
        )
    }


@router.post("")
async def create_acp_session(payload: AcpSessionCreate, request: Request) -> dict:
    principal = await authorize(request, "operator")
    saved = await _store(request).create(
        principal.tenant_id,
        principal.subject,
        principal.actor_type,
        payload.cwd_sha256,
        payload.client_name,
    )
    await request.app.state.redis.append_audit(
        principal.tenant_id,
        {
            "action": "acp.session.create",
            "status": "created",
            "subject": principal.subject,
            "session_id": saved["session_id"],
        },
    )
    return {"session": _public(saved), "execution_started": False}


@router.get("/{session_id}")
async def get_acp_session(
    request: Request,
    session_id: str = Path(pattern=ID_PATTERN),
    include_history: bool = Query(default=True),
) -> dict:
    principal = await authorize(request, "operator")
    value = await _store(request).get(principal.tenant_id, session_id)
    if value is None:
        raise HTTPException(status_code=404, detail="ACP session not found")
    await require_access(request, principal, "conversation", value["conversation_id"], "read")
    history = (
        await request.app.state.redis.get_conversation(
            value["conversation_id"], tenant_id=principal.tenant_id
        )
        if include_history
        else []
    )
    return {"session": _public(value), "history": history, "execution_started": False}


@router.put("/{session_id}/mode")
async def set_acp_mode(
    payload: AcpModeUpdate,
    request: Request,
    session_id: str = Path(pattern=ID_PATTERN),
) -> dict:
    principal = await authorize(request, "operator")
    await require_access(request, principal, "conversation", session_id, "operate")
    try:
        value = await _store(request).set_mode(
            principal.tenant_id, session_id, payload.mode, payload.expected_version
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if value is None:
        raise HTTPException(status_code=404, detail="ACP session not found")
    await request.app.state.redis.append_audit(
        principal.tenant_id,
        {
            "action": "acp.session.mode",
            "status": payload.mode,
            "subject": principal.subject,
            "session_id": session_id,
            "execution_started": False,
            "approval_granted": False,
        },
    )
    return {"session": _public(value), "execution_started": False, "approval_granted": False}
