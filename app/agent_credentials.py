"""Admin API for one-time Agent credential issuance, rotation, and revocation."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from app.core.agent_credentials import AgentCredentialStore
from app.core.security import authorize

router = APIRouter(prefix="/v1/agents", tags=["agent-credentials"])


class CredentialIssue(BaseModel):
    model_config = ConfigDict(extra="forbid")
    label: str = Field(default="default", min_length=1, max_length=100)
    expires_in_seconds: int = Field(default=2_592_000, ge=60, le=31_536_000)


def _store(request: Request) -> AgentCredentialStore:
    runtime = request.app.state.redis
    if runtime is None or not hasattr(runtime, "_client"):
        raise HTTPException(status_code=503, detail="Agent credentials require Redis")
    return AgentCredentialStore(runtime)


async def _issue(
    agent_id: str, payload: CredentialIssue, request: Request, *, rotate: bool
) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    if principal.actor_type != "human":
        raise HTTPException(status_code=403, detail="Human admin identity required")
    try:
        credential, token = await _store(request).issue(
            principal.tenant_id,
            agent_id,
            label=payload.label,
            expires_in_seconds=payload.expires_in_seconds,
            issued_by=principal.subject,
            rotate=rotate,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except FileExistsError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"credential": credential, "token": token, "token_visible_once": True}


@router.post("/{agent_id}/credentials", status_code=201)
async def issue_credential(
    agent_id: str, payload: CredentialIssue, request: Request
) -> dict[str, Any]:
    return await _issue(agent_id, payload, request, rotate=False)


@router.post("/{agent_id}/credentials/rotate", status_code=201)
async def rotate_credential(
    agent_id: str, payload: CredentialIssue, request: Request
) -> dict[str, Any]:
    return await _issue(agent_id, payload, request, rotate=True)


@router.get("/{agent_id}/credentials")
async def list_credentials(agent_id: str, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    if principal.actor_type != "human":
        raise HTTPException(status_code=403, detail="Human admin identity required")
    try:
        credentials = await _store(request).list(principal.tenant_id, agent_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"credentials": credentials}


@router.delete("/{agent_id}/credentials/{credential_id}")
async def revoke_credential(
    agent_id: str, credential_id: str, request: Request
) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    if principal.actor_type != "human":
        raise HTTPException(status_code=403, detail="Human admin identity required")
    credential = await _store(request).revoke(
        principal.tenant_id,
        agent_id,
        credential_id,
        revoked_by=principal.subject,
    )
    if credential is None:
        raise HTTPException(status_code=404, detail="Agent credential not found")
    return credential
