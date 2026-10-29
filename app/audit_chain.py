"""Human-admin verification and signed-checkpoint API for tenant audit chains."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict

from app.core.audit_chain import AuditChainStore, verify_checkpoint
from app.core.security import authorize

router = APIRouter(prefix="/v1/audit-chain", tags=["audit-chain"])


class CheckpointVerifyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    checkpoint: dict[str, Any]


async def _admin(request: Request):
    principal = await authorize(request, "admin")
    if principal.actor_type != "human":
        raise HTTPException(status_code=403, detail="Human admin identity required")
    if request.app.state.redis is None or not hasattr(request.app.state.redis, "_client"):
        raise HTTPException(status_code=503, detail="Audit chain requires Redis")
    return principal


@router.get("/verify")
async def verify_audit_chain(request: Request) -> dict[str, Any]:
    principal = await _admin(request)
    try:
        return await AuditChainStore(request.app.state.redis).verify(principal.tenant_id)
    except (RuntimeError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=409, detail="Audit chain cannot be verified") from exc


@router.post("/checkpoints")
async def export_audit_checkpoint(request: Request) -> dict[str, Any]:
    principal = await _admin(request)
    settings = request.app.state.settings
    if len(settings.audit_checkpoint_signing_key.encode("utf-8")) < 32:
        raise HTTPException(status_code=503, detail="Audit checkpoint signing is not configured")
    try:
        # The export request becomes part of the signed head rather than an unaudited read.
        await request.app.state.redis.append_audit(
            principal.tenant_id,
            {"action": "audit.checkpoint_exported", "subject": principal.subject},
        )
        return await AuditChainStore(request.app.state.redis).export_checkpoint(
            principal.tenant_id,
            secret=settings.audit_checkpoint_signing_key,
            key_id=settings.audit_checkpoint_key_id,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=503, detail="Audit checkpoint signing is not configured"
        ) from exc
    except (RuntimeError, TypeError) as exc:
        raise HTTPException(status_code=409, detail="Audit checkpoint cannot be exported") from exc


@router.post("/checkpoints/verify")
async def verify_audit_checkpoint(
    payload: CheckpointVerifyRequest, request: Request
) -> dict[str, bool]:
    principal = await _admin(request)
    settings = request.app.state.settings
    secret = settings.audit_checkpoint_signing_key
    if len(secret.encode("utf-8")) < 32:
        raise HTTPException(status_code=503, detail="Audit checkpoint signing is not configured")
    return {
        "valid": verify_checkpoint(
            payload.checkpoint,
            secret,
            tenant_id=principal.tenant_id,
            key_id=settings.audit_checkpoint_key_id,
        )
    }
