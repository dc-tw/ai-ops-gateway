"""Human-admin API for server-approved MCP and ATR policy references."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field

from app.core.security import authorize
from app.core.security_policies import (
    PolicyConfig,
    SecurityPolicyStore,
    server_fingerprint,
)


class SecurityPolicyRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def handle(request):
            try:
                return await original(request)
            except RequestValidationError:
                return JSONResponse(
                    status_code=422, content={"detail": "Invalid security policy request"}
                )

        return handle


router = APIRouter(
    prefix="/v1/security-policies",
    tags=["security-policies"],
    route_class=SecurityPolicyRoute,
)


class SecurityPolicyCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=200)
    config: PolicyConfig


class SecurityPolicyUpdate(SecurityPolicyCreate):
    expected_version: int = Field(ge=1)


class SecurityPolicyEnabled(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool


def _store(request: Request) -> SecurityPolicyStore:
    runtime = request.app.state.redis
    if runtime is None or not hasattr(runtime, "_client"):
        raise HTTPException(status_code=503, detail="Security policies require Redis")
    return SecurityPolicyStore(runtime)


async def _admin(request: Request):
    principal = await authorize(request, "admin")
    if principal.actor_type != "human":
        raise HTTPException(status_code=403, detail="Human admin identity required")
    return principal


def _fingerprint(payload: SecurityPolicyCreate, request: Request) -> tuple[dict[str, Any], str]:
    config = payload.config.model_dump(mode="json")
    try:
        fingerprint = server_fingerprint(
            config, request.app.state.settings, request.app.state.threat_detector
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return config, fingerprint


@router.post("", status_code=201)
async def create_security_policy(
    payload: SecurityPolicyCreate, request: Request
) -> dict[str, Any]:
    principal = await _admin(request)
    config, fingerprint = _fingerprint(payload, request)
    try:
        record = await _store(request).write(
            principal.tenant_id,
            name=payload.name,
            config=config,
            fingerprint=fingerprint,
            subject=principal.subject,
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(
            status_code=503, detail="Security policy storage unavailable"
        ) from exc
    if record is None:  # Defensive: create writes never use optimistic-version misses.
        raise HTTPException(status_code=503, detail="Security policy storage unavailable")
    return record


@router.get("")
async def list_security_policies(request: Request) -> dict[str, Any]:
    principal = await _admin(request)
    return {"policies": await _store(request).list(principal.tenant_id)}


@router.get("/{policy_id}")
async def get_security_policy(policy_id: str, request: Request) -> dict[str, Any]:
    principal = await _admin(request)
    record = await _store(request).get(principal.tenant_id, policy_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Security policy not found")
    return record


@router.put("/{policy_id}")
async def update_security_policy(
    policy_id: str, payload: SecurityPolicyUpdate, request: Request
) -> dict[str, Any]:
    principal = await _admin(request)
    config, fingerprint = _fingerprint(payload, request)
    try:
        record = await _store(request).write(
            principal.tenant_id,
            policy_id=policy_id,
            expected_version=payload.expected_version,
            name=payload.name,
            config=config,
            fingerprint=fingerprint,
            subject=principal.subject,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail="Security policy storage unavailable") from exc
    if record is None:
        if await _store(request).get(principal.tenant_id, policy_id) is None:
            raise HTTPException(status_code=404, detail="Security policy not found")
        raise HTTPException(status_code=409, detail="Security policy version conflict")
    return record


@router.post("/{policy_id}/enabled")
async def set_security_policy_enabled(
    policy_id: str, payload: SecurityPolicyEnabled, request: Request
) -> dict[str, Any]:
    principal = await _admin(request)
    record = await _store(request).set_enabled(
        principal.tenant_id, policy_id, payload.enabled, principal.subject
    )
    if record is None:
        raise HTTPException(status_code=404, detail="Security policy not found")
    return record
