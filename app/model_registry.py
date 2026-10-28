"""Administrative API for shared model identity and explicit calibration."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, StrictInt

from app.core.model_registry import ModelRegistry, ModelRegistryConflict
from app.core.security import authorize

router = APIRouter(prefix="/v1/models", tags=["models"])


class PublicationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    alias: str = Field(min_length=1, max_length=200)
    expected_revision: StrictInt = Field(ge=0)
    actual_model_version: str | None = Field(default=None, min_length=1, max_length=256)
    deployment_id: str | None = Field(default=None, min_length=1, max_length=256)
    tokenizer_version: str | None = Field(default=None, min_length=1, max_length=256)
    runtime_version: str | None = Field(default=None, min_length=1, max_length=256)


class CalibrationInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    alias: str = Field(min_length=1, max_length=200)
    publication_revision: StrictInt = Field(ge=1)
    expected_revision: StrictInt = Field(ge=0)
    minimum_samples: StrictInt | None = Field(default=None, ge=5, le=10_000)


def _registry(request: Request) -> ModelRegistry:
    if not request.app.state.settings.model_publication_enabled:
        raise HTTPException(status_code=409, detail="Model publication is disabled")
    runtime = request.app.state.redis
    if runtime is None or not hasattr(runtime, "_client"):
        raise HTTPException(status_code=503, detail="Model publication requires Redis")
    return ModelRegistry(runtime)


@router.post("/publications")
async def publish_model(payload: PublicationInput, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    try:
        route = request.app.state.model_router.planning_route(payload.alias)
        return await _registry(request).publish(
            principal.tenant_id,
            principal.subject,
            route,
            payload.model_dump(exclude={"alias", "expected_revision"}),
            payload.expected_revision,
        )
    except ModelRegistryConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/publications")
async def get_publication(
    request: Request, alias: str = Query(min_length=1, max_length=200)
) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    try:
        request.app.state.model_router.planning_route(alias)
        value = await _registry(request).get(principal.tenant_id, alias)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if value is None:
        raise HTTPException(status_code=404, detail="Model publication not found")
    return value


@router.post("/calibrations")
async def calibrate_model(payload: CalibrationInput, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    registry = _registry(request)
    publication = await registry.get(principal.tenant_id, payload.alias)
    if publication is None:
        raise HTTPException(status_code=404, detail="Model publication not found")
    if publication["revision"] != payload.publication_revision:
        raise HTTPException(status_code=409, detail="Model publication revision conflict")
    try:
        return await registry.calibrate(
            principal.tenant_id,
            principal.subject,
            publication,
            payload.minimum_samples or request.app.state.settings.forecast_calibration_min_samples,
            payload.expected_revision,
        )
    except ModelRegistryConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/telemetry")
async def list_model_telemetry(
    request: Request,
    after: str = Query(default="0-0", pattern=r"^[0-9]+-[0-9]+$"),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    records, next_after = await _registry(request).telemetry(
        principal.tenant_id, after=after, limit=limit
    )
    return {"records": records, "next_after": next_after}
