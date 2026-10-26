"""Authenticated API for reusable, versioned Gateway instruction Skills."""

from __future__ import annotations

import json
import re
from typing import Any

from fastapi import APIRouter, HTTPException, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field, StrictInt

from app.core.builtin_skills import BUILTIN_SKILLS, get_builtin
from app.core.security import authorize
from app.core.skill_store import (
    SkillConflict,
    SkillContent,
    SkillNotFound,
    SkillStore,
    content_fields,
)
from app.core.threat_detection import reject_incomplete_scan
from app.core.threat_feedback import record_threat_findings


class SkillRoute(APIRoute):
    """Reject malformed requests without echoing private Skill content."""

    def get_route_handler(self):
        original = super().get_route_handler()

        async def handle(request: Request):
            try:
                return await original(request)
            except RequestValidationError:
                # Input/ctx and even error locations can contain instructions,
                # sensitive version values or unencodable surrogate characters.
                return JSONResponse(status_code=422, content={"detail": "Invalid Skill request"})

        return handle


router = APIRouter(prefix="/v1/skills", tags=["skills"], route_class=SkillRoute)


class SkillCreate(SkillContent):
    enabled: bool = True


class SkillUpdate(SkillContent):
    expected_version: StrictInt = Field(ge=1)


class BuiltinInstall(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=64)


class SkillEnabled(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool
    expected_version: StrictInt = Field(ge=1)


class SkillRollback(BaseModel):
    model_config = ConfigDict(extra="forbid")
    revision: StrictInt = Field(ge=1)
    expected_version: StrictInt = Field(ge=1)


def _store(request: Request) -> SkillStore:
    runtime = request.app.state.redis
    if runtime is None:
        raise HTTPException(status_code=503, detail="Skill management requires Redis")
    return getattr(request.app.state, "skill_store", None) or SkillStore(runtime)


def _summary(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key != "instructions"}


def _error(exc: ValueError) -> HTTPException:
    status = (
        404 if isinstance(exc, SkillNotFound) else 409 if isinstance(exc, SkillConflict) else 422
    )
    return HTTPException(status_code=status, detail=str(exc))


async def _check_content(request: Request, tenant: str, body: dict[str, Any]) -> None:
    """Admin authors remain subject to content checks; this is not authorization."""
    settings = request.app.state.settings
    text = "\n\n".join(body[field] for field in ("display_name", "description", "instructions"))
    if any(
        re.search(pattern, text, re.IGNORECASE) for pattern in settings.content_blocked_patterns
    ):
        raise HTTPException(status_code=403, detail="Skill blocked by content policy")
    scan = await request.app.state.threat_detector.ascan(
        text, event_type="llm_input", location="skill_definition", fields={"user_input": text}
    )
    if scan.needs_record:
        await record_threat_findings(
            request.app.state.redis,
            settings,
            tenant,
            scan.to_event(),
            context_id=f"skill-definition:{body['name']}",
            model="skill-definition",
        )
    if scan.blocked:
        reject_incomplete_scan(scan)
        raise HTTPException(status_code=403, detail="Skill blocked by ATR policy")


@router.get("/builtins")
async def list_builtin_skills(request: Request) -> dict[str, Any]:
    await authorize(request, "viewer")
    return {"skills": [_summary(value) for value in BUILTIN_SKILLS]}


@router.get("/builtins/{name}")
async def get_builtin_skill(name: str, request: Request) -> dict[str, Any]:
    await authorize(request, "viewer")
    value = get_builtin(name)
    if value is None:
        raise HTTPException(status_code=404, detail="Built-in Skill not found")
    return value


@router.post("/install")
async def install_builtin_skill(payload: BuiltinInstall, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    store = _store(request)
    builtin = get_builtin(payload.name)
    if builtin is None:
        raise HTTPException(status_code=404, detail="Built-in Skill not found")
    body = SkillContent.model_validate(content_fields(builtin)).model_dump()
    await _check_content(request, principal.tenant_id, body)
    try:
        return await store.create(
            principal.tenant_id,
            body,
            principal.subject,
            builtin_version=builtin["builtin_version"],
        )
    except ValueError as exc:
        raise _error(exc) from exc


@router.post("")
async def create_skill(payload: SkillCreate, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    store = _store(request)
    body = content_fields(payload.model_dump())
    await _check_content(request, principal.tenant_id, body)
    try:
        return await store.create(
            principal.tenant_id, body, principal.subject, enabled=payload.enabled
        )
    except ValueError as exc:
        raise _error(exc) from exc


@router.get("")
async def list_skills(request: Request) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    return {
        "skills": [_summary(value) for value in await _store(request).list(principal.tenant_id)]
    }


@router.get("/{skill_id}")
async def get_skill(skill_id: str, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    value = await _store(request).get(principal.tenant_id, skill_id)
    if value is None:
        raise HTTPException(status_code=404, detail="Skill not found")
    return value


@router.put("/{skill_id}")
@router.post("/{skill_id}/revisions")
async def publish_skill(skill_id: str, payload: SkillUpdate, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    store = _store(request)
    if await store.get(principal.tenant_id, skill_id) is None:
        raise HTTPException(status_code=404, detail="Skill not found")
    body = content_fields(payload.model_dump())
    await _check_content(request, principal.tenant_id, body)
    try:
        return await store.publish(
            principal.tenant_id, skill_id, body, payload.expected_version, principal.subject
        )
    except ValueError as exc:
        raise _error(exc) from exc


@router.get("/{skill_id}/revisions")
async def list_skill_revisions(
    skill_id: str,
    request: Request,
    after: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=100),
) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    try:
        values, next_after = await _store(request).revisions(
            principal.tenant_id, skill_id, after=after, limit=limit
        )
    except ValueError as exc:
        raise _error(exc) from exc
    return {"revisions": [_summary(value) for value in values], "next_after": next_after}


@router.get("/{skill_id}/revisions/{revision}")
async def get_skill_revision(
    skill_id: str, request: Request, revision: int = Path(ge=1)
) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    value = await _store(request).get_revision(principal.tenant_id, skill_id, revision)
    if value is None:
        raise HTTPException(status_code=404, detail="Skill revision not found")
    return value


@router.post("/{skill_id}/enabled")
async def set_skill_enabled(
    skill_id: str, payload: SkillEnabled, request: Request
) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    try:
        return await _store(request).set_enabled(
            principal.tenant_id,
            skill_id,
            payload.enabled,
            payload.expected_version,
            principal.subject,
        )
    except ValueError as exc:
        raise _error(exc) from exc


@router.post("/{skill_id}/rollback")
async def rollback_skill(skill_id: str, payload: SkillRollback, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    store = _store(request)
    value = await store.get_revision(principal.tenant_id, skill_id, payload.revision)
    if value is None:
        raise HTTPException(status_code=404, detail="Skill revision not found")
    body = content_fields(value)
    await _check_content(request, principal.tenant_id, body)
    try:
        return await store.publish(
            principal.tenant_id,
            skill_id,
            body,
            payload.expected_version,
            principal.subject,
            restored_from=payload.revision,
        )
    except ValueError as exc:
        raise _error(exc) from exc


@router.get("/{skill_id}/export")
async def export_skill(
    skill_id: str, request: Request, revision: int | None = Query(default=None, ge=1)
) -> Response:
    principal = await authorize(request, "viewer")
    store = _store(request)
    value = (
        await store.get(principal.tenant_id, skill_id)
        if revision is None
        else await store.get_revision(principal.tenant_id, skill_id, revision)
    )
    if value is None:
        raise HTTPException(status_code=404, detail="Skill or revision not found")

    # JSON-quoted scalars are valid YAML; no executable resources or filesystem paths.
    def quote(text: Any) -> str:
        return json.dumps(str(text), ensure_ascii=False)

    text = (
        "---\n"
        + f"name: {quote(value['name'])}\n"
        + f"description: {quote(value['description'])}\nmetadata:\n"
        + f"  gateway-skill-id: {quote(value['skill_id'])}\n"
        + f"  gateway-revision: {quote(value['revision'])}\n"
        + f"  gateway-content-sha256: {quote(value['content_sha256'])}\n"
        + f"  gateway-required-tools: {quote(' '.join(value['required_tools']))}\n"
        + "---\n\n"
        + value["instructions"]
        + "\n"
    )
    return Response(
        text,
        media_type="text/markdown",
        headers={"content-disposition": f'attachment; filename="{value["name"]}-SKILL.md"'},
    )
