"""Read-only planning sessions: accepting a plan never dispatches execution."""

from __future__ import annotations

import asyncio
import json
import re
from copy import deepcopy
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, StrictInt, model_validator

from app.core.planning_snapshot import (
    PlanningSnapshotInvalid,
    PlanningSnapshotNotFound,
    build_planning_snapshot,
    guard_planning_snapshot,
    validate_planning_plan,
)
from app.core.planning_store import (
    MAX_PLAN_BYTES,
    PlanningConflict,
    PlanningIntegrityError,
    PlanningNotFound,
    PlanningStore,
)
from app.core.security import authorize, redact_pii
from app.core.threat_detection import reject_incomplete_scan
from app.core.threat_feedback import record_threat_findings
from app.core.workflow_plan import (
    WorkflowPlan,
    adapt_legacy_plan,
    compile_workflow,
    load_workflow_yaml,
)


class PlanningRoute(APIRoute):
    """Keep private drafts and malformed Unicode out of validation responses."""

    def get_route_handler(self):
        original = super().get_route_handler()

        async def handle(request: Request):
            try:
                return await original(request)
            except RequestValidationError:
                # Error input, context, locations and messages may contain user
                # text, including surrogate characters that JSON cannot encode.
                return JSONResponse(status_code=422, content={"detail": "Invalid planning request"})

        return handle


router = APIRouter(prefix="/v1", tags=["planning"], route_class=PlanningRoute)
ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9-]{0,127}$"


class PlanningInput(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True, allow_inf_nan=False)


class PlanningStep(PlanningInput):
    query: str = Field(min_length=1, max_length=8_000)
    target_url: HttpUrl | None = None


class WorkerAssignment(PlanningInput):
    agent_id: str = Field(pattern=ID_PATTERN)
    objective: str = Field(min_length=1, max_length=8_000)
    steps: list[PlanningStep] = Field(default_factory=list, max_length=100)


class PlanContent(PlanningInput):
    objective: str = Field(min_length=1, max_length=8_000)
    context: str = Field(default="", max_length=16_000)
    steps: list[PlanningStep] = Field(default_factory=list, max_length=100)
    assignments: list[WorkerAssignment] = Field(default_factory=list, max_length=8)
    max_retries: StrictInt = Field(default=2, ge=0, le=5)
    max_tool_rounds: StrictInt = Field(default=8, ge=1, le=20)
    step_deadline_seconds: float = Field(default=300.0, ge=1, le=3_600)

    @model_validator(mode="after")
    def bounded_plan(self) -> PlanContent:
        if not self.objective.strip() or any(not step.query.strip() for step in self.steps):
            raise ValueError("Plan objective and queries cannot be blank")
        if any(
            not item.objective.strip() or any(not step.query.strip() for step in item.steps)
            for item in self.assignments
        ):
            raise ValueError("Assignment objective and queries cannot be blank")
        if len({item.agent_id for item in self.assignments}) != len(self.assignments):
            raise ValueError("Assign each Worker at most once")
        try:
            encoded = json.dumps(self.model_dump(mode="json"), ensure_ascii=False).encode("utf-8")
        except UnicodeError:
            raise ValueError("Plan text must be valid UTF-8") from None
        if len(encoded) > MAX_PLAN_BYTES:
            raise ValueError("Plan exceeds the 64 KiB UTF-8 limit")
        return self


class PlanSave(PlanningInput):
    expected_version: StrictInt = Field(ge=1)
    plan: PlanContent | WorkflowPlan


class WorkflowYamlSave(PlanningInput):
    expected_version: StrictInt = Field(ge=1)
    yaml: str = Field(min_length=1, max_length=65_536)


def _validate_plan(request, snapshot, plan):
    if plan.get("schema_version") == 2:
        compile_workflow(plan, snapshot, check_schemas=False, **_workflow_limits(request))
        return True
    return validate_planning_plan(snapshot, plan)


def _workflow_limits(request):
    settings = request.app.state.settings
    return {
        "max_nodes": settings.workflow_max_nodes,
        "max_edges": settings.workflow_max_edges,
        "max_parallelism": min(
            settings.workflow_max_parallelism, settings.tenant_max_concurrent_runs
        ),
    }


def _workflow_enabled(request):
    if not request.app.state.settings.workflow_enabled:
        raise HTTPException(status_code=409, detail="Workflow authoring is disabled")


class PlanningRefresh(PlanningInput):
    expected_version: StrictInt = Field(ge=1)


class PlanningMode(PlanningRefresh):
    mode: Literal["plan", "execute"]
    plan_revision: StrictInt | None = Field(default=None, ge=1)


def _store(request: Request) -> PlanningStore:
    runtime = request.app.state.redis
    if runtime is None or not hasattr(runtime, "_client"):
        raise HTTPException(status_code=503, detail="Planning requires Redis")
    return PlanningStore(runtime, workflow_limits=_workflow_limits(request))


def _error(exc: ValueError) -> HTTPException:
    if isinstance(exc, (PlanningNotFound, PlanningSnapshotNotFound)):
        code = 404
    elif isinstance(exc, (PlanningConflict, PlanningSnapshotInvalid, PlanningIntegrityError)):
        code = 409
    else:
        code = 422
    # Stored persona, instructions and corrupted payloads must never enter errors.
    detail = "Planning resource not found" if code == 404 else str(exc)
    if isinstance(exc, PlanningIntegrityError):
        detail = "Planning record failed integrity verification"
    return HTTPException(status_code=code, detail=detail)


async def _rate_limit(request: Request, tenant: str) -> None:
    try:
        await request.app.state.redis.enforce_rate_limit(
            tenant, request.app.state.settings.tenant_requests_per_minute
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=429, detail="Planning request rate limit exceeded") from exc


def _builder(request: Request, tenant: str, agent_id: str):
    async def build(pipe):
        return await build_planning_snapshot(
            request.app.state.redis,
            request.app.state.model_router,
            tenant,
            agent_id,
            None,
            client=pipe,
            watch=True,
        )

    return build


async def _session(store: PlanningStore, tenant: str, session_id: str) -> dict:
    value = await store.get(tenant, session_id)
    if value is None:
        raise PlanningNotFound("Planning session not found")
    return value


async def _scan_plan(request: Request, tenant: str, session_id: str, plan: dict) -> dict:
    settings = request.app.state.settings
    safe = deepcopy(plan)
    texts = [(safe, "objective"), (safe, "context")]
    texts.extend((step, "query") for step in safe.get("steps", []))
    texts.extend((node, "query") for node in safe.get("nodes", []))
    for assignment in safe.get("assignments", []):
        texts.append((assignment, "objective"))
        texts.extend((step, "query") for step in assignment["steps"])
    for item, field in texts:
        if settings.pii_redaction:
            item[field] = redact_pii(item[field])
    # Preserve literal newlines for detection; JSON escaping must not alter the
    # text that will eventually become model context. Include every URL too.
    urls = [step["target_url"] for step in safe.get("steps", []) if step.get("target_url")]
    urls.extend(node["target_url"] for node in safe.get("nodes", []) if node.get("target_url"))
    urls.extend(
        step["target_url"]
        for assignment in safe.get("assignments", [])
        for step in assignment["steps"]
        if step.get("target_url")
    )
    text = "\n\n".join([item[field] for item, field in texts] + urls)
    if safe.get("schema_version") == 2:
        # Include literal condition/default/schema strings, not escaped JSON.
        pending, strings = [safe], set()
        while pending:
            value = pending.pop()
            if isinstance(value, dict):
                pending.extend(value.keys())
                pending.extend(value.values())
            elif isinstance(value, list):
                pending.extend(value)
            elif isinstance(value, str):
                strings.add(value)
        text = "\n\n".join(sorted(strings))
    if settings.atr_enabled and len(text) > settings.atr_max_content_chars:
        raise HTTPException(status_code=413, detail="Plan exceeds the configured full-scan limit")
    if any(
        re.search(pattern, text, re.IGNORECASE) for pattern in settings.content_blocked_patterns
    ):
        raise HTTPException(status_code=403, detail="Plan blocked by content policy")
    scan = await request.app.state.threat_detector.ascan(
        text, event_type="llm_input", location="planning_plan", fields={"user_input": text}
    )
    if scan.needs_record:
        await record_threat_findings(
            request.app.state.redis,
            settings,
            tenant,
            scan.to_event(),
            context_id=f"planning:{session_id}",
            model="planning-input",
            emit_internal_events=False,
        )
    if scan.blocked:
        reject_incomplete_scan(scan)
        raise HTTPException(status_code=403, detail="Plan blocked by ATR policy")
    return safe


async def _view(request: Request, tenant: str, session: dict) -> dict:
    try:
        await guard_planning_snapshot(
            request.app.state.redis,
            request.app.state.model_router,
            tenant,
            session["snapshot"],
            client=request.app.state.redis._client,
            watch=False,
        )
        status = "current"
    except (PlanningSnapshotInvalid, PlanningSnapshotNotFound, ValueError):
        status = "stale"
    return session | {"configuration_status": status, "execution_started": False}


@router.post("/agents/{agent_id}/planning-sessions")
async def create_session(
    request: Request,
    agent_id: str = Path(pattern=ID_PATTERN),
    payload: PlanningInput | None = None,
) -> dict[str, Any]:
    principal = await authorize(request, "operator")
    store = _store(request)
    await _rate_limit(request, principal.tenant_id)
    try:
        saved = await store.create(
            principal.tenant_id,
            agent_id,
            principal.subject,
            _builder(request, principal.tenant_id, agent_id),
        )
        return await _view(request, principal.tenant_id, saved)
    except ValueError as exc:
        raise _error(exc) from exc


@router.get("/planning-sessions/{session_id}")
async def get_session(
    request: Request, session_id: str = Path(pattern=ID_PATTERN)
) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    try:
        saved = await _session(_store(request), principal.tenant_id, session_id)
        return await _view(request, principal.tenant_id, saved)
    except ValueError as exc:
        raise _error(exc) from exc


@router.post("/planning-sessions/{session_id}/plans")
async def save_plan(
    payload: PlanSave, request: Request, session_id: str = Path(pattern=ID_PATTERN)
) -> dict[str, Any]:
    principal = await authorize(request, "operator")
    native = isinstance(payload.plan, WorkflowPlan)
    if native:
        _workflow_enabled(request)
    store = _store(request)
    try:
        session = await _session(store, principal.tenant_id, session_id)
        if session["mode"] != "plan":
            raise PlanningConflict("Plan changes require plan mode")
        if session["version"] != payload.expected_version:
            raise PlanningConflict("Planning session version conflict")
        await _rate_limit(request, principal.tenant_id)
        plan = await _scan_plan(
            request, principal.tenant_id, session_id, payload.plan.model_dump(mode="json")
        )
        complete = True if native else validate_planning_plan(session["snapshot"], plan)
        saved = await store.save_plan(
            principal.tenant_id,
            session_id,
            plan,
            payload.expected_version,
            principal.subject,
            _builder(request, principal.tenant_id, session["agent_id"]),
            complete=complete,
        )
        return await _view(request, principal.tenant_id, saved)
    except ValueError as exc:
        raise _error(exc) from exc


@router.post("/planning-sessions/{session_id}/workflow-yaml")
async def save_workflow_yaml(
    payload: WorkflowYamlSave, request: Request, session_id: str = Path(pattern=ID_PATTERN)
):
    await authorize(request, "operator")
    _workflow_enabled(request)
    try:
        plan = await asyncio.to_thread(load_workflow_yaml, payload.yaml)
        return await save_plan(
            PlanSave(expected_version=payload.expected_version, plan=plan), request, session_id
        )
    except ValueError as exc:
        raise _error(exc) from exc


@router.get("/planning-sessions/{session_id}/compiled")
async def get_compiled_plan(
    request: Request,
    session_id: str = Path(pattern=ID_PATTERN),
    revision: int | None = Query(default=None, ge=1),
):
    principal = await authorize(request, "viewer")
    try:
        store = _store(request)
        session = await _session(store, principal.tenant_id, session_id)
        number = revision or session["plan_revision"]
        if not number:
            raise PlanningNotFound("Plan revision not found")
        artifact = await store.get_revision(principal.tenant_id, session_id, number)
        if artifact is None:
            raise PlanningNotFound("Plan revision not found")
        compiled = artifact.get("compiled")
        if compiled is None:
            _workflow_enabled(request)
            await _rate_limit(request, principal.tenant_id)
            adapted = adapt_legacy_plan(artifact["plan"], artifact["snapshot"])
            compiled = await asyncio.to_thread(
                compile_workflow, adapted, artifact["snapshot"], **_workflow_limits(request)
            )
        return {
            "session_id": session_id,
            "plan_revision": number,
            "compiled": compiled,
            "source_kind": "native" if artifact.get("compiled") else "legacy_adapter",
            "execution_started": False,
            "execution_authority": False,
        }
    except ValueError as exc:
        raise _error(exc) from exc


@router.get("/planning-sessions/{session_id}/plans")
async def list_plans(
    request: Request,
    session_id: str = Path(pattern=ID_PATTERN),
    after: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=100),
) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    try:
        plans, next_after = await _store(request).revisions(
            principal.tenant_id, session_id, after=after, limit=limit
        )
        return {"plans": plans, "next_after": next_after}
    except ValueError as exc:
        raise _error(exc) from exc


@router.get("/planning-sessions/{session_id}/plans/{revision}")
async def get_plan(
    request: Request,
    session_id: str = Path(pattern=ID_PATTERN),
    revision: int = Path(ge=1),
) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    try:
        value = await _store(request).get_revision(principal.tenant_id, session_id, revision)
        if value is None:
            raise PlanningNotFound("Plan revision not found")
        return value
    except ValueError as exc:
        raise _error(exc) from exc


@router.post("/planning-sessions/{session_id}/refresh")
async def refresh_session(
    payload: PlanningRefresh, request: Request, session_id: str = Path(pattern=ID_PATTERN)
) -> dict[str, Any]:
    principal = await authorize(request, "operator")
    store = _store(request)
    try:
        session = await _session(store, principal.tenant_id, session_id)
        if session["mode"] != "plan":
            raise PlanningConflict("Refresh requires plan mode")
        if session["version"] != payload.expected_version:
            raise PlanningConflict("Planning session version conflict")
        await _rate_limit(request, principal.tenant_id)
        if session["plan_revision"]:
            revision = await store.get_revision(
                principal.tenant_id, session_id, session["plan_revision"]
            )
            if revision is None:
                raise PlanningIntegrityError("Saved plan revision is unavailable")
            # Recheck current input policy, but refresh never changes saved plan text.
            scanned = await _scan_plan(request, principal.tenant_id, session_id, revision["plan"])
            if scanned != revision["plan"]:
                raise PlanningConflict(
                    "Saved plan requires redaction under current policy; create a new session"
                )
        saved = await store.refresh(
            principal.tenant_id,
            session_id,
            payload.expected_version,
            principal.subject,
            _builder(request, principal.tenant_id, session["agent_id"]),
            plan_validator=lambda snapshot, plan: _validate_plan(request, snapshot, plan),
        )
        return await _view(request, principal.tenant_id, saved)
    except ValueError as exc:
        raise _error(exc) from exc


@router.post("/planning-sessions/{session_id}/mode")
async def change_mode(
    payload: PlanningMode, request: Request, session_id: str = Path(pattern=ID_PATTERN)
) -> dict[str, Any]:
    principal = await authorize(request, "operator")
    store = _store(request)
    try:
        session = await _session(store, principal.tenant_id, session_id)
        await _rate_limit(request, principal.tenant_id)
        saved = await store.set_mode(
            principal.tenant_id,
            session_id,
            payload.mode,
            payload.expected_version,
            principal.subject,
            _builder(request, principal.tenant_id, session["agent_id"]),
            plan_revision=payload.plan_revision,
        )
        return await _view(request, principal.tenant_id, saved)
    except ValueError as exc:
        raise _error(exc) from exc
