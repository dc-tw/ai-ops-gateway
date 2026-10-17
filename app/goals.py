import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from app.api.routes import _sse
from app.api.schemas import GoalCreateRequest
from app.core.observability import observe_agent_event
from app.core.security import authorize, redact_pii

router = APIRouter(prefix="/v1/goals", tags=["goals"])
TERMINAL_STATUSES = {"completed", "failed", "cancelled"}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _require_redis(request: Request):
    runtime = request.app.state.redis
    if runtime is None:
        raise HTTPException(status_code=503, detail="Goal Mode requires Redis")
    return runtime


@router.post("")
async def create_goal(payload: GoalCreateRequest, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "operator")
    runtime = _require_redis(request)
    try:
        request.app.state.model_router.get(payload.model)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    goal_id = str(uuid4())
    now = _now()
    objective = (
        redact_pii(payload.objective)
        if request.app.state.settings.pii_redaction
        else payload.objective
    )
    steps = [
        {
            "id": str(uuid4()),
            "query": (
                redact_pii(step.query)
                if request.app.state.settings.pii_redaction
                else step.query
            ),
            "target_url": str(step.target_url) if step.target_url else None,
            "status": "pending",
            "answer": "",
            "error": "",
        }
        for step in payload.steps
    ]
    threat_scans = [
        await request.app.state.threat_detector.ascan(
            objective,
            event_type="llm_input",
            location="goal_objective",
            fields={"user_input": objective},
        ),
        *[
            await request.app.state.threat_detector.ascan(
                step["query"],
                event_type="llm_input",
                location=f"goal_step:{index}",
                fields={"user_input": step["query"]},
            )
            for index, step in enumerate(steps)
        ],
    ]
    threat_scans = [scan for scan in threat_scans if scan.findings]
    model_name = payload.model or request.app.state.model_router.default_model
    for scan in threat_scans:
        observe_agent_event(scan.to_event(), model=model_name)
    blocked_scan = next((scan for scan in threat_scans if scan.blocked), None)
    if blocked_scan:
        await runtime.append_audit(
            principal.tenant_id,
            {
                "action": "goal.threat_detection",
                "status": "blocked",
                "subject": principal.subject,
                "goal_id": goal_id,
                "event": blocked_scan.to_event(),
            },
        )
        raise HTTPException(
            status_code=403,
            detail={
                "error": "atr_threat_blocked",
                "goal_id": goal_id,
                "findings": list(blocked_scan.findings),
            },
        )
    goal = {
        "goal_id": goal_id,
        "objective": objective,
        "model": model_name,
        "max_retries": payload.max_retries,
        "max_tool_rounds": payload.max_tool_rounds,
        "step_deadline_seconds": payload.step_deadline_seconds,
        "status": "queued",
        "current_step": 0,
        "steps": steps,
        "created_at": now,
        "updated_at": now,
        "tenant_id": principal.tenant_id,
        "created_by": principal.subject,
    }
    await runtime.save_goal(goal_id, goal, principal.tenant_id)
    await runtime.append_goal_event(
        goal_id, {"type": "goal_queued", "goal": goal}, principal.tenant_id
    )
    for scan in threat_scans:
        await runtime.append_goal_event(goal_id, scan.to_event(), principal.tenant_id)
        await runtime.append_audit(
            principal.tenant_id,
            {
                "action": "goal.threat_detection",
                "status": "detected",
                "subject": principal.subject,
                "goal_id": goal_id,
                "event": scan.to_event(),
            },
        )
    await runtime.enqueue_goal(goal_id, principal.tenant_id)
    await runtime.append_audit(
        principal.tenant_id,
        {
            "action": "goal.create",
            "status": "queued",
            "subject": principal.subject,
            "goal_id": goal_id,
        },
    )
    return goal


@router.post("/{goal_id}/cancel")
async def cancel_goal(goal_id: str, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "operator")
    runtime = _require_redis(request)
    goal = await runtime.get_goal(goal_id, principal.tenant_id)
    if goal is None:
        raise HTTPException(status_code=404, detail="Goal not found")
    if goal["status"] in TERMINAL_STATUSES:
        raise HTTPException(status_code=409, detail="Goal is already terminal")
    goal["status"] = "cancelled"
    goal["updated_at"] = _now()
    await runtime.save_goal(goal_id, goal, principal.tenant_id)
    await runtime.append_goal_event(
        goal_id, {"type": "goal_cancelled", "by": principal.subject}, principal.tenant_id
    )
    await runtime.append_audit(
        principal.tenant_id,
        {
            "action": "goal.cancel",
            "status": "completed",
            "subject": principal.subject,
            "goal_id": goal_id,
        },
    )
    return goal


@router.get("")
async def list_goals(request: Request) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    runtime = _require_redis(request)
    goals = []
    for goal_id in await runtime.list_goal_ids(principal.tenant_id):
        goal = await runtime.get_goal(goal_id, principal.tenant_id)
        if goal:
            goals.append(goal)
    goals.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return {"goals": goals}


@router.get("/{goal_id}")
async def get_goal(goal_id: str, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    runtime = _require_redis(request)
    goal = await runtime.get_goal(goal_id, principal.tenant_id)
    if goal is None:
        raise HTTPException(status_code=404, detail="Goal not found")
    return goal


@router.post("/{goal_id}/pause")
async def pause_goal(goal_id: str, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "operator")
    runtime = _require_redis(request)
    goal = await runtime.get_goal(goal_id, principal.tenant_id)
    if goal is None:
        raise HTTPException(status_code=404, detail="Goal not found")
    if goal["status"] in TERMINAL_STATUSES:
        raise HTTPException(status_code=409, detail="Completed or failed goals cannot be paused")
    goal["status"] = "pausing" if goal["status"] == "running" else "paused"
    goal["updated_at"] = _now()
    await runtime.save_goal(goal_id, goal, principal.tenant_id)
    await runtime.append_goal_event(
        goal_id,
        {"type": "goal_status", "status": goal["status"]},
        principal.tenant_id,
    )
    await runtime.append_audit(
        principal.tenant_id,
        {
            "action": "goal.pause",
            "status": goal["status"],
            "subject": principal.subject,
            "goal_id": goal_id,
        },
    )
    return goal


@router.post("/{goal_id}/resume")
async def resume_goal(goal_id: str, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "operator")
    runtime = _require_redis(request)
    goal = await runtime.get_goal(goal_id, principal.tenant_id)
    if goal is None:
        raise HTTPException(status_code=404, detail="Goal not found")
    if goal["status"] == "pausing":
        raise HTTPException(status_code=409, detail="Goal is still reaching a pause boundary")
    if goal["status"] != "paused":
        raise HTTPException(status_code=409, detail="Only paused goals can be resumed")
    goal["status"] = "queued"
    goal["updated_at"] = _now()
    await runtime.save_goal(goal_id, goal, principal.tenant_id)
    await runtime.append_goal_event(
        goal_id, {"type": "goal_status", "status": "queued"}, principal.tenant_id
    )
    await runtime.enqueue_goal(goal_id, principal.tenant_id)
    await runtime.append_audit(
        principal.tenant_id,
        {
            "action": "goal.resume",
            "status": "queued",
            "subject": principal.subject,
            "goal_id": goal_id,
        },
    )
    return goal


@router.get("/{goal_id}/events")
async def stream_goal_events(goal_id: str, request: Request) -> StreamingResponse:
    principal = await authorize(request, "viewer")
    runtime = _require_redis(request)
    if await runtime.get_goal(goal_id, principal.tenant_id) is None:
        raise HTTPException(status_code=404, detail="Goal not found")
    last_id = request.headers.get("last-event-id", "0-0")

    async def event_stream():
        nonlocal last_id
        while not await request.is_disconnected():
            events = await runtime.read_goal_events(
                goal_id, last_id, block_ms=10_000, tenant_id=principal.tenant_id
            )
            if not events:
                goal = await runtime.get_goal(goal_id, principal.tenant_id)
                yield ": heartbeat\n\n"
                if goal and goal.get("status") in TERMINAL_STATUSES:
                    break
                continue
            for event_id, event in events:
                last_id = event_id
                yield _sse(str(event.get("type", "message")), event, event_id)
            await asyncio.sleep(0)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
        },
    )
