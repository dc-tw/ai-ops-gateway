import asyncio
import json
import re
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.api.routes import _sse
from app.api.schemas import GoalCreateRequest
from app.core.goal_execution import GoalExecutionStore
from app.core.observability import observe_agent_event
from app.core.resource_access import filter_resources, stream_access
from app.core.security import authorize, redact_pii
from app.core.threat_detection import reject_incomplete_scan
from app.core.threat_feedback import record_threat_findings

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
                redact_pii(step.query) if request.app.state.settings.pii_redaction else step.query
            ),
            "target_url": str(step.target_url) if step.target_url else None,
            "status": "pending",
            "answer": "",
            "error": "",
        }
        for step in payload.steps
    ]
    for content in [objective, *(step["query"] for step in steps)]:
        if any(
            re.search(pattern, content, re.IGNORECASE)
            for pattern in request.app.state.settings.content_blocked_patterns
        ):
            raise HTTPException(status_code=403, detail="Request blocked by content policy")
        if max(1, len(content.encode("utf-8"))) > request.app.state.settings.request_token_budget:
            raise HTTPException(status_code=413, detail="Request exceeds token budget")
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
    threat_scans = [scan for scan in threat_scans if scan.needs_record]
    model_name = payload.model or request.app.state.model_router.default_model
    for scan in threat_scans:
        observe_agent_event(scan.to_event(), model=model_name)
        await record_threat_findings(
            runtime,
            request.app.state.settings,
            principal.tenant_id,
            scan.to_event(),
            context_id=goal_id,
            model=model_name,
        )
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
        reject_incomplete_scan(blocked_scan)
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
        "conversation_id": payload.conversation_id,
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
        "created_by_type": principal.actor_type,
        "history_search_approved": True,
    }
    atomic_store = hasattr(runtime, "_client") or hasattr(runtime, "goal_execution_mutate")
    if atomic_store:
        await GoalExecutionStore(runtime).submit_manual(goal)
    else:
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
    if not atomic_store:
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
    if hasattr(runtime, "_client") or hasattr(runtime, "goal_execution_mutate"):
        return await _control_goal(runtime, principal, goal_id, "cancel")
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
    return {"goals": await filter_resources(request, principal, "goal", goals, "goal_id")}


@router.get("/{goal_id}/receipts")
async def goal_receipts(
    goal_id: str,
    request: Request,
    after: str = Query(default="", max_length=512),
    limit: int = Query(default=50, ge=1, le=100),
):
    """Read bounded side-effect facts; never exposes arguments or grants replay."""
    principal = await authorize(request, "viewer")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,127}", goal_id):
        raise HTTPException(status_code=404, detail="Goal not found")
    runtime = _require_redis(request)
    if await runtime.get_goal(goal_id, principal.tenant_id) is None:
        raise HTTPException(status_code=404, detail="Goal not found")
    prefix = runtime._tenant_key(principal.tenant_id, f"goal:{goal_id}:tool-receipt:")
    index = runtime._tenant_key(principal.tenant_id, f"goal:{goal_id}:tool-receipts")
    keys = await runtime._client.smembers(index)
    if not keys:
        # Bounded compatibility read for pre-index receipts, never a backfill.
        async for key in runtime._client.scan_iter(prefix + "*"):
            keys.add(key)
            if len(keys) > 256:
                raise HTTPException(
                    status_code=413, detail="Legacy receipts exceed bounded listing"
                )
    if len(keys) > 256 or any(not key.startswith(prefix) for key in keys):
        raise HTTPException(status_code=409, detail="Receipt index failed integrity verification")
    candidates = sorted(key for key in keys if key[len(prefix) :] > after)
    selected = candidates[:limit]
    items = []
    for key, raw in zip(
        selected, await runtime._client.mget(selected) if selected else [], strict=True
    ):
        try:
            value = json.loads(raw or "null")
            if (
                not isinstance(value, dict)
                or value.get("status") not in {"started", "completed"}
                or not isinstance(value.get("tool"), str)
                or len(value["tool"]) > 128
            ):
                raise ValueError
            items.append(
                {
                    "action_id": key[len(prefix) :],
                    "tool": value.get("tool"),
                    "status": value.get("status"),
                    "review_required": value.get("status") != "completed",
                }
            )
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=409, detail="Receipt failed integrity verification"
            ) from None
    return {
        "goal_id": goal_id,
        "receipts": items,
        "automatic_replay": False,
        "next_after": selected[-1][len(prefix) :] if len(candidates) > limit else None,
    }


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
    if hasattr(runtime, "_client") or hasattr(runtime, "goal_execution_mutate"):
        return await _control_goal(runtime, principal, goal_id, "pause")
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
    if hasattr(runtime, "_client") or hasattr(runtime, "goal_execution_mutate"):
        return await _control_goal(runtime, principal, goal_id, "resume")
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


async def _control_goal(runtime: Any, principal: Any, goal_id: str, action: str) -> dict[str, Any]:
    store = GoalExecutionStore(runtime)
    try:
        goal = await store.control(principal.tenant_id, goal_id, action, principal.subject)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if goal is None:
        raise HTTPException(status_code=404, detail="Goal not found")
    if goal["status"] == "cancelled":
        await store.reconcile_budget(goal)
        from app.core.delegation import DelegationStore

        await DelegationStore(runtime).cancel_children(
            principal.tenant_id, goal_id, principal.subject
        )
    return goal


class GoalApprovalDecision(BaseModel):
    approval_id: str = Field(min_length=1, max_length=128)
    approved: bool
    reason: str = Field(default="", max_length=500)


@router.post("/{goal_id}/approval")
async def decide_goal_approval(
    goal_id: str, payload: GoalApprovalDecision, request: Request
) -> dict[str, Any]:
    principal = await authorize(request, "operator")
    if principal.actor_type != "human":
        raise HTTPException(status_code=403, detail="Human identity required for approval")
    runtime = _require_redis(request)
    store = GoalExecutionStore(runtime)
    try:
        goal = await store.decide_approval(
            principal.tenant_id,
            goal_id,
            payload.approval_id,
            payload.approved,
            payload.reason,
            principal.subject,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if goal is None:
        raise HTTPException(status_code=404, detail="Goal not found")
    if goal["status"] == "cancelled":
        await store.reconcile_budget(goal)
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
                if not await stream_access(request, principal, "goal", goal_id):
                    return
                goal = await runtime.get_goal(goal_id, principal.tenant_id)
                yield ": heartbeat\n\n"
                if goal and goal.get("status") in TERMINAL_STATUSES:
                    break
                continue
            for event_id, event in events:
                if not await stream_access(request, principal, "goal", goal_id):
                    return
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
