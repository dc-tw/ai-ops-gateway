import asyncio
import contextlib
import json
import re
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from langgraph.types import Command

from app.api.schemas import AgentRequest, ApprovalDecision
from app.core.observability import metrics, observe_agent_event
from app.core.safety_journal import graph_journal
from app.core.security import authorize, redact_pii
from app.core.threat_detection import reject_incomplete_scan
from app.core.threat_feedback import record_threat_findings
from app.graph.workflow import build_agent_graph, initial_state

router = APIRouter()
_DONE = object()


def _sse(event: str, data: dict[str, Any], event_id: str | int | None = None) -> str:
    lines = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    lines.extend(f"data: {line}" for line in payload.splitlines() or [""])
    return "\n".join(lines) + "\n\n"


@router.post("/v1/agent/stream")
async def stream_agent(payload: AgentRequest, request: Request) -> StreamingResponse:
    principal = await authorize(request, "operator")
    settings = request.app.state.settings
    safe_query = redact_pii(payload.query) if settings.pii_redaction else payload.query
    request_id = str(uuid4())
    model_name = payload.model or request.app.state.model_router.default_model
    input_threat_scan = await request.app.state.threat_detector.ascan(
        safe_query,
        event_type="llm_input",
        location="agent_request",
        fields={"user_input": safe_query},
    )
    if input_threat_scan.needs_record:
        observe_agent_event(input_threat_scan.to_event(), model=model_name)
        await record_threat_findings(
            request.app.state.redis,
            settings,
            principal.tenant_id,
            input_threat_scan.to_event(),
            context_id=request_id,
            model=model_name,
        )
    if input_threat_scan.blocked:
        if request.app.state.redis:
            await request.app.state.redis.append_audit(
                principal.tenant_id,
                {
                    "action": "agent.threat_detection",
                    "status": "blocked",
                    "subject": principal.subject,
                    "request_id": request_id,
                    "event": input_threat_scan.to_event(),
                },
            )
        reject_incomplete_scan(input_threat_scan)
        raise HTTPException(
            status_code=403,
            detail={
                "error": "atr_threat_blocked",
                "request_id": request_id,
                "findings": list(input_threat_scan.findings),
            },
        )
    if any(
        re.search(pattern, safe_query, re.IGNORECASE)
        for pattern in settings.content_blocked_patterns
    ):
        raise HTTPException(status_code=403, detail="Request blocked by content policy")
    if max(1, len(safe_query) // 4) > settings.request_token_budget:
        raise HTTPException(status_code=413, detail="Request exceeds token budget")

    conversation_id = payload.conversation_id or str(uuid4())
    try:
        selected_model = request.app.state.model_router.get(model_name)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    graph = request.app.state.graph or build_agent_graph(
        selected_model,
        request.app.state.scraper,
        tool_executor=request.app.state.tool_executor,
        autonomous_tool_planning=settings.autonomous_tool_planning,
        checkpointer=request.app.state.checkpointer,
        threat_detector=request.app.state.threat_detector,
        safety_journal=graph_journal(
            request.app.state.redis,
            settings,
            principal.tenant_id,
            request_id,
            model=model_name,
            subject=principal.subject,
        ),
    )
    redis_runtime = request.app.state.redis
    max_retries = (
        payload.max_retries if payload.max_retries is not None else settings.agent_max_retries
    )
    history: list[dict[str, str]] = []
    reserved_tokens = max(1, len(safe_query) // 3) + settings.llm_max_output_tokens
    if redis_runtime and hasattr(redis_runtime, "get_conversation"):
        if hasattr(redis_runtime, "get_conversation_context"):
            history = await redis_runtime.get_conversation_context(
                conversation_id,
                query=safe_query,
                limit=settings.conversation_max_turns * 2,
                tenant_id=principal.tenant_id,
            )
        else:
            history = await redis_runtime.get_conversation(
                conversation_id,
                settings.conversation_max_turns * 2,
                tenant_id=principal.tenant_id,
            )
        try:
            await redis_runtime.enforce_rate_limit(
                principal.tenant_id, settings.tenant_requests_per_minute
            )
            await redis_runtime.consume_daily_tokens(
                principal.tenant_id,
                reserved_tokens,
                settings.tenant_daily_token_budget,
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        if not await redis_runtime.acquire_run_slot(
            principal.tenant_id,
            request_id,
            settings.tenant_max_concurrent_runs,
            ttl_seconds=max(60, int(settings.llm_timeout_seconds * 3)),
        ):
            raise HTTPException(status_code=429, detail="Tenant concurrent run limit exceeded")
    state = initial_state(
        request_id,
        safe_query,
        str(payload.target_url) if payload.target_url else None,
        max_retries,
        conversation_id=conversation_id,
        model=model_name,
        conversation_history=history,
        context_max_tokens=settings.conversation_context_max_tokens,
        approved_tools=payload.approved_tools,
        target_urls=[str(url) for url in payload.source_urls],
        max_tool_rounds=payload.max_tool_rounds or settings.agent_max_tool_rounds,
        interactive_approval=request.app.state.checkpointer is not None,
    )
    queue: asyncio.Queue[tuple[str | None, dict[str, Any]] | object] = asyncio.Queue(maxsize=128)
    consumer_attached = asyncio.Event()
    consumer_attached.set()
    run_metadata = {
        "conversation_id": conversation_id,
        "model": model_name,
        "query": safe_query,
        "target_url": str(payload.target_url) if payload.target_url else None,
        "max_retries": max_retries,
        "max_tool_rounds": payload.max_tool_rounds or settings.agent_max_tool_rounds,
        "deadline_seconds": payload.deadline_seconds or settings.agent_default_deadline_seconds,
        "approved_tools": payload.approved_tools,
        "reserved_tokens": reserved_tokens,
    }

    async def publish(event: dict[str, Any]) -> None:
        event_id = (
            await redis_runtime.append_event(request_id, event, principal.tenant_id)
            if redis_runtime
            else None
        )
        if consumer_attached.is_set():
            await queue.put((event_id, event))

    async def produce() -> None:
        started_at = asyncio.get_running_loop().time()
        first_token_seen = False
        waiting_for_approval = False
        latest_usage: dict[str, Any] = {}
        deadline_seconds = float(run_metadata["deadline_seconds"])
        metrics.gauge_add("ai_gateway_active_runs", 1, tenant=principal.tenant_id)
        try:
            if redis_runtime:
                await redis_runtime.save_run_state(
                    request_id,
                    {**run_metadata, "status": "running"},
                    principal.tenant_id,
                )
                await redis_runtime.append_audit(
                    principal.tenant_id,
                    {
                        "action": "agent.run",
                        "status": "started",
                        "subject": principal.subject,
                        "request_id": request_id,
                        "model": model_name,
                    },
                )
            await publish(
                {
                    "type": "accepted",
                    "request_id": request_id,
                    "conversation_id": conversation_id,
                    "model": model_name,
                    "history_messages": len(history),
                }
            )
            if input_threat_scan.needs_record:
                await publish(input_threat_scan.to_event())
                if redis_runtime:
                    await redis_runtime.append_audit(
                        principal.tenant_id,
                        {
                            "action": "agent.threat_detection",
                            "status": "detected",
                            "subject": principal.subject,
                            "request_id": request_id,
                            "event": input_threat_scan.to_event(),
                        },
                    )
            async with asyncio.timeout(deadline_seconds):
                async for event in graph.astream(
                    state,
                    config={"configurable": {"thread_id": request_id}},
                    stream_mode="custom",
                ):
                    observe_agent_event(event, model=model_name)
                    if event.get("type") in {"draft_token", "token"} and not first_token_seen:
                        first_token_seen = True
                        metrics.inc(
                            "ai_gateway_ttft_seconds_sum",
                            asyncio.get_running_loop().time() - started_at,
                            model=model_name,
                        )
                        metrics.inc("ai_gateway_ttft_observations_total", model=model_name)
                    await publish(event)
                    if (
                        event.get("type") == "threat_detection"
                        and redis_runtime
                        and not event.get("safety_event_id")
                    ):
                        await record_threat_findings(
                            redis_runtime,
                            settings,
                            principal.tenant_id,
                            event,
                            context_id=request_id,
                            model=model_name,
                        )
                        await redis_runtime.append_audit(
                            principal.tenant_id,
                            {
                                "action": "agent.threat_detection",
                                "status": event.get("status", "detected"),
                                "subject": principal.subject,
                                "request_id": request_id,
                                "event": event,
                            },
                        )
                    if event.get("type") == "model_usage" and isinstance(event.get("usage"), dict):
                        latest_usage = dict(event["usage"])
                        if redis_runtime:
                            await redis_runtime.save_run_state(
                                request_id,
                                {**run_metadata, "status": "running", "usage": latest_usage},
                                principal.tenant_id,
                            )
                    if event.get("type") == "approval_required":
                        waiting_for_approval = True
                        if redis_runtime:
                            await redis_runtime.save_run_state(
                                request_id,
                                {
                                    **run_metadata,
                                    "status": "waiting_approval",
                                    "pending_approval": event,
                                    "usage": latest_usage,
                                },
                                principal.tenant_id,
                            )
                    if (
                        event.get("type") == "completed"
                        and redis_runtime
                        and hasattr(redis_runtime, "append_conversation_turn")
                    ):
                        waiting_for_approval = False
                        usage = event.get("usage", {})
                        if (
                            isinstance(usage, dict)
                            and usage.get("provider_reported")
                            and hasattr(redis_runtime, "reconcile_daily_tokens")
                        ):
                            await redis_runtime.reconcile_daily_tokens(
                                principal.tenant_id,
                                reserved_tokens,
                                int(usage.get("total_tokens", reserved_tokens)),
                                request_id=request_id,
                            )
                        await redis_runtime.append_conversation_turn(
                            conversation_id,
                            query=safe_query,
                            answer=str(event.get("answer", "")),
                            model=model_name,
                            request_id=request_id,
                            max_turns=settings.conversation_max_turns,
                            ttl_seconds=settings.conversation_ttl_seconds,
                            tenant_id=principal.tenant_id,
                        )
                        await redis_runtime.save_run_state(
                            request_id,
                            {
                                **run_metadata,
                                "status": "completed",
                                "usage": event.get("usage", {}),
                            },
                            principal.tenant_id,
                        )
                        await redis_runtime.append_audit(
                            principal.tenant_id,
                            {
                                "action": "agent.run",
                                "status": "completed",
                                "subject": principal.subject,
                                "request_id": request_id,
                                "evidence_validated": event.get("evidence_validated"),
                                "usage": event.get("usage", {}),
                            },
                        )
            if waiting_for_approval and redis_runtime:
                await redis_runtime.release_run_slot(principal.tenant_id, request_id)
        except TimeoutError:
            await publish(
                {
                    "type": "error",
                    "request_id": request_id,
                    "error": "deadline_exceeded",
                    "message": f"Run exceeded {deadline_seconds:g} seconds",
                }
            )
            if redis_runtime:
                if latest_usage.get("provider_reported") and hasattr(
                    redis_runtime, "reconcile_daily_tokens"
                ):
                    await redis_runtime.reconcile_daily_tokens(
                        principal.tenant_id,
                        reserved_tokens,
                        int(latest_usage.get("total_tokens", 0)),
                        request_id=request_id,
                    )
                await redis_runtime.save_run_state(
                    request_id,
                    {**run_metadata, "status": "timed_out", "usage": latest_usage},
                    principal.tenant_id,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await publish(
                {
                    "type": "error",
                    "request_id": request_id,
                    "error": type(exc).__name__,
                    "message": str(exc),
                }
            )
            if redis_runtime:
                if latest_usage.get("provider_reported") and hasattr(
                    redis_runtime, "reconcile_daily_tokens"
                ):
                    await redis_runtime.reconcile_daily_tokens(
                        principal.tenant_id,
                        reserved_tokens,
                        int(latest_usage.get("total_tokens", 0)),
                        request_id=request_id,
                    )
                await redis_runtime.save_run_state(
                    request_id,
                    {**run_metadata, "status": "failed", "usage": latest_usage},
                    principal.tenant_id,
                )
                await redis_runtime.append_audit(
                    principal.tenant_id,
                    {
                        "action": "agent.run",
                        "status": "failed",
                        "subject": principal.subject,
                        "request_id": request_id,
                        "error": type(exc).__name__,
                    },
                )
        finally:
            metrics.gauge_add("ai_gateway_active_runs", -1, tenant=principal.tenant_id)
            if redis_runtime:
                await redis_runtime.release_run_slot(principal.tenant_id, request_id)
            if consumer_attached.is_set():
                await queue.put(_DONE)

    async def event_stream() -> AsyncIterator[str]:
        sequence = 0
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    item = await asyncio.wait_for(
                        queue.get(), timeout=settings.sse_heartbeat_seconds
                    )
                except TimeoutError:
                    yield ": heartbeat\n\n"
                    continue
                if item is _DONE:
                    break
                assert isinstance(item, tuple)
                persisted_id, event = item
                sequence += 1
                yield _sse(str(event.get("type", "message")), event, persisted_id or sequence)
        finally:
            consumer_attached.clear()
            if not redis_runtime:
                producer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await producer

    request.app.state.active_run_ids.add(request_id)
    request.app.state.active_runs.add(producer := asyncio.create_task(produce()))
    request.app.state.active_run_tasks[request_id] = producer

    def discard_run(task: asyncio.Task[None]) -> None:
        request.app.state.active_runs.discard(task)
        request.app.state.active_run_ids.discard(request_id)
        request.app.state.active_run_tasks.pop(request_id, None)

    producer.add_done_callback(discard_run)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "X-Request-ID": request_id,
        },
    )


@router.post("/v1/agent/runs/{request_id}/resume")
async def resume_agent_run(request_id: str, request: Request) -> dict[str, object]:
    principal = await authorize(request, "operator")
    runtime = request.app.state.redis
    if runtime is None or request.app.state.checkpointer is None:
        raise HTTPException(status_code=503, detail="Resume requires Redis checkpointing")
    run = await runtime.get_run_state(request_id, principal.tenant_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Agent run not found")
    if run.get("status") in {"completed", "cancelled", "timed_out", "rejected"}:
        raise HTTPException(status_code=409, detail="Terminal runs cannot be resumed")
    if run.get("status") == "waiting_approval":
        raise HTTPException(status_code=409, detail="Use the approval endpoint for this run")
    if request_id in request.app.state.active_run_ids:
        raise HTTPException(status_code=409, detail="Agent run is already active")

    model_name = str(run["model"])
    try:
        model = request.app.state.model_router.get(model_name)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    graph = build_agent_graph(
        model,
        request.app.state.scraper,
        tool_executor=request.app.state.tool_executor,
        autonomous_tool_planning=request.app.state.settings.autonomous_tool_planning,
        checkpointer=request.app.state.checkpointer,
        threat_detector=request.app.state.threat_detector,
        safety_journal=graph_journal(
            runtime,
            request.app.state.settings,
            principal.tenant_id,
            request_id,
            model=model_name,
            subject=principal.subject,
        ),
    )
    if not await runtime.acquire_run_slot(
        principal.tenant_id,
        request_id,
        request.app.state.settings.tenant_max_concurrent_runs,
        ttl_seconds=max(60, int(float(run.get("deadline_seconds", 300)))),
    ):
        raise HTTPException(status_code=429, detail="Tenant concurrent run limit exceeded")

    async def resume() -> None:
        waiting = False
        latest_usage = dict(run.get("usage", {}))
        await runtime.save_run_state(request_id, {**run, "status": "running"}, principal.tenant_id)
        await runtime.append_audit(
            principal.tenant_id,
            {
                "action": "agent.run.resume",
                "status": "started",
                "subject": principal.subject,
                "request_id": request_id,
            },
        )
        try:
            async with asyncio.timeout(float(run.get("deadline_seconds", 300))):
                async for event in graph.astream(
                    None,
                    config={"configurable": {"thread_id": request_id}},
                    stream_mode="custom",
                ):
                    observe_agent_event(event, model=model_name)
                    await runtime.append_event(request_id, event, principal.tenant_id)
                    if event.get("type") == "threat_detection" and not event.get("safety_event_id"):
                        await record_threat_findings(
                            runtime,
                            request.app.state.settings,
                            principal.tenant_id,
                            event,
                            context_id=request_id,
                            model=model_name,
                        )
                        await runtime.append_audit(
                            principal.tenant_id,
                            {
                                "action": "agent.threat_detection",
                                "status": event.get("status", "detected"),
                                "subject": principal.subject,
                                "request_id": request_id,
                                "event": event,
                            },
                        )
                    if event.get("type") == "model_usage" and isinstance(event.get("usage"), dict):
                        latest_usage = dict(event["usage"])
                        await runtime.save_run_state(
                            request_id,
                            {**run, "status": "running", "usage": latest_usage},
                            principal.tenant_id,
                        )
                    if event.get("type") == "approval_required":
                        waiting = True
                        await runtime.save_run_state(
                            request_id,
                            {
                                **run,
                                "status": "waiting_approval",
                                "pending_approval": event,
                                "usage": latest_usage,
                            },
                            principal.tenant_id,
                        )
                    if event.get("type") == "completed":
                        waiting = False
                        usage = event.get("usage", {})
                        if isinstance(usage, dict) and usage.get("provider_reported"):
                            await runtime.reconcile_daily_tokens(
                                principal.tenant_id,
                                int(run.get("reserved_tokens", 0)),
                                int(usage.get("total_tokens", 0)),
                                request_id=request_id,
                            )
                        await runtime.append_conversation_turn(
                            str(run["conversation_id"]),
                            query=str(run["query"]),
                            answer=str(event.get("answer", "")),
                            model=model_name,
                            request_id=request_id,
                            max_turns=request.app.state.settings.conversation_max_turns,
                            ttl_seconds=request.app.state.settings.conversation_ttl_seconds,
                            tenant_id=principal.tenant_id,
                        )
                        await runtime.save_run_state(
                            request_id,
                            {**run, "status": "completed", "usage": usage},
                            principal.tenant_id,
                        )
        except TimeoutError:
            if latest_usage.get("provider_reported"):
                await runtime.reconcile_daily_tokens(
                    principal.tenant_id,
                    int(run.get("reserved_tokens", 0)),
                    int(latest_usage.get("total_tokens", 0)),
                    request_id=request_id,
                )
            await runtime.append_event(
                request_id,
                {"type": "error", "error": "deadline_exceeded"},
                principal.tenant_id,
            )
            await runtime.save_run_state(
                request_id,
                {**run, "status": "timed_out", "usage": latest_usage},
                principal.tenant_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if latest_usage.get("provider_reported"):
                await runtime.reconcile_daily_tokens(
                    principal.tenant_id,
                    int(run.get("reserved_tokens", 0)),
                    int(latest_usage.get("total_tokens", 0)),
                    request_id=request_id,
                )
            await runtime.append_event(
                request_id,
                {
                    "type": "error",
                    "request_id": request_id,
                    "error": type(exc).__name__,
                    "message": str(exc),
                },
                principal.tenant_id,
            )
            await runtime.save_run_state(
                request_id,
                {**run, "status": "failed", "usage": latest_usage},
                principal.tenant_id,
            )
        finally:
            await runtime.release_run_slot(principal.tenant_id, request_id)
            if waiting:
                metrics.inc("ai_gateway_approval_waits_total", tenant=principal.tenant_id)

    request.app.state.active_run_ids.add(request_id)
    task = asyncio.create_task(resume())
    request.app.state.active_runs.add(task)
    request.app.state.active_run_tasks[request_id] = task

    def discard_resumed(completed_task: asyncio.Task[None]) -> None:
        request.app.state.active_runs.discard(completed_task)
        request.app.state.active_run_ids.discard(request_id)
        request.app.state.active_run_tasks.pop(request_id, None)

    task.add_done_callback(discard_resumed)
    return {
        "request_id": request_id,
        "status": "resuming",
        "events_url": f"/v1/agent/runs/{request_id}/events",
    }


@router.post("/v1/agent/runs/{request_id}/approval")
async def decide_agent_approval(
    request_id: str, payload: ApprovalDecision, request: Request
) -> dict[str, object]:
    principal = await authorize(request, "operator")
    runtime = request.app.state.redis
    if runtime is None or request.app.state.checkpointer is None:
        raise HTTPException(status_code=503, detail="Approval requires Redis checkpointing")
    run = await runtime.get_run_state(request_id, principal.tenant_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Agent run not found")
    if run.get("status") != "waiting_approval":
        raise HTTPException(status_code=409, detail="Run is not waiting for approval")
    if not payload.approved:
        usage = run.get("usage", {})
        if isinstance(usage, dict) and usage.get("provider_reported"):
            await runtime.reconcile_daily_tokens(
                principal.tenant_id,
                int(run.get("reserved_tokens", 0)),
                int(usage.get("total_tokens", 0)),
                request_id=request_id,
            )
        event = {
            "type": "approval_rejected",
            "request_id": request_id,
            "reason": payload.reason,
        }
        await runtime.append_event(request_id, event, principal.tenant_id)
        await runtime.save_run_state(request_id, {**run, "status": "rejected"}, principal.tenant_id)
        await runtime.append_audit(
            principal.tenant_id,
            {
                "action": "agent.run.approval",
                "status": "rejected",
                "subject": principal.subject,
                "request_id": request_id,
                "reason": payload.reason,
            },
        )
        return {"request_id": request_id, "status": "rejected"}
    if request_id in request.app.state.active_run_ids:
        raise HTTPException(status_code=409, detail="Agent run is already active")

    model_name = str(run["model"])
    try:
        model = request.app.state.model_router.get(model_name)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    graph = build_agent_graph(
        model,
        request.app.state.scraper,
        tool_executor=request.app.state.tool_executor,
        autonomous_tool_planning=request.app.state.settings.autonomous_tool_planning,
        checkpointer=request.app.state.checkpointer,
        threat_detector=request.app.state.threat_detector,
        safety_journal=graph_journal(
            runtime,
            request.app.state.settings,
            principal.tenant_id,
            request_id,
            model=model_name,
            subject=principal.subject,
        ),
    )
    if not await runtime.acquire_run_slot(
        principal.tenant_id,
        request_id,
        request.app.state.settings.tenant_max_concurrent_runs,
        ttl_seconds=max(60, int(request.app.state.settings.agent_default_deadline_seconds)),
    ):
        raise HTTPException(status_code=429, detail="Tenant concurrent run limit exceeded")

    async def continue_after_approval() -> None:
        waiting = False
        latest_usage = dict(run.get("usage", {}))
        await runtime.save_run_state(request_id, {**run, "status": "running"}, principal.tenant_id)
        await runtime.append_audit(
            principal.tenant_id,
            {
                "action": "agent.run.approval",
                "status": "approved",
                "subject": principal.subject,
                "request_id": request_id,
            },
        )
        try:
            async with asyncio.timeout(float(run.get("deadline_seconds", 300))):
                async for event in graph.astream(
                    Command(resume={"approved": True, "reason": payload.reason}),
                    config={"configurable": {"thread_id": request_id}},
                    stream_mode="custom",
                ):
                    observe_agent_event(event, model=model_name)
                    await runtime.append_event(request_id, event, principal.tenant_id)
                    if event.get("type") == "threat_detection" and not event.get("safety_event_id"):
                        await record_threat_findings(
                            runtime,
                            request.app.state.settings,
                            principal.tenant_id,
                            event,
                            context_id=request_id,
                            model=model_name,
                        )
                        await runtime.append_audit(
                            principal.tenant_id,
                            {
                                "action": "agent.threat_detection",
                                "status": event.get("status", "detected"),
                                "subject": principal.subject,
                                "request_id": request_id,
                                "event": event,
                            },
                        )
                    if event.get("type") == "model_usage" and isinstance(event.get("usage"), dict):
                        latest_usage = dict(event["usage"])
                        await runtime.save_run_state(
                            request_id,
                            {**run, "status": "running", "usage": latest_usage},
                            principal.tenant_id,
                        )
                    if event.get("type") == "approval_required":
                        waiting = True
                        await runtime.save_run_state(
                            request_id,
                            {
                                **run,
                                "status": "waiting_approval",
                                "pending_approval": event,
                                "usage": latest_usage,
                            },
                            principal.tenant_id,
                        )
                    if event.get("type") == "completed":
                        waiting = False
                        usage = event.get("usage", {})
                        if isinstance(usage, dict) and usage.get("provider_reported"):
                            await runtime.reconcile_daily_tokens(
                                principal.tenant_id,
                                int(run.get("reserved_tokens", 0)),
                                int(usage.get("total_tokens", 0)),
                                request_id=request_id,
                            )
                        await runtime.append_conversation_turn(
                            str(run["conversation_id"]),
                            query=str(run["query"]),
                            answer=str(event.get("answer", "")),
                            model=model_name,
                            request_id=request_id,
                            max_turns=request.app.state.settings.conversation_max_turns,
                            ttl_seconds=request.app.state.settings.conversation_ttl_seconds,
                            tenant_id=principal.tenant_id,
                        )
                        await runtime.save_run_state(
                            request_id,
                            {
                                **run,
                                "status": "completed",
                                "usage": event.get("usage", {}),
                            },
                            principal.tenant_id,
                        )
        except TimeoutError:
            if latest_usage.get("provider_reported"):
                await runtime.reconcile_daily_tokens(
                    principal.tenant_id,
                    int(run.get("reserved_tokens", 0)),
                    int(latest_usage.get("total_tokens", 0)),
                    request_id=request_id,
                )
            await runtime.append_event(
                request_id,
                {"type": "error", "error": "deadline_exceeded"},
                principal.tenant_id,
            )
            await runtime.save_run_state(
                request_id, {**run, "status": "timed_out"}, principal.tenant_id
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if latest_usage.get("provider_reported"):
                await runtime.reconcile_daily_tokens(
                    principal.tenant_id,
                    int(run.get("reserved_tokens", 0)),
                    int(latest_usage.get("total_tokens", 0)),
                    request_id=request_id,
                )
            await runtime.append_event(
                request_id,
                {"type": "error", "error": type(exc).__name__, "message": str(exc)},
                principal.tenant_id,
            )
            await runtime.save_run_state(
                request_id, {**run, "status": "failed"}, principal.tenant_id
            )
        finally:
            await runtime.release_run_slot(principal.tenant_id, request_id)
            if waiting:
                metrics.inc("ai_gateway_approval_waits_total", tenant=principal.tenant_id)

    request.app.state.active_run_ids.add(request_id)
    task = asyncio.create_task(continue_after_approval())
    request.app.state.active_runs.add(task)
    request.app.state.active_run_tasks[request_id] = task

    def discard_approved(completed_task: asyncio.Task[None]) -> None:
        request.app.state.active_runs.discard(completed_task)
        request.app.state.active_run_ids.discard(request_id)
        request.app.state.active_run_tasks.pop(request_id, None)

    task.add_done_callback(discard_approved)
    return {
        "request_id": request_id,
        "status": "resuming",
        "events_url": f"/v1/agent/runs/{request_id}/events",
    }


@router.post("/v1/agent/runs/{request_id}/cancel")
async def cancel_agent_run(request_id: str, request: Request) -> dict[str, object]:
    principal = await authorize(request, "operator")
    runtime = request.app.state.redis
    if runtime is None:
        raise HTTPException(status_code=503, detail="Cancellation requires Redis")
    run = await runtime.get_run_state(request_id, principal.tenant_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Agent run not found")
    if run.get("status") in {"completed", "failed", "cancelled", "timed_out", "rejected"}:
        raise HTTPException(status_code=409, detail="Run is already terminal")
    event = {"type": "cancelled", "request_id": request_id, "by": principal.subject}
    usage = run.get("usage", {})
    if (
        isinstance(usage, dict)
        and usage.get("provider_reported")
        and hasattr(runtime, "reconcile_daily_tokens")
    ):
        await runtime.reconcile_daily_tokens(
            principal.tenant_id,
            int(run.get("reserved_tokens", 0)),
            int(usage.get("total_tokens", 0)),
            request_id=request_id,
        )
    await runtime.append_event(request_id, event, principal.tenant_id)
    await runtime.save_run_state(request_id, {**run, "status": "cancelled"}, principal.tenant_id)
    await runtime.append_audit(
        principal.tenant_id,
        {
            "action": "agent.run.cancel",
            "status": "completed",
            "subject": principal.subject,
            "request_id": request_id,
        },
    )
    task = request.app.state.active_run_tasks.get(request_id)
    if task is not None:
        task.cancel()
    return {"request_id": request_id, "status": "cancelled"}


@router.get("/v1/agent/runs/{request_id}/events")
async def replay_agent_events(request_id: str, request: Request) -> StreamingResponse:
    principal = await authorize(request, "viewer")
    runtime = request.app.state.redis
    if runtime is None:
        raise HTTPException(status_code=503, detail="SSE replay requires Redis")
    if await runtime.get_run_state(request_id, principal.tenant_id) is None:
        raise HTTPException(status_code=404, detail="Agent run not found")
    last_id = request.headers.get("last-event-id", "0-0")

    async def replay_stream() -> AsyncIterator[str]:
        nonlocal last_id
        while not await request.is_disconnected():
            events = await runtime.read_agent_events(
                request_id, last_id, block_ms=10_000, tenant_id=principal.tenant_id
            )
            if not events:
                state = await runtime.get_run_state(request_id, principal.tenant_id)
                yield ": heartbeat\n\n"
                if state and state.get("status") in {
                    "completed",
                    "failed",
                    "cancelled",
                    "timed_out",
                    "rejected",
                }:
                    break
                continue
            for event_id, event in events:
                last_id = event_id
                yield _sse(str(event.get("type", "message")), event, event_id)

    return StreamingResponse(
        replay_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no"},
    )


@router.get("/healthz")
async def health(request: Request) -> dict[str, object]:
    redis_ok = None
    if request.app.state.redis:
        try:
            redis_ok = await request.app.state.redis.ping()
        except Exception:
            redis_ok = False
    detector = request.app.state.threat_detector
    return {
        "status": "ok",
        "redis": redis_ok,
        "atr": {
            "enabled": detector.enabled,
            "blocking": detector.blocking,
            "rules": detector.rule_count,
            "ruleset_fingerprint": detector.ruleset_fingerprint,
            "full_scan_required": detector.enabled,
        },
    }


@router.get("/readyz")
async def readiness(request: Request) -> JSONResponse:
    redis_ok = False
    if request.app.state.redis:
        try:
            redis_ok = await request.app.state.redis.ping()
        except Exception:
            redis_ok = False
    ready = bool(redis_ok) and (
        not request.app.state.settings.langgraph_checkpointing
        or request.app.state.checkpointer is not None
    )
    return JSONResponse(
        {
            "status": "ready" if ready else "not_ready",
            "redis": bool(redis_ok),
            "atr_rules": request.app.state.threat_detector.rule_count,
        },
        status_code=200 if ready else 503,
    )


@router.get("/v1/conversations/{conversation_id}")
async def get_conversation(conversation_id: str, request: Request) -> dict[str, object]:
    principal = await authorize(request, "viewer")
    if not request.app.state.redis:
        raise HTTPException(status_code=503, detail="Conversation memory requires Redis")
    messages = await request.app.state.redis.get_conversation(
        conversation_id,
        request.app.state.settings.conversation_max_turns * 2,
        tenant_id=principal.tenant_id,
    )
    return {"conversation_id": conversation_id, "messages": messages}


@router.get("/v1/models")
async def list_models(request: Request) -> dict[str, object]:
    await authorize(request, "viewer")
    return {"models": request.app.state.model_router.available_models()}


@router.delete("/v1/conversations/{conversation_id}")
async def delete_conversation(conversation_id: str, request: Request) -> dict[str, object]:
    principal = await authorize(request, "admin")
    if not request.app.state.redis:
        raise HTTPException(status_code=503, detail="Conversation memory requires Redis")
    deleted = await request.app.state.redis.delete_conversation(
        conversation_id, tenant_id=principal.tenant_id
    )
    await request.app.state.redis.append_audit(
        principal.tenant_id,
        {
            "action": "conversation.delete",
            "status": "completed",
            "subject": principal.subject,
            "conversation_id": conversation_id,
            "deleted": deleted,
        },
    )
    return {"conversation_id": conversation_id, "deleted": deleted}
