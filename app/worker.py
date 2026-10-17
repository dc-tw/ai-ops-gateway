import asyncio
import json
import re
import socket
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from app.core.config import get_settings
from app.core.observability import observe_agent_event
from app.core.redis_runtime import RedisRuntime
from app.core.threat_detection import ThreatBlockedError, ThreatDetector
from app.core.threat_feedback import record_threat_findings
from app.graph.workflow import build_agent_graph, initial_state
from app.llm.client import ModelRouter
from app.tools.browser import BrowserScraper
from app.tools.registry import RemoteBrowserScraper, RemoteBrowserToolExecutor, ToolExecutor


def _now() -> str:
    return datetime.now(UTC).isoformat()


async def _plan_steps(goal: dict[str, Any], router: ModelRouter) -> list[dict[str, Any]]:
    model = router.get(goal["model"])
    response = await model.complete(
        "You decompose goals into safe, finite work items. Return JSON only.",
        "Return a JSON array of 1-10 short task strings for this objective. "
        f"Do not include commentary. Objective: {goal['objective']}",
    )
    match = re.search(r"\[[\s\S]*\]", response)
    try:
        tasks = json.loads(match.group(0) if match else response)
        if not isinstance(tasks, list):
            raise ValueError("planner output is not a list")
        tasks = [str(task).strip() for task in tasks if str(task).strip()][:10]
    except (ValueError, json.JSONDecodeError):
        tasks = [goal["objective"]]
    return [
        {
            "id": str(uuid4()),
            "query": task,
            "target_url": None,
            "status": "pending",
            "answer": "",
            "error": "",
        }
        for task in tasks
    ]


async def execute_goal(
    goal_id: str,
    runtime: RedisRuntime,
    router: ModelRouter,
    scraper: BrowserScraper,
    *,
    conversation_max_turns: int,
    conversation_ttl_seconds: int,
    context_max_tokens: int = 6_000,
    checkpointer: object | None = None,
    tenant_id: str = "default",
    tool_executor: ToolExecutor | None = None,
    threat_detector: ThreatDetector | None = None,
) -> None:
    goal = await runtime.get_goal(goal_id, tenant_id)
    if not goal or goal.get("status") != "queued":
        return
    goal["status"] = "running"
    goal["updated_at"] = _now()
    await runtime.save_goal(goal_id, goal, tenant_id)
    await runtime.append_goal_event(
        goal_id, {"type": "goal_status", "status": "running"}, tenant_id
    )

    try:
        if not goal["steps"]:
            goal["steps"] = await _plan_steps(goal, router)
            if threat_detector:
                for index, step in enumerate(goal["steps"]):
                    scan = await threat_detector.ascan(
                        step["query"],
                        event_type="llm_output",
                        location=f"goal_planner_step:{index}",
                        fields={"agent_output": step["query"]},
                    )
                    if scan.findings:
                        await record_threat_findings(
                            runtime,
                            get_settings(),
                            tenant_id,
                            scan.to_event(),
                            context_id=goal_id,
                            model=goal["model"],
                        )
                        await runtime.append_goal_event(
                            goal_id, scan.to_event(), tenant_id
                        )
                    if scan.blocked:
                        raise ThreatBlockedError(scan)
            await runtime.save_goal(goal_id, goal, tenant_id)
            await runtime.append_goal_event(
                goal_id,
                {"type": "goal_planned", "steps": goal["steps"]},
                tenant_id,
            )

        for index in range(goal["current_step"], len(goal["steps"])):
            latest = await runtime.get_goal(goal_id, tenant_id)
            if latest and latest.get("status") == "cancelled":
                return
            if latest and latest.get("status") in {"pausing", "paused"}:
                goal = latest
                goal["status"] = "paused"
                goal["updated_at"] = _now()
                await runtime.save_goal(goal_id, goal, tenant_id)
                await runtime.append_goal_event(
                    goal_id,
                    {"type": "goal_status", "status": "paused", "current_step": index},
                    tenant_id,
                )
                return

            step = goal["steps"][index]
            resume_checkpoint = checkpointer is not None and step.get("status") == "running"
            step["status"] = "running"
            goal["current_step"] = index
            goal["updated_at"] = _now()
            await runtime.save_goal(goal_id, goal, tenant_id)
            await runtime.append_goal_event(
                goal_id,
                {"type": "goal_step", "status": "running", "index": index, "step": step},
                tenant_id,
            )

            request_id = str(uuid4())
            history = await runtime.get_conversation(
                goal_id, conversation_max_turns * 2, tenant_id=tenant_id
            )
            graph = build_agent_graph(
                router.get(goal["model"]),
                scraper,
                checkpointer=checkpointer,
                tool_executor=tool_executor,
                threat_detector=threat_detector,
            )
            completed: dict[str, Any] | None = None
            graph_input = None if resume_checkpoint else initial_state(
                request_id,
                step["query"],
                step.get("target_url"),
                goal["max_retries"],
                conversation_id=goal_id,
                model=goal["model"],
                conversation_history=history,
                context_max_tokens=context_max_tokens,
                max_tool_rounds=int(goal.get("max_tool_rounds", 8)),
            )
            async with asyncio.timeout(float(goal.get("step_deadline_seconds", 300))):
                async for event in graph.astream(
                    graph_input,
                    config={"configurable": {"thread_id": f"{goal_id}:{step['id']}"}},
                    stream_mode="custom",
                ):
                    observe_agent_event(event, model=goal["model"])
                    if event.get("type") == "threat_detection":
                        await record_threat_findings(
                            runtime,
                            get_settings(),
                            tenant_id,
                            event,
                            context_id=goal_id,
                            model=goal["model"],
                        )
                    await runtime.append_goal_event(
                        goal_id,
                        {"type": "agent_event", "step_index": index, "event": event},
                        tenant_id,
                    )
                    if event.get("type") == "completed":
                        completed = event
                    latest = None
                    if event.get("type") not in {"token", "draft_token"}:
                        latest = await runtime.get_goal(goal_id, tenant_id)
                    if latest and latest.get("status") == "cancelled":
                        return
                    if latest and latest.get("status") == "pausing":
                        step["status"] = "pending"
                        goal = latest
                        goal["steps"][index] = step
                        goal["status"] = "paused"
                        goal["current_step"] = index
                        goal["updated_at"] = _now()
                        await runtime.save_goal(goal_id, goal, tenant_id)
                        await runtime.append_goal_event(
                            goal_id,
                            {
                                "type": "goal_status",
                                "status": "paused",
                                "current_step": index,
                            },
                            tenant_id,
                        )
                        return

            if completed is None:
                raise RuntimeError("Agent run ended without a completed event")
            if not completed.get("evidence_validated"):
                raise RuntimeError(
                    "Agent step exhausted retries without an evidence-validated result"
                )
            step["status"] = "completed"
            step["answer"] = str(completed.get("answer", ""))
            goal["steps"][index] = step
            goal["current_step"] = index + 1
            goal["updated_at"] = _now()
            await runtime.append_conversation_turn(
                goal_id,
                query=step["query"],
                answer=step["answer"],
                model=goal["model"],
                request_id=request_id,
                max_turns=conversation_max_turns,
                ttl_seconds=conversation_ttl_seconds,
                tenant_id=tenant_id,
            )
            await runtime.save_goal(goal_id, goal, tenant_id)
            await runtime.append_goal_event(
                goal_id,
                {"type": "goal_step", "status": "completed", "index": index, "step": step},
                tenant_id,
            )

        goal["status"] = "completed"
        goal["updated_at"] = _now()
        await runtime.save_goal(goal_id, goal, tenant_id)
        await runtime.append_goal_event(
            goal_id, {"type": "goal_completed", "goal": goal}, tenant_id
        )
    except Exception as exc:
        current_step = int(goal.get("current_step", 0))
        if current_step < len(goal.get("steps", [])):
            goal["steps"][current_step]["status"] = "failed"
            goal["steps"][current_step]["error"] = f"{type(exc).__name__}: {exc}"
        goal["status"] = "failed"
        goal["error"] = f"{type(exc).__name__}: {exc}"
        goal["updated_at"] = _now()
        await runtime.save_goal(goal_id, goal, tenant_id)
        await runtime.append_goal_event(
            goal_id,
            {"type": "goal_failed", "error": goal["error"]},
            tenant_id,
        )


async def recover_goals(runtime: RedisRuntime) -> None:
    for tenant_id in await runtime.list_goal_tenants():
        for goal_id in await runtime.list_goal_ids(tenant_id):
            goal = await runtime.get_goal(goal_id, tenant_id)
            if not goal:
                continue
            if goal.get("status") == "running":
                goal["status"] = "queued"
                goal["updated_at"] = _now()
                await runtime.save_goal(goal_id, goal, tenant_id)
                await runtime.enqueue_goal(goal_id, tenant_id)
            elif goal.get("status") == "pausing":
                goal["status"] = "paused"
                goal["updated_at"] = _now()
                await runtime.save_goal(goal_id, goal, tenant_id)


async def main() -> None:
    settings = get_settings()
    runtime = RedisRuntime(settings.redis_url, settings.redis_stream_maxlen)
    router = ModelRouter(settings)
    scraper = BrowserScraper(settings)
    threat_detector = ThreatDetector.from_settings(settings)
    tool_executor = None
    if settings.browser_worker_url:
        tool_executor = RemoteBrowserToolExecutor(
            settings.browser_worker_url,
            settings.browser_worker_token,
            timeout_seconds=max(60.0, settings.playwright_timeout_ms / 1_000 + 10),
        )
        scraper = RemoteBrowserScraper(tool_executor)
    consumer = f"{socket.gethostname()}-{uuid4().hex[:8]}"
    checkpointer = None
    if settings.langgraph_checkpointing:
        from langgraph.checkpoint.redis.aio import AsyncRedisSaver

        checkpointer = AsyncRedisSaver(
            redis_url=settings.redis_url,
            ttl={"default_ttl": settings.checkpoint_ttl_minutes, "refresh_on_read": True},
        )
        await checkpointer.__aenter__()
    await recover_goals(runtime)
    try:
        while True:
            claimed = await runtime.claim_goal(
                group=settings.goal_consumer_group,
                consumer=consumer,
                block_seconds=max(1, int(settings.goal_worker_poll_seconds)),
                lease_ms=settings.goal_lease_ms,
            )
            if claimed:
                event_id, tenant_id, goal_id = claimed
                await execute_goal(
                    goal_id,
                    runtime,
                    router,
                    scraper,
                    conversation_max_turns=settings.conversation_max_turns,
                    conversation_ttl_seconds=settings.conversation_ttl_seconds,
                    context_max_tokens=settings.conversation_context_max_tokens,
                    checkpointer=checkpointer,
                    tenant_id=tenant_id,
                    tool_executor=tool_executor,
                    threat_detector=threat_detector,
                )
                finished = await runtime.get_goal(goal_id, tenant_id)
                error = (
                    str(finished.get("error"))
                    if finished and finished.get("status") == "failed"
                    else None
                )
                await runtime.acknowledge_goal(
                    event_id,
                    group=settings.goal_consumer_group,
                    goal_id=goal_id,
                    tenant_id=tenant_id,
                    error=error,
                )
    finally:
        if checkpointer:
            await checkpointer.__aexit__(None, None, None)
        await runtime.close()


if __name__ == "__main__":
    asyncio.run(main())
