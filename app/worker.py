import asyncio
import json
import re
import socket
import time
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from langgraph.types import Command

from app.core.checkpoint_bootstrap import open_checkpointer
from app.core.config import get_settings
from app.core.goal_execution import GoalExecutionStore
from app.core.goal_protocol import QUEUES, is_workflow_id, require_goal_protocol
from app.core.observability import observe_agent_event
from app.core.redis_runtime import RedisRuntime
from app.core.safety_journal import graph_journal
from app.core.skill_store import guard_goal_skills, skill_text
from app.core.threat_detection import ThreatBlockedError, ThreatDetector
from app.core.threat_feedback import record_threat_findings
from app.graph.workflow import build_agent_graph, initial_state
from app.llm.client import ModelRouter
from app.tools.browser import BrowserScraper, BrowserSessionManager
from app.tools.registry import (
    LocalBrowserToolExecutor,
    MCPBrowserToolExecutor,
    RemoteBrowserScraper,
    RemoteBrowserToolExecutor,
    ToolExecutor,
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


async def _plan_steps(goal: dict[str, Any], model: Any) -> list[dict[str, Any]]:
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
        if not tasks:
            tasks = [goal["objective"]]
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


class GoalOwnershipLost(RuntimeError):
    """A worker whose lease was fenced must not commit further state."""


class _ScraperExecutor:
    def __init__(self, scraper):
        self.scraper = scraper

    def specs(self):
        from app.tools.registry import TOOL_SPECS

        return [TOOL_SPECS["scrape_dynamic_page"]]

    async def invoke(self, name, arguments):
        if name != "scrape_dynamic_page":
            raise PermissionError(f"No executor is configured for {name}")
        return await self.scraper.scrape(str(arguments["url"]))


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
    settings: Any = None,
) -> None:
    from app.core.execution_policy import BudgetedModel, PolicyToolExecutor

    settings = settings or get_settings()
    existing = await runtime.get_goal(goal_id, tenant_id)
    if existing and (
        is_workflow_id(goal_id) or existing.get("workflow") or existing.get("workflow_node")
    ):
        try:
            require_goal_protocol(existing)
        except ValueError:
            if existing.get("workflow"):
                from app.core.workflow_execution import WorkflowExecutionStore

                await WorkflowExecutionStore(runtime).quarantine(tenant_id, goal_id)
            elif existing.get("status") not in {"completed", "failed", "cancelled"}:
                await GoalExecutionStore(runtime).control(
                    tenant_id, goal_id, "cancel", "system:protocol"
                )
            return
    if existing and existing.get("workflow"):
        from app.core.workflow_execution import WorkflowExecutionStore

        await WorkflowExecutionStore(runtime).reconcile(tenant_id, goal_id, settings)
        return
    store = GoalExecutionStore(runtime)
    owner = uuid4().hex
    lease_ms = settings.goal_lease_ms
    goal = await store.claim(goal_id, tenant_id, owner, lease_ms)
    if goal is None:
        return
    goal["tenant_id"] = tenant_id
    policy = goal.get("policy", {})
    started = time.monotonic()
    prior_runtime = float(goal.get("runtime_seconds", 0))
    slot_id = f"goal:{goal_id}:{owner}"
    slot_acquired = False
    execution_task = asyncio.current_task()
    heartbeat = None
    ownership_revoked = False

    async def save(event=None):
        goal["runtime_seconds"] = prior_runtime + time.monotonic() - started
        if not await store.save_owned(goal, owner, event):
            raise GoalOwnershipLost("Goal ownership or authorization was revoked")

    async def on_usage(usage):
        await require_enabled_agent()
        await store.reserve_budget(
            goal,
            owner,
            settings.tenant_daily_token_budget,
            usage=usage,
            current_acl=settings.resource_acl_enabled,
        )
        goal["usage"] = dict(usage)
        await save()

    async def require_enabled_agent():
        if goal.get("workflow_node"):
            from app.core.planning_snapshot import _safe_route

            if _safe_route(router, goal["model"]) != goal.get("agent_config", {}).get(
                "model_route"
            ):
                raise PermissionError("Workflow model route differs from its accepted snapshot")
        if goal.get("agent_config"):
            from app.agents import AgentStore

            configured = await AgentStore(runtime).get(tenant_id, goal["agent_id"])
            if not configured or not configured["enabled"]:
                raise PermissionError("Agent is disabled")
        config = goal.get("agent_config") or {}
        if config.get("skills") or config.get("skill_snapshots"):
            await guard_goal_skills(runtime, goal)
        if goal.get("parent_goal_id"):
            parent = await runtime.get_goal(goal["parent_goal_id"], tenant_id)
            if not parent or parent["status"] in {"completed", "failed", "cancelled", "cancelling"}:
                raise PermissionError("Chief task is no longer active")
            if goal.get("workflow_node"):
                from app.core.workflow_execution import guard_workflow_access

                await guard_workflow_access(
                    runtime, parent, goal, current_acl=settings.resource_acl_enabled
                )

    async def record_scan_event(event):
        await record_threat_findings(
            runtime,
            settings,
            tenant_id,
            event,
            context_id=goal_id,
            model=goal["model"],
            source_automation_id=goal.get("automation_id"),
        )
        await runtime.append_audit(
            tenant_id,
            {
                "action": "goal.threat_detection",
                "status": event.get("status", "detected"),
                "goal_id": goal_id,
                "agent_id": goal.get("agent_id"),
                "event": event,
            },
        )

    async def validate_inputs():
        await require_enabled_agent()
        values = [("goal_objective", goal["objective"])] + [
            (f"goal_step:{index}", step["query"]) for index, step in enumerate(goal["steps"])
        ]
        values.extend(
            (
                f"skill:{snapshot['skill_id']}:r{snapshot['revision']}",
                skill_text(snapshot),
            )
            for snapshot in (goal.get("agent_config") or {}).get("skill_snapshots", [])
        )
        for location, value in values:
            if any(
                re.search(pattern, value, re.IGNORECASE)
                for pattern in settings.content_blocked_patterns
            ):
                raise PermissionError("Goal input is blocked by current content policy")
            if len(value.encode("utf-8")) > settings.request_token_budget:
                raise ValueError("Goal input exceeds current request token budget")
            if threat_detector:
                scan = await threat_detector.ascan(
                    value,
                    event_type="llm_input",
                    location=location,
                    fields={"user_input": value},
                )
                if scan.needs_record:
                    await record_scan_event(scan.to_event())
                    await save(scan.to_event())
                if scan.blocked:
                    raise ThreatBlockedError(scan)

    async def keep_alive():
        nonlocal ownership_revoked
        while True:
            await asyncio.sleep(min(1.0, lease_ms / 3_000))
            try:
                alive = await store.renew(goal_id, tenant_id, owner, lease_ms)
                if alive and slot_acquired:
                    alive = await store.renew_run_slot(
                        tenant_id, slot_id, max(60, int(lease_ms / 1_000))
                    )
            except Exception:
                alive = False
            if not alive:
                ownership_revoked = True
                if execution_task:
                    execution_task.cancel()
                return

    async def finish(status, event):
        await require_enabled_agent()
        goal["status"] = status
        goal["runtime_seconds"] = prior_runtime + time.monotonic() - started
        if not await store.finish(goal, owner, event):
            raise GoalOwnershipLost("Goal state changed before completion")

    async def _execute_steps():
        await validate_inputs()
        if not goal["steps"]:
            goal["steps"] = await _plan_steps(goal, model)
            if threat_detector:
                for index, step in enumerate(goal["steps"]):
                    scan = await threat_detector.ascan(
                        step["query"],
                        event_type="llm_output",
                        location=f"goal_planner_step:{index}",
                        fields={"agent_output": step["query"]},
                    )
                    if scan.needs_record:
                        await record_scan_event(scan.to_event())
                        await runtime.append_goal_event(goal_id, scan.to_event(), tenant_id)
                    if scan.blocked:
                        raise ThreatBlockedError(scan)
            await save({"type": "goal_planned", "steps": goal["steps"]})

        for index in range(goal["current_step"], len(goal["steps"])):
            await validate_inputs()
            latest = await runtime.get_goal(goal_id, tenant_id)
            if latest and latest.get("status") == "cancelled":
                return
            if latest and latest.get("status") in {"pausing", "paused"}:
                await finish("paused", {"type": "goal_status", "status": "paused"})
                return

            step = goal["steps"][index]
            resume_checkpoint = checkpointer is not None and step.get("status") == "running"
            step["status"] = "running"
            goal["current_step"] = index
            goal["updated_at"] = _now()
            await save({"type": "goal_step", "status": "running", "index": index, "step": step})

            request_id = f"{goal_id}:{step['id']}"
            history = await runtime.get_conversation(
                goal_id, conversation_max_turns * 2, tenant_id=tenant_id
            )
            graph = build_agent_graph(
                model,
                scraper,
                checkpointer=checkpointer,
                tool_executor=executor,
                threat_detector=threat_detector,
                safety_journal=graph_journal(
                    runtime,
                    settings,
                    tenant_id,
                    goal_id,
                    model=goal["model"],
                    kind="goal",
                    step_index=index,
                    subject=goal.get("agent_id") or "agent",
                    source_automation_id=goal.get("automation_id"),
                ),
                autonomous_tool_planning=(
                    settings.autonomous_tool_planning
                    and goal.get("agent_config", {}).get("role") != "chief"
                ),
            )
            completed: dict[str, Any] | None = None
            decision = goal.get("approval_decision")
            pending_approval = goal.get("pending_approval") or {}
            graph_input = (
                Command(resume={**decision, "action_id": pending_approval.get("action_id")})
                if decision and decision.get("approved")
                else None
                if resume_checkpoint
                else initial_state(
                    request_id,
                    step["query"],
                    step.get("target_url"),
                    goal["max_retries"],
                    conversation_id=goal_id,
                    model=goal["model"],
                    conversation_history=history,
                    context_max_tokens=context_max_tokens,
                    max_tool_rounds=int(goal.get("max_tool_rounds", 8)),
                    interactive_approval=checkpointer is not None,
                    require_bound_approval=True,
                    structured_output_schema=(goal.get("workflow_node") or {}).get("output_schema"),
                )
            )
            config = {"configurable": {"thread_id": f"{tenant_id}:{goal_id}:{step['id']}"}}
            pending = None
            async with asyncio.timeout(float(goal.get("step_deadline_seconds", 300))):
                async for event in graph.astream(
                    graph_input,
                    config=config,
                    stream_mode="custom",
                ):
                    observe_agent_event(event, model=goal["model"])
                    if event.get("type") == "threat_detection" and not event.get("safety_event_id"):
                        await record_scan_event(event)
                    await runtime.append_goal_event(
                        goal_id,
                        {"type": "agent_event", "step_index": index, "event": event},
                        tenant_id,
                    )
                    if event.get("type") == "completed":
                        completed = event
                    if event.get("type") == "approval_required":
                        pending = event
                    if event.get("type") not in {"token", "draft_token"}:
                        await save()
                    latest = None
                    if event.get("type") not in {"token", "draft_token"}:
                        latest = await runtime.get_goal(goal_id, tenant_id)
                    if latest and latest.get("status") == "cancelled":
                        return
                    if latest and latest.get("status") == "pausing":
                        step["status"] = "running" if checkpointer is not None else "pending"
                        goal["steps"][index] = step
                        await finish("paused", {"type": "goal_status", "status": "paused"})
                        return

            if checkpointer is not None:
                snapshot = await graph.aget_state(config)
                interrupts = list(getattr(snapshot, "interrupts", ()) or ())
                for task in getattr(snapshot, "tasks", ()):
                    interrupts.extend(getattr(task, "interrupts", ()) or ())
                if interrupts:
                    pending = pending or getattr(interrupts[0], "value", {})
                    if not isinstance(pending, dict) or not pending.get("action_id"):
                        raise RuntimeError("Approval interruption is missing exact action identity")
                    approval_id = str(uuid4())
                    ttl = int(policy.get("approval_timeout_seconds", 86_400))
                    expires = datetime.fromtimestamp(time.time() + ttl, UTC).isoformat()
                    goal["pending_approval"] = {
                        **pending,
                        "approval_id": approval_id,
                        "expires_at": expires,
                        "step_id": step["id"],
                    }
                    goal.pop("approval_decision", None)
                    await finish(
                        "waiting_approval",
                        {
                            "type": "goal_waiting_approval",
                            **goal["pending_approval"],
                        },
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
            goal.pop("pending_approval", None)
            goal.pop("approval_decision", None)
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
            await save({"type": "goal_step", "status": "completed", "index": index, "step": step})

        if goal.get("workflow_node"):
            from app.core.workflow_plan import node_output

            goal["workflow_output"] = await asyncio.to_thread(
                node_output, goal["workflow_node"], goal["steps"][-1]["answer"]
            )
        await finish(
            "completed", {"type": "goal_completed", "goal": {**goal, "status": "completed"}}
        )

    try:
        await require_enabled_agent()
        if goal.get("agent_config"):
            from app.agents import AgentStore

            if not await AgentStore(runtime).admit_goal(
                tenant_id, goal["agent_id"], goal_id, owner
            ):
                raise PermissionError("Agent daily run limit exceeded")
            admitted = await runtime.get_goal(goal_id, tenant_id)
            for field in ("agent_admitted", "agent_admitted_at", "agent_admitted_day"):
                goal[field] = admitted[field]
        await runtime.enforce_rate_limit(tenant_id, settings.tenant_requests_per_minute)
        slot_acquired = await runtime.acquire_run_slot(
            tenant_id,
            slot_id,
            settings.tenant_max_concurrent_runs,
            ttl_seconds=max(60, int(lease_ms / 1_000)),
        )
        if not slot_acquired:
            goal["execution_waiting_capacity"] = True
            goal["status"] = "queued"
            await store.finish(goal, owner, {"type": "goal_capacity_wait"})
            return
        goal.pop("execution_waiting_capacity", None)
        await store.reserve_budget(
            goal,
            owner,
            settings.tenant_daily_token_budget,
            current_acl=settings.resource_acl_enabled,
        )
        heartbeat = asyncio.create_task(keep_alive())
        model = BudgetedModel(
            router.get(goal["model"]),
            int(policy.get("token_budget", 20_000)),
            existing_usage=goal.get("usage"),
            on_usage=on_usage,
            max_output_tokens=settings.llm_max_output_tokens,
            persona=goal.get("agent_config", {}).get("persona", ""),
            skill_snapshots=goal.get("agent_config", {}).get("skill_snapshots", []),
        )
        executor = PolicyToolExecutor(
            tool_executor or _ScraperExecutor(scraper),
            policy,
            runtime=runtime,
            tenant_id=tenant_id,
            goal_id=goal_id,
            execution_owner=owner,
            current_acl=settings.resource_acl_enabled,
        )
        total_deadline = min(
            3_600, float(goal.get("step_deadline_seconds", 300)) * max(1, len(goal["steps"]) or 10)
        )
        remaining = total_deadline - prior_runtime
        if remaining <= 0:
            raise TimeoutError("Goal total runtime budget exhausted")
        async with asyncio.timeout(remaining):
            if goal.get("agent_config", {}).get("role") == "chief" and not goal.get(
                "workflow_node"
            ):
                from app.core.chief import prepare_chief, prepare_summary

                await validate_inputs()
                if await prepare_chief(goal, model, runtime, owner, settings, threat_detector):
                    return
                if goal.get("delegation_results") and not goal.get("synthesis_prepared"):
                    goal.update(prepare_summary(goal))
                    await save()
            await _execute_steps()
    except asyncio.CancelledError:
        # A cancelled/reclaimed goal must never be overwritten by the stale worker.
        if not ownership_revoked:
            raise
    except GoalOwnershipLost:
        return
    except Exception as exc:
        current_step = int(goal.get("current_step", 0))
        if current_step < len(goal.get("steps", [])):
            goal["steps"][current_step]["status"] = "failed"
            goal["steps"][current_step]["error"] = f"{type(exc).__name__}: {exc}"
        goal["error"] = f"{type(exc).__name__}: {exc}"
        goal["error_type"] = type(exc).__name__
        await store.finish(
            {
                **goal,
                "status": "failed",
                "runtime_seconds": prior_runtime + time.monotonic() - started,
            },
            owner,
            {"type": "goal_failed", "error": goal["error"]},
        )
    finally:
        if heartbeat:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
        if slot_acquired:
            await runtime.release_run_slot(tenant_id, slot_id)
        await store.reconcile_budget(goal, owner)
        if goal.get("parent_goal_id"):
            if goal.get("workflow_node"):
                from app.core.workflow_execution import WorkflowExecutionStore

                await WorkflowExecutionStore(runtime).reconcile(
                    tenant_id, goal["parent_goal_id"], settings
                )
            else:
                from app.core.delegation import DelegationStore

                await DelegationStore(runtime).requeue_parent(tenant_id, goal["parent_goal_id"])


async def recover_goals(runtime: RedisRuntime) -> None:
    store = GoalExecutionStore(runtime)
    await store.recover_expired()
    await store.expire_approvals()


async def main() -> None:
    settings = get_settings()
    runtime = RedisRuntime(
        settings.redis_url,
        settings.redis_stream_maxlen,
        durable_source_id=settings.postgres_source_id if settings.postgres_dsn else None,
    )
    router = ModelRouter(settings)
    scraper = BrowserScraper(settings)
    threat_detector = ThreatDetector.from_settings(settings)
    sessions = None
    tool_executor = None
    if settings.browser_worker_url:
        tool_executor = RemoteBrowserToolExecutor(
            settings.browser_worker_url,
            settings.browser_worker_token,
            timeout_seconds=max(60.0, settings.playwright_timeout_ms / 1_000 + 10),
        )
        scraper = RemoteBrowserScraper(tool_executor)
    elif settings.agent_tool_transport == "mcp":
        tool_executor = MCPBrowserToolExecutor()
    else:
        sessions = BrowserSessionManager(settings)
        tool_executor = LocalBrowserToolExecutor(scraper, sessions)
    consumer = f"{socket.gethostname()}-{uuid4().hex[:8]}"
    checkpointer = None
    if settings.langgraph_checkpointing:
        checkpointer = await open_checkpointer(runtime, settings)
    await recover_goals(runtime)
    queue_index = 0
    try:
        while True:
            queue = QUEUES[queue_index % len(QUEUES)]
            queue_index += 1
            claimed = await runtime.claim_goal(
                group=settings.goal_consumer_group,
                consumer=consumer,
                block_seconds=max(1, int(settings.goal_worker_poll_seconds)),
                lease_ms=settings.goal_lease_ms,
                queue=queue,
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
                    queue=queue,
                )
    finally:
        if checkpointer:
            await checkpointer.__aexit__(None, None, None)
        if sessions:
            await sessions.close_all()
        if isinstance(tool_executor, MCPBrowserToolExecutor):
            await tool_executor.close()
        await runtime.close()


if __name__ == "__main__":
    asyncio.run(main())
