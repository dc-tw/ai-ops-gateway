"""Redis authority for explicit Plan admission and restartable DAG decisions.

The controller does not call a model or a tool. Every node is an ordinary fenced
Goal, with deterministic identity, pinned policy and the existing approval flow.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from redis.exceptions import WatchError

from app.core.goal_execution import GoalExecutionStore, GoalMutation
from app.core.goal_protocol import (
    WORKFLOW_PREFIX,
    WORKFLOW_QUEUE,
    goal_index,
    goal_key,
    require_goal_protocol,
    tenants_key,
)
from app.core.planning_store import (
    PlanningConflict,
    PlanningIntegrityError,
    PlanningStore,
    canonical_hash,
)
from app.core.resource_access import AccessStore, acl_key, guard_owner, queue_owner
from app.core.security import Principal
from app.core.skill_store import guard_goal_skills
from app.core.workflow_plan import (
    TERMINAL,
    adapt_legacy_plan,
    canonical,
    compile_workflow,
    readiness,
    resolve_inputs,
    verify_compiled,
)


def now():
    return datetime.now(UTC).isoformat()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def node_id(tenant, root_id, key):
    return WORKFLOW_PREFIX + str(uuid5(NAMESPACE_URL, f"gateway:workflow:{tenant}:{root_id}:{key}"))


def charged(goal):
    usage = goal.get("usage") or {}
    return max(int(usage.get("total_tokens", 0)), int(usage.get("policy_charged_tokens", 0)))


async def guard_access(
    runtime, pipe, principal, kind, resource_id, enabled, permission="operate", *, watch=True
):
    if not enabled:
        return
    key = acl_key(principal.tenant_id, kind, resource_id)
    if watch:
        await pipe.watch(key)
    raw = await pipe.get(key)
    if raw and AccessStore.decode(raw, kind, resource_id)["revoked"]:
        raise PermissionError("Workflow resource access was revoked")
    if not await AccessStore(runtime).can(principal, kind, resource_id, permission):
        raise PermissionError("Workflow resource is not authorized")


async def guard_workflow_access(
    runtime, root, child=None, *, client=None, watch=False, current_acl=False
):
    require_goal_protocol(root)
    workflow = root["workflow"]
    if not (workflow["acl_enforced"] or current_acl):
        return
    principal = Principal(**workflow["principal"])
    if principal.tenant_id != root["tenant_id"]:
        raise PermissionError("Workflow authority tenant does not match")
    reader = client if client is not None else runtime._client
    resources = [
        ("goal", root["goal_id"], "operate"),
        ("planning_session", workflow["session_id"], "operate"),
    ]
    if child:
        require_goal_protocol(child)
        if (
            child["tenant_id"] != root["tenant_id"]
            or child["workflow_node"]["root_id"] != root["goal_id"]
        ):
            raise PermissionError("Workflow child authority does not match")
        resources.extend(
            [("goal", child["goal_id"], "operate"), ("agent", child["agent_id"], "operate")]
        )
        resources.extend(
            ("skill", ref["skill_id"], "read")
            for ref in child.get("agent_config", {}).get("skills", [])
        )
    for kind, resource_id, permission in resources:
        await guard_access(
            runtime, reader, principal, kind, resource_id, True, permission, watch=watch
        )


async def destinations(pipe, mapping):
    """MULTI does not roll back WRONGTYPE; validate every command destination."""
    await pipe.watch(*mapping)
    for key, expected in mapping.items():
        if await pipe.type(key) not in {"none", expected}:
            raise RuntimeError("Workflow persistence destination unavailable")


class WorkflowExecutionStore:
    def __init__(self, runtime):
        self.runtime, self.client = runtime, runtime._client

    @staticmethod
    def root_id(tenant, session_id, idempotency_key):
        return WORKFLOW_PREFIX + str(
            uuid5(NAMESPACE_URL, f"gateway:workflow:{tenant}:{session_id}:{idempotency_key}")
        )

    async def admit(
        self,
        principal,
        session_id,
        expected_version,
        accepted_hash,
        idempotency_key,
        builder,
        settings,
        *,
        scanned_hash,
    ):
        """Atomically reserve team quota, create root/ACL, bind session and queue.

        `scanned_hash` must be supplied by the trusted HTTP service after ATR,
        never taken from a client field. WATCH rechecks that exact artifact.
        """
        tenant = principal.tenant_id
        store = PlanningStore(self.runtime)
        session_key = store.session_key(tenant, session_id)
        root_id = self.root_id(tenant, session_id, idempotency_key)
        key = goal_key(tenant, root_id)
        fingerprint = hashlib.sha256(idempotency_key.encode()).hexdigest()
        receipt_key = f"tenant:{tenant}:planning:admission:{session_id}:{fingerprint}"
        for _ in range(16):
            day = datetime.now(UTC).strftime("%Y-%m-%d")
            quota_key = f"tenant:{tenant}:quota:tokens:{day}"
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(session_key, key, receipt_key, quota_key)
                    session = store._session(await pipe.get(session_key), tenant, session_id)
                    await guard_access(
                        self.runtime,
                        pipe,
                        principal,
                        "planning_session",
                        session_id,
                        settings.resource_acl_enabled,
                    )
                    prior = await pipe.get(receipt_key)
                    if prior:
                        receipt = json.loads(prior)
                        if receipt.get("accepted_hash") != accepted_hash:
                            raise PlanningConflict("Idempotency key belongs to another Plan hash")
                        await guard_access(
                            self.runtime,
                            pipe,
                            principal,
                            "goal",
                            root_id,
                            settings.resource_acl_enabled,
                            "read",
                        )
                        result = json.loads(await pipe.get(key) or "null")
                        if (
                            not result
                            or result.get("goal_id") != root_id
                            or result.get("tenant_id") != tenant
                            or (result.get("workflow") or {}).get("accepted_hash") != accepted_hash
                        ):
                            raise PlanningConflict("Admission receipt has no matching Goal")
                        old = result["workflow"]
                        if (
                            old.get("session_id") != session_id
                            or type(old.get("revision")) is not int
                        ):
                            raise PlanningIntegrityError("Workflow admission identity is invalid")
                        original_key = store.revision_key(tenant, session_id, old["revision"])
                        await pipe.watch(original_key)
                        original = store._revision(
                            await pipe.get(original_key),
                            tenant,
                            session_id,
                            old["revision"],
                            session["agent_id"],
                        )
                        if (
                            original["content_sha256"] != accepted_hash
                            or original["snapshot"] != old["snapshot"]
                        ):
                            raise PlanningConflict(
                                "Admission artifact no longer matches its revision"
                            )
                        try:
                            verify_compiled(old["compiled"], old["plan"], old["snapshot"])
                        except ValueError:
                            raise PlanningIntegrityError(
                                "Workflow admission artifact is invalid"
                            ) from None
                        pipe.multi()
                        await pipe.execute()
                        return result
                    store._expected(session, expected_version)
                    revision = await store._current_revision(pipe, session, watch=True)
                    if (
                        session["mode"] != "execute"
                        or not revision
                        or not revision["complete"]
                        or accepted_hash != session["accepted_hash"]
                        or scanned_hash != accepted_hash
                    ):
                        raise PlanningConflict(
                            "Explicitly accept and scan the current complete Plan"
                        )
                    if session.get("active_goal_id"):
                        raise PlanningConflict("This accepted Plan already has an execution")
                    current = await builder(pipe)
                    if canonical_hash(current) != session["snapshot_sha256"]:
                        raise PlanningConflict("Planning configuration is stale")
                    limits = {
                        "max_nodes": settings.workflow_max_nodes,
                        "max_edges": settings.workflow_max_edges,
                        "max_parallelism": min(
                            settings.workflow_max_parallelism, settings.tenant_max_concurrent_runs
                        ),
                    }
                    plan = revision["plan"]
                    native = plan.get("schema_version") == 2
                    source = plan if native else adapt_legacy_plan(plan, current)
                    compiled = await asyncio.to_thread(compile_workflow, source, current, **limits)
                    if native and compiled != revision["compiled"]:
                        raise PlanningConflict(
                            "Workflow execution limits changed; refresh the Plan"
                        )
                    for node in compiled["nodes"]:
                        await guard_access(
                            self.runtime,
                            pipe,
                            principal,
                            "agent",
                            node["agent_id"],
                            settings.resource_acl_enabled,
                        )
                        for skill in node["skills"]:
                            await guard_access(
                                self.runtime,
                                pipe,
                                principal,
                                "skill",
                                skill["skill_id"],
                                settings.resource_acl_enabled,
                                "read",
                            )
                    budget = compiled["token_budget"]
                    if (
                        int(await pipe.get(quota_key) or 0) + budget
                        > settings.tenant_daily_token_budget
                    ):
                        raise PlanningConflict("Tenant daily token budget exceeded")
                    root = {
                        "workflow_protocol": 1,
                        "goal_id": root_id,
                        "tenant_id": tenant,
                        "created_by": principal.subject,
                        "created_by_type": principal.actor_type,
                        "objective": compiled["objective"],
                        "model": current["root_agent"]["model"],
                        "agent_id": session["agent_id"],
                        "status": "queued",
                        "steps": [],
                        "current_step": 0,
                        "policy": {"token_budget": budget},
                        "token_budget": budget,
                        "created_at": now(),
                        "updated_at": now(),
                        "history_search_approved": True,
                        "token_reservation": {
                            "day": day,
                            "reserved": budget,
                            "base_usage": 0,
                            "reconciled": False,
                        },
                        "workflow": {
                            "version": 1,
                            "acl_enforced": settings.resource_acl_enabled,
                            "session_id": session_id,
                            "accepted_hash": accepted_hash,
                            "revision": revision["revision"],
                            "plan": source,
                            "snapshot": current,
                            "compiled": compiled,
                            "principal": {
                                "subject": principal.subject,
                                "tenant_id": tenant,
                                "role": principal.role,
                                "auth_method": principal.auth_method,
                                "actor_type": principal.actor_type,
                            },
                            "states": {},
                            "phase": "running",
                        },
                    }
                    for kind in ("goal", "conversation"):
                        await guard_owner(
                            pipe, tenant, kind, root_id, principal.subject, principal.actor_type
                        )
                    await destinations(
                        pipe,
                        {
                            key: "string",
                            receipt_key: "string",
                            quota_key: "string",
                            goal_index(tenant, root_id): "set",
                            tenants_key(root_id): "set",
                            WORKFLOW_QUEUE: "stream",
                            f"tenant:{tenant}:agent:goal:events:{root_id}": "stream",
                            f"tenant:{tenant}:audit": "stream",
                            store.events_key(tenant, session_id): "stream",
                        },
                    )
                    if await pipe.exists(key):
                        # Another replica may have committed after our initial
                        # receipt read. Validate WATCH before diagnosing a real
                        # persistent identity collision.
                        pipe.multi()
                        await pipe.execute()
                        raise PlanningConflict("Workflow Goal identity already exists")
                    session.update(
                        active_goal_id=root_id,
                        version=session["version"] + 1,
                        updated_at=now(),
                        updated_by=principal.subject,
                    )
                    if datetime.now(UTC).strftime("%Y-%m-%d") != day:
                        raise WatchError("Admission crossed UTC midnight")
                    pipe.multi()
                    pipe.incrby(quota_key, budget)
                    pipe.expire(quota_key, 172800)
                    pipe.set(key, encoded(root))
                    pipe.set(
                        receipt_key, encoded({"goal_id": root_id, "accepted_hash": accepted_hash})
                    )
                    pipe.set(session_key, encoded(session))
                    for kind in ("goal", "conversation"):
                        queue_owner(
                            pipe, tenant, kind, root_id, principal.subject, principal.actor_type
                        )
                    pipe.sadd(goal_index(tenant, root_id), root_id)
                    pipe.sadd(tenants_key(root_id), tenant)
                    pipe.xadd(WORKFLOW_QUEUE, {"tenant_id": tenant, "goal_id": root_id})
                    self._event(pipe, root, "workflow.admitted")
                    store._record_event(
                        pipe, session, "planning.execution_admitted", principal.subject
                    )
                    await pipe.execute()
                    return root
                except WatchError:
                    await asyncio.sleep(0)
        raise PlanningConflict("Workflow admission changed too often; retry")

    @staticmethod
    def _event(pipe, root, event_type):
        tenant, root_id = root["tenant_id"], root["goal_id"]
        event = {"type": event_type, "goal_id": root_id, "status": root["status"], "time": now()}
        pipe.xadd(f"tenant:{tenant}:agent:goal:events:{root_id}", {"payload": encoded(event)})
        pipe.xadd(f"tenant:{tenant}:audit", {"payload": encoded(event | {"action": event_type})})

    @staticmethod
    def _child(root, node, inputs, states):
        workflow, tenant = root["workflow"], root["tenant_id"]
        snapshot = workflow["snapshot"]
        agents = {a["agent_id"]: a for a in [snapshot["root_agent"], *snapshot["workers"]]}
        agent = deepcopy(agents[node["agent_id"]])
        agent.update(
            skills=node["skills"],
            skill_snapshots=node["skill_snapshots"],
            skill_selection=node["skill_selection"],
        )
        policy = agent["policy"] | {"token_budget": node["token_budget"]}
        query = "\n\n".join(
            filter(
                None,
                [
                    workflow["compiled"]["objective"],
                    workflow["compiled"]["context"],
                    node["query"],
                    "Validated upstream inputs:\n" + canonical(inputs) if inputs else "",
                ],
            )
        )
        if node["kind"] == "synthesis":
            # Legacy summaries also consume evidence; no free replanning/delegation.
            query += "\n\nUpstream results:\n" + canonical(
                {key: states[key] for key in node["dependencies"]}, 65536
            )
        child_id = node_id(tenant, root["goal_id"], node["node_id"])
        return {
            "workflow_protocol": 1,
            "goal_id": child_id,
            "tenant_id": tenant,
            "created_by": root["created_by"],
            "created_by_type": root.get("created_by_type", "human"),
            "parent_goal_id": root["goal_id"],
            "root_goal_id": root["goal_id"],
            "objective": node["query"],
            "agent_id": node["agent_id"],
            "agent_config": agent,
            "agent_role": agent["role"],
            "model": node["model"],
            "policy": policy,
            "token_budget": node["token_budget"],
            "status": "queued",
            "current_step": 0,
            "max_retries": workflow["compiled"]["max_retries"],
            "max_tool_rounds": workflow["compiled"]["max_tool_rounds"],
            "step_deadline_seconds": workflow["compiled"]["step_deadline_seconds"],
            "created_at": now(),
            "updated_at": now(),
            "history_search_approved": True,
            "approval_timeout_seconds": policy.get("approval_timeout_seconds", 86400),
            "workflow_node": {
                "root_id": root["goal_id"],
                "node_id": node["node_id"],
                "output_schema": node["output_schema"],
            },
            "steps": [
                {
                    "id": node["node_id"],
                    "query": query,
                    "target_url": node["target_url"],
                    "status": "pending",
                    "answer": "",
                    "error": "",
                }
            ],
        }

    async def reconcile(self, tenant, root_id, settings):
        try:
            return await self._reconcile(tenant, root_id, settings)
        except (ValueError, KeyError, TypeError, AttributeError):
            # Corrupt tenant data is not a reason to stop every scheduler tick.
            # Never dispatch from an unverifiable artifact; retain it for
            # diagnosis and fence its children before proceeding to other roots.
            return await self.quarantine(tenant, root_id)

    async def quarantine(self, tenant, root_id):
        execution = GoalExecutionStore(self.runtime)

        def change(root):
            if not root or not root.get("workflow"):
                return GoalMutation()
            workflow = root["workflow"]
            if isinstance(workflow, dict) and workflow.get("phase") == "quarantined":
                return GoalMutation(result=root)
            if not isinstance(workflow, dict):
                root["quarantined_workflow"] = workflow
                workflow = {}
            workflow["phase"] = "quarantined"
            workflow["integrity_status"] = "invalid"
            root.update(
                workflow=workflow,
                status="failed",
                error_type="WorkflowIntegrityError",
                error="Workflow artifact failed integrity checks; manual review required",
                execution_owner=None,
                execution_lease_until=0,
                updated_at=now(),
            )
            return GoalMutation(
                root,
                [{"type": "goal_failed", "error_type": "WorkflowIntegrityError"}],
                [
                    {
                        "action": "workflow.quarantined",
                        "goal_id": root_id,
                        "error_type": "WorkflowIntegrityError",
                    }
                ],
                result=root,
            )

        root = await execution._mutate(tenant, root_id, change)
        if root:
            # Use actual child authority records, never corrupted graph edges.
            for child_id in await self.runtime.list_goal_ids(tenant):
                child = await self.runtime.get_goal(child_id, tenant)
                if (
                    isinstance(child, dict)
                    and child.get("parent_goal_id") == root_id
                    and child.get("status") not in TERMINAL
                ):
                    try:
                        await execution.control(
                            tenant, child_id, "cancel", "system:workflow-integrity"
                        )
                    except ValueError:
                        pass
            await execution.reconcile_budget(root)
        return root

    async def _reconcile(self, tenant, root_id, settings):
        key = goal_key(tenant, root_id)
        for _ in range(16):
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    root = json.loads(await pipe.get(key) or "null")
                    if not root or not root.get("workflow"):
                        return None
                    require_goal_protocol(root)
                    workflow = root["workflow"]
                    acl_required = settings.resource_acl_enabled or workflow["acl_enforced"]
                    prior_status = root["status"]
                    compiled = workflow["compiled"]
                    verify_compiled(compiled, workflow["plan"], workflow["snapshot"])
                    if workflow.get("phase") == "finished" and root["status"] in TERMINAL:
                        await GoalExecutionStore(self.runtime).reconcile_budget(root)
                        return root
                    child_keys = {
                        node["node_id"]: goal_key(tenant, node_id(tenant, root_id, node["node_id"]))
                        for node in compiled["nodes"]
                    }
                    await pipe.watch(*child_keys.values())
                    children = {
                        name: json.loads(raw)
                        for name, raw in zip(
                            child_keys, await pipe.mget(list(child_keys.values())), strict=True
                        )
                        if raw
                    }
                    states = deepcopy(workflow["states"])
                    for name, child in children.items():
                        state = {"status": child["status"], "goal_id": child["goal_id"]}
                        if child["status"] == "completed":
                            state.update(child.get("workflow_output") or {})
                        states[name] = state
                    principal = Principal(**workflow["principal"])
                    revoked = False
                    try:
                        for kind, resource in (
                            ("planning_session", workflow["session_id"]),
                            ("goal", root_id),
                        ):
                            await guard_access(
                                self.runtime,
                                pipe,
                                principal,
                                kind,
                                resource,
                                acl_required,
                            )
                    except PermissionError:
                        revoked = True
                    stopped = (
                        root["status"] in {"cancelled", "failed"}
                        or workflow["phase"] == "cancelling"
                        or revoked
                    )
                    decision = readiness(compiled, states)
                    stopping = stopped or decision["status"] == "failed"
                    pausing = not stopping and root["status"] in {"pausing", "paused"}
                    resuming = not stopping and not pausing and workflow.get("paused", False)
                    pause_children = (
                        [
                            c
                            for c in children.values()
                            if c["status"] in {"queued", "running", "pausing"}
                        ]
                        if pausing
                        else []
                    )
                    resume_children = (
                        [
                            c
                            for c in children.values()
                            if c["status"] == "paused"
                            and c["goal_id"] in workflow.get("paused_children", [])
                        ]
                        if resuming
                        else []
                    )
                    pending_cancellation = (
                        [child for child in children.values() if child["status"] not in TERMINAL]
                        if stopping
                        else []
                    )
                    if stopping:
                        decision["ready"] = []
                        workflow["phase"] = "cancelling" if pending_cancellation else "finished"
                        if root["status"] != "cancelled":
                            root["status"] = "cancelling" if pending_cancellation else "failed"
                    else:
                        root["status"] = (
                            "completed" if decision["status"] == "completed" else "waiting_nodes"
                        )
                        workflow["phase"] = (
                            "finished" if root["status"] == "completed" else "running"
                        )
                        if pausing:
                            decision["ready"] = []
                            root["status"] = "pausing" if pause_children else "paused"
                            workflow["phase"] = "running"
                            workflow["paused"] = True
                            workflow["paused_children"] = sorted(
                                set(workflow.get("paused_children", []))
                                | {c["goal_id"] for c in pause_children}
                            )
                        elif resuming:
                            decision["ready"] = []
                            workflow["paused"] = bool(resume_children)
                            if not resume_children:
                                workflow["paused_children"] = []
                    for name in decision["skip"]:
                        states[name] = {"status": "skipped"}
                    for name in decision.get("fail", []):
                        states[name] = {
                            "status": "failed",
                            "error_type": "RequiredInputUnavailable",
                        }
                    new_children = []
                    for name in decision["ready"]:
                        node = next(node for node in compiled["nodes"] if node["node_id"] == name)
                        try:
                            child = self._child(root, node, resolve_inputs(node, states), states)
                            if any(
                                len(value.encode("utf-8")) > settings.request_token_budget
                                for value in (child["objective"], child["steps"][0]["query"])
                            ):
                                raise ValueError("Node input exceeds the current request limit")
                            await guard_access(
                                self.runtime,
                                pipe,
                                principal,
                                "agent",
                                node["agent_id"],
                                acl_required,
                            )
                            for skill in node["skills"]:
                                await guard_access(
                                    self.runtime,
                                    pipe,
                                    principal,
                                    "skill",
                                    skill["skill_id"],
                                    acl_required,
                                    "read",
                                )
                            agent_key = f"tenant:{tenant}:agents:record:{node['agent_id']}"
                            await pipe.watch(agent_key)
                            current_agent = json.loads(await pipe.get(agent_key) or "null")
                            if not current_agent or not current_agent.get("enabled"):
                                raise PermissionError("Workflow Agent is disabled")
                            await guard_goal_skills(self.runtime, child, client=pipe, watch=True)
                        except (PermissionError, ValueError):
                            # Persist the failed admission; the next bounded tick
                            # applies required-node cancellation without dispatch.
                            states[name] = {"status": "failed", "error_type": "NodeAdmissionDenied"}
                            continue
                        for kind in ("goal", "conversation"):
                            await guard_owner(
                                pipe,
                                tenant,
                                kind,
                                child["goal_id"],
                                root["created_by"],
                                root.get("created_by_type", "human"),
                            )
                        new_children.append(child)
                        states[name] = {"status": "queued", "goal_id": child["goal_id"]}
                    if any(
                        node["required"]
                        and states.get(node["node_id"], {}).get("status") == "failed"
                        for node in compiled["nodes"]
                    ):
                        # A denied required admission in this very transaction
                        # must suppress unrelated ready nodes, not one tick later.
                        for child in new_children:
                            states.pop(child["workflow_node"]["node_id"], None)
                        new_children = []
                        pending_cancellation = [
                            c for c in children.values() if c["status"] not in TERMINAL
                        ]
                        workflow["phase"] = "cancelling" if pending_cancellation else "finished"
                        if root["status"] != "cancelled":
                            root["status"] = "cancelling" if pending_cancellation else "failed"
                    # Outputs remain authoritative on their bounded child Goal.
                    # Do not duplicate up to 128 x 16 KiB into the root history
                    # record (or accidentally grant child-output access via a
                    # status-only root share).
                    workflow["states"] = {
                        name: {
                            field: value
                            for field, value in state.items()
                            if field not in {"output", "output_validated"}
                        }
                        for name, state in states.items()
                    }
                    if workflow["phase"] == "finished":
                        uncertain, total_receipts = [], 0
                        for child in children.values():
                            uncertain_count = 0
                            index = f"tenant:{tenant}:goal:{child['goal_id']}:tool-receipts"
                            await pipe.watch(index)
                            receipt_keys = sorted(await pipe.smembers(index))
                            if len(receipt_keys) > 256:
                                raise RuntimeError("Workflow receipt capacity exceeded")
                            if receipt_keys:
                                await pipe.watch(*receipt_keys)
                                for _receipt_key, raw in zip(
                                    receipt_keys, await pipe.mget(receipt_keys), strict=True
                                ):
                                    receipt = json.loads(raw or "null")
                                    total_receipts += 1
                                    if not receipt or receipt.get("status") != "completed":
                                        uncertain_count += 1
                            if uncertain_count:
                                uncertain.append(
                                    {
                                        "goal_id": child["goal_id"],
                                        "receipt_index": index,
                                        "count": uncertain_count,
                                        "status": "review_required",
                                    }
                                )
                        workflow["receipt_reconciliation"] = {
                            "status": "review_required" if uncertain else "recorded",
                            "total": total_receipts,
                            "uncertain": uncertain,
                            "automatic_replay": False,
                        }
                    root["usage"] = {
                        "policy_charged_tokens": sum(charged(c) for c in children.values())
                    }
                    root["updated_at"] = now()
                    if root["status"] in {"cancelling", "failed"}:
                        root["error_type"] = (
                            "WorkflowAuthorizationRevoked" if revoked else "WorkflowNodeFailed"
                        )
                        root["error"] = (
                            "Workflow stopped; inspect node states and receipt reconciliation"
                        )
                    internal_failure = None
                    if root["status"] == "failed" and prior_status != "failed":
                        from app.core.internal_events import INTERNAL_STREAM, build_internal_event

                        internal_failure = build_internal_event(
                            "goal.failed",
                            f"goal.failed:{root_id}",
                            {
                                "goal_id": root_id,
                                "status": "failed",
                                "error_type": root["error_type"],
                            },
                        )
                    mapping = {
                        key: "string",
                        WORKFLOW_QUEUE: "stream",
                        goal_index(tenant, root_id): "set",
                        f"tenant:{tenant}:agent:goal:events:{root_id}": "stream",
                        f"tenant:{tenant}:audit": "stream",
                    }
                    mapping.update(
                        {child_keys[c["workflow_node"]["node_id"]]: "string" for c in new_children}
                    )
                    for child in new_children:
                        mapping[f"tenant:{tenant}:agent:goal:events:{child['goal_id']}"] = "stream"
                    if internal_failure:
                        mapping[INTERNAL_STREAM] = "stream"
                    await destinations(pipe, mapping)
                    pipe.multi()
                    pipe.set(key, encoded(root))
                    for child in new_children:
                        pipe.set(goal_key(tenant, child["goal_id"]), encoded(child))
                        for kind in ("goal", "conversation"):
                            queue_owner(
                                pipe,
                                tenant,
                                kind,
                                child["goal_id"],
                                root["created_by"],
                                root.get("created_by_type", "human"),
                            )
                        pipe.sadd(goal_index(tenant, child["goal_id"]), child["goal_id"])
                        pipe.xadd(
                            WORKFLOW_QUEUE, {"tenant_id": tenant, "goal_id": child["goal_id"]}
                        )
                        self._event(pipe, child, "workflow.node_queued")
                    self._event(pipe, root, "workflow.reconciled")
                    if root["status"] != prior_status:
                        compatibility = {
                            "completed": "goal_completed",
                            "failed": "goal_failed",
                            "cancelled": "goal_cancelled",
                        }.get(root["status"], "goal_status")
                        self._event(pipe, root, compatibility)
                    if internal_failure:
                        pipe.xadd(
                            INTERNAL_STREAM,
                            {"tenant_id": tenant, "payload": encoded(internal_failure)},
                        )
                    await pipe.execute()
                    execution = GoalExecutionStore(self.runtime)
                    for action, values in (("pause", pause_children), ("resume", resume_children)):
                        for child in values:
                            try:
                                await execution.control(
                                    tenant, child["goal_id"], action, "system:workflow"
                                )
                            except ValueError:
                                pass  # Concurrent human decision/terminal state wins.
                    for child in pending_cancellation:
                        try:
                            await execution.control(
                                tenant, child["goal_id"], "cancel", "system:workflow"
                            )
                        except ValueError:
                            pass  # A fenced worker reached terminal state first.
                    for child in children.values():
                        if child["status"] in TERMINAL:
                            await execution.reconcile_budget(child)
                    if root["status"] in {"completed", "failed", "cancelled"}:
                        await execution.reconcile_budget(root)
                    return root
                except WatchError:
                    await asyncio.sleep(0)
        return None  # Durable state remains discoverable for a later tick.

    async def reconcile_all(self, settings):
        count = 0
        for tenant in await self.runtime.list_goal_tenants():
            for root_id in await self.runtime.list_goal_ids(tenant):
                root = await self.runtime.get_goal(root_id, tenant)
                if root and root.get("workflow"):
                    workflow = root["workflow"]
                    if not isinstance(workflow, dict) or workflow.get("phase") == "quarantined":
                        # A prior process may have exited after quarantining the
                        # root but before cancelling every actual child.
                        await self.quarantine(tenant, root_id)
                        count += 1
                    elif workflow.get("phase") != "finished" or root["status"] not in TERMINAL:
                        await self.reconcile(tenant, root_id, settings)
                        count += 1
        return count

    async def reserve_node_budget(self, goal, owner, limit, usage=None, *, current_acl=False):
        """Children use the root reservation, including one atomic UTC rollover."""
        tenant, child_id = goal["tenant_id"], goal["goal_id"]
        key = goal_key(tenant, child_id)
        root_key = goal_key(tenant, goal["workflow_node"]["root_id"])
        execution = GoalExecutionStore(self.runtime)
        for _ in range(32):
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key, root_key)
                    child = json.loads(await pipe.get(key) or "null")
                    root = json.loads(await pipe.get(root_key) or "null")
                    if (
                        not execution._owned(child, owner)
                        or not root
                        or root["status"] not in {"queued", "waiting_nodes", "pausing"}
                        or root["workflow"]["phase"] != "running"
                    ):
                        raise PermissionError("Workflow execution ownership was revoked")
                    await guard_workflow_access(
                        self.runtime, root, child, client=pipe, watch=True, current_acl=current_acl
                    )
                    compiled = root["workflow"]["compiled"]
                    all_keys = [
                        goal_key(tenant, node_id(tenant, root["goal_id"], node["node_id"]))
                        for node in compiled["nodes"]
                    ]
                    await pipe.watch(*all_keys)
                    records = [json.loads(raw) for raw in await pipe.mget(all_keys) if raw]
                    if usage is not None:
                        child["usage"] = usage
                    actual = charged(child)
                    if actual > int(child["policy"]["token_budget"]):
                        raise PermissionError("Workflow node token allocation exceeded")
                    total = sum(
                        actual if item["goal_id"] == child_id else charged(item) for item in records
                    )
                    budget = int(root["token_budget"])
                    if total > budget:
                        raise PermissionError("Workflow team token budget exceeded")
                    reservation = deepcopy(root["token_reservation"])
                    if reservation.get("reconciled"):
                        raise PermissionError("Workflow reservation was already reconciled")
                    day = datetime.now(UTC).strftime("%Y-%m-%d")
                    quota_key = f"tenant:{tenant}:quota:tokens:{day}"
                    adjustments = {}
                    if reservation["day"] != day:
                        old = f"tenant:{tenant}:quota:tokens:{reservation['day']}"
                        previous_usage = charged(root)
                        adjustments[old] = (
                            max(0, previous_usage - reservation["base_usage"])
                            - reservation["reserved"]
                        )
                        remaining = max(0, budget - previous_usage)
                        adjustments[quota_key] = remaining
                        reservation = {
                            "day": day,
                            "reserved": remaining,
                            "base_usage": previous_usage,
                            "reconciled": False,
                            "carried_from_day": reservation["day"],
                        }
                    if total < reservation["base_usage"]:
                        correction = reservation["base_usage"] - total
                        old_day = reservation.get("carried_from_day")
                        if old_day:
                            old = f"tenant:{tenant}:quota:tokens:{old_day}"
                            adjustments[old] = adjustments.get(old, 0) - correction
                        adjustments[quota_key] = adjustments.get(quota_key, 0) + correction
                        reservation["base_usage"] = total
                        reservation["reserved"] += correction
                    if adjustments:
                        await pipe.watch(*adjustments)
                        balances = {q: await pipe.get(q) for q in adjustments}
                        if (
                            int(balances.get(quota_key) or 0) + adjustments.get(quota_key, 0)
                            > limit
                        ):
                            raise PermissionError("Tenant daily token budget exceeded")
                    root.update(
                        usage={"policy_charged_tokens": total}, token_reservation=reservation
                    )
                    if datetime.now(UTC).strftime("%Y-%m-%d") != day:
                        raise WatchError("Workflow accounting crossed UTC midnight")
                    pipe.multi()
                    pipe.set(key, encoded(child))
                    pipe.set(root_key, encoded(root))
                    for q, delta in adjustments.items():
                        if delta >= 0 or balances[q] is not None:
                            pipe.incrby(q, delta)
                            pipe.expire(q, 172800)
                    await pipe.execute()
                    return
                except WatchError:
                    await asyncio.sleep(0)
        raise RuntimeError("Workflow token accounting contention; retry safely")
