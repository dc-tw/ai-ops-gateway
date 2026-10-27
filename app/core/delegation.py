"""Atomic, single-layer Chief delegation on the existing Redis Goal queue."""

from __future__ import annotations

import json
import time
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from redis.exceptions import WatchError

from app.core.internal_events import INTERNAL_STREAM, build_internal_event
from app.core.skill_store import skill_refs

TERMINAL = {"completed", "failed", "cancelled"}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _goal_key(tenant: str, goal_id: str) -> str:
    return f"tenant:{tenant}:agent:goal:{goal_id}"


def _scope_child(child: dict[str, Any], allowed_agents: set[str]) -> None:
    from app.core.skill_store import validate_skill_snapshots

    config = child.get("agent_config") or {}
    if child.get("agent_id") not in allowed_agents:
        raise ValueError("Chief cannot delegate to an unapproved Agent")
    if config.get("agent_id") != child.get("agent_id") or config.get("role") != "worker":
        raise ValueError("A delegation target must be a pinned Worker Agent")
    if config.get("delegate_agent_ids"):
        raise ValueError("Worker Agents cannot recursively delegate")
    pinned, effective = config.get("policy") or {}, child.get("policy") or {}
    validate_skill_snapshots(
        config.get("skill_snapshots", []),
        config.get("skills", []),
        effective,
        config.get("role", "worker"),
    )
    if not set(effective.get("allowed_tools", [])).issubset(pinned.get("allowed_tools", [])):
        raise ValueError("Child tools exceed the pinned Agent policy")
    domains, limits = effective.get("allowed_domains", []), pinned.get("allowed_domains", [])
    if limits and (
        not domains
        or any(
            not any(domain == limit or domain.endswith("." + limit) for limit in limits)
            for domain in domains
        )
    ):
        raise ValueError("Child domains exceed the pinned Agent policy")
    budget = int(child.get("token_budget", effective.get("token_budget", 0)))
    if budget < 1 or budget > int(pinned.get("token_budget", 0)):
        raise ValueError("Child budget exceeds the pinned Agent policy")
    if int(effective.get("token_budget", budget)) != budget:
        raise ValueError("Child budget fields disagree")
    if int(effective.get("approval_timeout_seconds", 86_400)) > int(
        pinned.get("approval_timeout_seconds", 86_400)
    ):
        raise ValueError("Child approval timeout exceeds the pinned Agent policy")
    if child.get("model") != config.get("model"):
        raise ValueError("Child model differs from the pinned Agent model")


class DelegationStore:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self.client = runtime._client

    @staticmethod
    def child_id(tenant: str, parent_id: str, index: int) -> str:
        return str(uuid5(NAMESPACE_URL, f"ai-ops-gateway:delegation:{tenant}:{parent_id}:{index}"))

    @staticmethod
    def _lifecycle(pipe: Any, goal: dict[str, Any], event_type: str) -> None:
        event = {
            "type": event_type,
            "goal_id": goal["goal_id"],
            "status": goal["status"],
            "time": _now(),
            "agent_id": goal.get("agent_id"),
        }
        tenant = goal["tenant_id"]
        pipe.xadd(
            f"tenant:{tenant}:agent:goal:events:{goal['goal_id']}",
            {
                "payload": json.dumps(event, ensure_ascii=False),
            },
        )
        if goal.get("automation_id"):
            pipe.xadd(
                f"tenant:{tenant}:automation:{goal['automation_id']}:events",
                {
                    "payload": json.dumps(event | {"automation_id": goal["automation_id"]}),
                },
            )

    @staticmethod
    def _audit(pipe: Any, tenant: str, action: str, goal: dict[str, Any], **fields: Any) -> None:
        event = {
            "action": action,
            "goal_id": goal["goal_id"],
            "agent_id": goal.get("agent_id"),
            "status": goal["status"],
            "time": _now(),
            "skills": skill_refs((goal.get("agent_config") or {}).get("skill_snapshots", [])),
            **fields,
        }
        pipe.xadd(f"tenant:{tenant}:audit", {"payload": json.dumps(event, ensure_ascii=False)})

    async def create_children(
        self,
        parent: dict[str, Any],
        owner: str,
        children: list[dict[str, Any]],
        plan_usage: dict[str, Any] | None = None,
    ) -> bool:
        """Save the entire delegation and its queue intents in one fenced transaction.

        Child definitions have already passed input checks and Agent binding in
        the Chief service. This persistence boundary additionally verifies pinned
        identity, tenant, one-layer scope and the immutable team budget.
        """
        from app.core.skill_store import guard_goal_skills

        if not 1 <= len(children) <= 20:
            raise ValueError("Chief delegation requires between one and twenty children")
        tenant, parent_id = parent["tenant_id"], parent["goal_id"]
        key = _goal_key(tenant, parent_id)
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if not raw:
                        return False
                    current = json.loads(raw)
                    if current.get("tenant_id") != tenant or current.get("goal_id") != parent_id:
                        raise ValueError("Stored Chief identity does not match its tenant key")
                    if current.get("delegation_planned"):
                        return False
                    from app.core.resource_access import AccessStore, acl_key

                    parent_access_key = acl_key(tenant, "goal", parent_id)
                    await pipe.watch(parent_access_key)
                    parent_access_raw = await pipe.get(parent_access_key)
                    # A legacy Goal without an ACL stays admin-only, including
                    # descendants. Explicit adoption overrides untrusted metadata.
                    access_owner = None
                    if parent_access_raw:
                        parent_access = AccessStore.decode(parent_access_raw, "goal", parent_id)
                        if parent_access["revoked"]:
                            raise ValueError("Parent access is revoked")
                        access_owner = parent_access["owner"]
                    if (
                        current.get("status") != "running"
                        or current.get("execution_owner") != owner
                        or float(current.get("execution_lease_until", 0)) <= time.time()
                        or (
                            parent.get("execution_fence") is not None
                            and parent.get("execution_fence") != current.get("execution_fence")
                        )
                    ):
                        return False
                    config = current.get("agent_config") or {}
                    if config.get("role") != "chief" or current.get("parent_goal_id"):
                        raise ValueError("Only a root Chief Goal can delegate")
                    supplied_config = parent.get("agent_config") or {}
                    for field in ("skills", "skill_snapshots"):
                        if supplied_config.get(field, []) != config.get(field, []):
                            raise ValueError("Chief Skill bindings cannot change during delegation")
                    allowed = set(config.get("delegate_agent_ids", []))
                    team_cap = int(
                        current.get(
                            "team_token_budget",
                            (current.get("policy") or {}).get("token_budget", 0),
                        )
                    )
                    if int(parent.get("team_token_budget", team_cap)) != team_cap:
                        raise ValueError("Delegation cannot enlarge the team budget")
                    chief_cap = int(
                        parent.get(
                            "chief_budget", (parent.get("policy") or {}).get("token_budget", 0)
                        )
                    )
                    usage = plan_usage if plan_usage is not None else parent.get("usage", {})
                    spent = int(usage.get("policy_charged_tokens", usage.get("total_tokens", 0)))
                    if chief_cap < spent or chief_cap < 1:
                        raise ValueError("Chief allocation cannot cover planning usage")
                    prepared, budgets = [], []
                    root_id = current.get("root_goal_id") or parent_id
                    if root_id != parent_id:
                        raise ValueError("A root Chief cannot claim another Goal's team identity")
                    for index, candidate in enumerate(children):
                        if candidate.get("tenant_id") != tenant:
                            raise ValueError("Child tenant must equal the Chief tenant")
                        if candidate.get("parent_goal_id", parent_id) != parent_id:
                            raise ValueError("Child parent cannot escape the Chief Goal")
                        if candidate.get("root_goal_id", root_id) != root_id:
                            raise ValueError("Child root cannot escape the Chief Goal")
                        _scope_child(candidate, allowed)
                        child = deepcopy(candidate)
                        child["token_budget"] = int(
                            candidate.get("token_budget", candidate["policy"]["token_budget"])
                        )
                        child.update(
                            goal_id=self.child_id(tenant, parent_id, index),
                            parent_goal_id=parent_id,
                            root_goal_id=root_id,
                            delegated=True,
                            status="queued",
                            current_step=0,
                            created_at=_now(),
                            updated_at=_now(),
                            execution_owner=None,
                            execution_lease_until=0,
                        )
                        child.pop("children_goal_ids", None)
                        child.pop("delegation_results", None)
                        child["delegation_planned"] = False
                        # An automation's provenance follows all descendants.
                        if current.get("automation_id"):
                            child["automation_id"] = current["automation_id"]
                        prepared.append(child)
                        budgets.append(int(child["token_budget"]))
                    if chief_cap + sum(budgets) > team_cap:
                        raise ValueError(
                            "Chief and child allocations exceed the shared team budget"
                        )
                    child_keys = [_goal_key(tenant, child["goal_id"]) for child in prepared]
                    await pipe.watch(*child_keys)
                    if any(await pipe.mget(child_keys)):
                        latest = json.loads(await pipe.get(key) or "null")
                        if latest and latest.get("delegation_planned"):
                            return False
                        raise ValueError("Delegation child identity already exists")
                    updated = {
                        **current,
                        **parent,
                        "status": "waiting_children",
                        "execution_owner": None,
                        "execution_lease_until": 0,
                        "team_token_budget": team_cap,
                        "chief_budget": chief_cap,
                        "children_goal_ids": [child["goal_id"] for child in prepared],
                        "delegation_planned": True,
                        "usage": usage,
                        "updated_at": _now(),
                    }
                    await guard_goal_skills(self.runtime, updated, client=pipe, watch=True)
                    for child in prepared:
                        await guard_goal_skills(self.runtime, child, client=pipe, watch=True)
                        from app.core.resource_access import guard_owner

                        for kind in ("goal", "conversation"):
                            await guard_owner(
                                pipe,
                                tenant,
                                kind,
                                child["goal_id"],
                                access_owner["subject"] if access_owner else None,
                                access_owner["actor_type"] if access_owner else "human",
                            )
                    pipe.multi()
                    pipe.set(key, json.dumps(updated, ensure_ascii=False))
                    pipe.zrem(f"tenant:{tenant}:quota:active-runs", f"goal:{parent_id}:{owner}")
                    self._lifecycle(pipe, updated, "goal_delegated")
                    self._audit(
                        pipe,
                        tenant,
                        "goal.delegate",
                        updated,
                        child_goal_ids=updated["children_goal_ids"],
                        child_count=len(prepared),
                    )
                    for child, child_key in zip(prepared, child_keys, strict=True):
                        pipe.set(child_key, json.dumps(child, ensure_ascii=False))
                        from app.core.resource_access import queue_owner

                        queue_owner(
                            pipe,
                            tenant,
                            "goal",
                            child["goal_id"],
                            access_owner["subject"] if access_owner else None,
                            access_owner["actor_type"] if access_owner else "human",
                        )
                        queue_owner(
                            pipe,
                            tenant,
                            "conversation",
                            child["goal_id"],
                            access_owner["subject"] if access_owner else None,
                            access_owner["actor_type"] if access_owner else "human",
                        )
                        pipe.sadd(f"tenant:{tenant}:agent:goals", child["goal_id"])
                        pipe.sadd("agent:goal:tenants", tenant)
                        pipe.xadd(
                            "agent:goal:queue", {"tenant_id": tenant, "goal_id": child["goal_id"]}
                        )
                        self._lifecycle(pipe, child, "goal_queued")
                    await pipe.execute()
                    return True
                except WatchError:
                    continue

    @staticmethod
    def _state(parent: dict[str, Any], values: list[str | None]) -> dict[str, Any]:
        child_ids = parent.get("children_goal_ids", [])
        results = []
        for goal_id, raw in zip(child_ids, values, strict=True):
            if not raw:
                return {"status": "pending", "results": results, "children_goal_ids": child_ids}
            child = json.loads(raw)
            if (
                child.get("tenant_id") != parent["tenant_id"]
                or child.get("parent_goal_id") != parent["goal_id"]
            ):
                raise ValueError("Stored delegation provenance does not match parent")
            results.append(
                {
                    "goal_id": goal_id,
                    "agent_id": child.get("agent_id"),
                    "status": child.get("status"),
                    "answer": "\n\n".join(
                        str(step.get("answer", "")) for step in child.get("steps", [])
                    ),
                }
            )
        if not child_ids or any(result["status"] not in TERMINAL for result in results):
            status = "pending"
        elif any(result["status"] != "completed" for result in results):
            status = "failed"
        else:
            status = "ready"
        return {"status": status, "results": results, "children_goal_ids": child_ids}

    async def children_ready(self, parent_id: str, tenant: str) -> dict[str, Any]:
        raw = await self.client.get(_goal_key(tenant, parent_id))
        if not raw:
            return {"status": "pending", "results": [], "children_goal_ids": []}
        parent = json.loads(raw)
        ids = parent.get("children_goal_ids", [])
        values = (
            await self.client.mget([_goal_key(tenant, child_id) for child_id in ids]) if ids else []
        )
        return self._state(parent, values)

    async def requeue_parent(self, tenant: str, parent_id: str) -> bool:
        key = _goal_key(tenant, parent_id)
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if not raw:
                        return False
                    parent = json.loads(raw)
                    if parent.get("status") != "waiting_children":
                        return False
                    keys = [
                        _goal_key(tenant, child_id)
                        for child_id in parent.get("children_goal_ids", [])
                    ]
                    if not keys:
                        return False
                    await pipe.watch(*keys)
                    state = self._state(parent, await pipe.mget(keys))
                    if state["status"] == "pending":
                        return False
                    failed = state["status"] == "failed"
                    parent.update(
                        status="failed" if failed else "queued",
                        delegation_results=state["results"],
                        execution_owner=None,
                        execution_lease_until=0,
                        updated_at=_now(),
                    )
                    if failed:
                        parent["error"] = (
                            "One or more delegated Worker Goals failed or were cancelled"
                        )
                    pipe.multi()
                    pipe.set(key, json.dumps(parent, ensure_ascii=False))
                    self._lifecycle(pipe, parent, "goal_failed" if failed else "goal_queued")
                    self._audit(
                        pipe,
                        tenant,
                        "goal.delegation_finished",
                        parent,
                        child_count=len(state["results"]),
                    )
                    if not failed:
                        pipe.xadd("agent:goal:queue", {"goal_id": parent_id, "tenant_id": tenant})
                    else:
                        event = build_internal_event(
                            "goal.failed",
                            f"goal.failed:{parent_id}",
                            {
                                "goal_id": parent_id,
                                "status": "failed",
                                "error_type": "DelegationChildFailed",
                            },
                            source_automation_id=parent.get("automation_id"),
                        )
                        if event is not None:
                            pipe.xadd(
                                INTERNAL_STREAM,
                                {
                                    "tenant_id": tenant,
                                    "payload": json.dumps(event),
                                },
                            )
                    await pipe.execute()
                    if failed:
                        from app.core.goal_execution import GoalExecutionStore

                        await GoalExecutionStore(self.runtime).reconcile_budget(parent)
                    return True
                except WatchError:
                    continue

    async def reconcile_parents(self) -> int:
        count = 0
        for tenant in await self.runtime.list_goal_tenants():
            for parent_id in await self.runtime.list_goal_ids(tenant):
                parent = await self.runtime.get_goal(parent_id, tenant)
                if parent and parent.get("status") == "waiting_children":
                    count += bool(await self.requeue_parent(tenant, parent_id))
                elif (
                    parent and parent.get("children_goal_ids") and parent.get("status") in TERMINAL
                ):
                    count += await self.cancel_children(tenant, parent_id, "system:parent-terminal")
                    from app.core.goal_execution import GoalExecutionStore

                    await GoalExecutionStore(self.runtime).reconcile_budget(parent)
        return count

    async def cancel_children(self, tenant: str, parent_id: str, subject: str) -> int:
        """Revoke unfinished descendants while preserving completed evidence."""
        key = _goal_key(tenant, parent_id)
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if not raw:
                        return 0
                    parent = json.loads(raw)
                    ids = parent.get("children_goal_ids", [])
                    if not ids:
                        return 0
                    keys = [_goal_key(tenant, child_id) for child_id in ids]
                    await pipe.watch(*keys)
                    values = await pipe.mget(keys)
                    children = [json.loads(value) for value in values if value]
                    if any(
                        child.get("tenant_id") != tenant or child.get("parent_goal_id") != parent_id
                        for child in children
                    ):
                        raise ValueError("Stored delegation provenance does not match parent")
                    changed = [child for child in children if child.get("status") not in TERMINAL]
                    pipe.multi()
                    for child in changed:
                        owner = child.get("execution_owner")
                        child.update(
                            status="cancelled",
                            execution_owner=None,
                            execution_lease_until=0,
                            updated_at=_now(),
                        )
                        pipe.set(
                            _goal_key(tenant, child["goal_id"]),
                            json.dumps(child, ensure_ascii=False),
                        )
                        if owner:
                            pipe.zrem(
                                f"tenant:{tenant}:quota:active-runs",
                                f"goal:{child['goal_id']}:{owner}",
                            )
                        self._lifecycle(pipe, child, "goal_status")
                        self._audit(
                            pipe,
                            tenant,
                            "goal.delegation_cancel",
                            child,
                            subject=subject,
                            parent_goal_id=parent_id,
                        )
                    await pipe.execute()
                    if children:
                        from app.core.goal_execution import GoalExecutionStore

                        execution = GoalExecutionStore(self.runtime)
                        # A prior process may have committed cancellation and
                        # died before reconciling its quota reservation.
                        for child in children:
                            await execution.reconcile_budget(child)
                    return len(changed)
                except WatchError:
                    continue
