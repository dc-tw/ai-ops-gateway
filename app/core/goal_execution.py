"""Atomic Goal ownership, controls, approval decisions and quota reservations.

The queue delivers at least once. The goal record is the authority for whether a
worker may execute it; a queue claim alone never grants execution ownership.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from redis.exceptions import WatchError

TERMINAL = {"completed", "failed", "cancelled"}


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def timestamp(value: float | datetime | None = None) -> float:
    return (
        value.timestamp()
        if isinstance(value, datetime)
        else (time.time() if value is None else float(value))
    )


@dataclass
class GoalMutation:
    goal: dict[str, Any] | None = None
    events: list[dict[str, Any]] = field(default_factory=list)
    audits: list[dict[str, Any]] = field(default_factory=list)
    enqueue: bool = False
    result: Any = None
    release_slot: str | None = None


class GoalExecutionStore:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self.client = getattr(runtime, "_client", None)

    @staticmethod
    def key(tenant: str, goal_id: str) -> str:
        return f"tenant:{tenant}:agent:goal:{goal_id}"

    async def _mutate(
        self,
        tenant: str,
        goal_id: str,
        change: Callable[[dict[str, Any] | None], GoalMutation],
    ) -> Any:
        # Explicit test adapter; real RedisRuntime always takes the WATCH path.
        if self.client is None:
            adapter = getattr(self.runtime, "goal_execution_mutate", None)
            if adapter is None:
                raise RuntimeError("Goal execution requires an atomic Redis store")
            return await adapter(tenant, goal_id, change)
        key = self.key(tenant, goal_id)
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    current = json.loads(raw) if raw else None
                    prior_status = current.get("status") if current else None
                    mutation = change(current)
                    if mutation.goal is None:
                        return mutation.result
                    pipe.multi()
                    pipe.set(key, json.dumps(mutation.goal, ensure_ascii=False))
                    pipe.sadd(f"tenant:{tenant}:agent:goals", goal_id)
                    pipe.sadd("agent:goal:tenants", tenant)
                    for event in mutation.events:
                        pipe.xadd(
                            f"tenant:{tenant}:agent:goal:events:{goal_id}",
                            {"payload": json.dumps(event, ensure_ascii=False)},
                        )
                        automation_id = mutation.goal.get("automation_id")
                        if automation_id and event.get("type") in {
                            "goal_queued",
                            "goal_status",
                            "goal_failed",
                            "goal_completed",
                            "goal_waiting_approval",
                            "approval_decided",
                            "goal_recovered",
                        }:
                            lifecycle = {
                                "type": event["type"],
                                "goal_id": goal_id,
                                "automation_id": automation_id,
                                "status": mutation.goal["status"],
                                "time": utc_now(),
                            }
                            if event.get("approval_id"):
                                lifecycle["approval_id"] = event["approval_id"]
                            pipe.xadd(
                                f"tenant:{tenant}:automation:{automation_id}:events",
                                {"payload": json.dumps(lifecycle, ensure_ascii=False)},
                            )
                    for audit in mutation.audits:
                        pipe.xadd(
                            f"tenant:{tenant}:audit",
                            {"payload": json.dumps(audit, ensure_ascii=False)},
                        )
                    if mutation.enqueue:
                        pipe.xadd("agent:goal:queue", {"goal_id": goal_id, "tenant_id": tenant})
                    if mutation.release_slot:
                        pipe.zrem(f"tenant:{tenant}:quota:active-runs", mutation.release_slot)
                    if mutation.goal.get("status") == "failed" and prior_status != "failed":
                        from app.core.internal_events import INTERNAL_STREAM, build_internal_event

                        internal = build_internal_event(
                            "goal.failed",
                            f"goal.failed:{goal_id}",
                            {
                                "goal_id": goal_id,
                                "status": "failed",
                                "error_type": mutation.goal.get(
                                    "error_type", "GoalExecutionFailed"
                                ),
                            },
                            source_automation_id=mutation.goal.get("automation_id"),
                        )
                        if internal:
                            pipe.xadd(
                                INTERNAL_STREAM,
                                {
                                    "tenant_id": tenant,
                                    "payload": json.dumps(internal, ensure_ascii=False),
                                },
                            )
                    await pipe.execute()
                    return mutation.result
                except WatchError:
                    continue

    async def submit_manual(self, goal: dict[str, Any]) -> dict[str, Any]:
        def change(current):
            if current:
                raise ValueError("Goal already exists")
            return GoalMutation(
                goal,
                [{"type": "goal_queued", "goal": goal}],
                [
                    {
                        "action": "goal.create",
                        "status": "queued",
                        "goal_id": goal["goal_id"],
                        "subject": goal.get("created_by", "unknown"),
                    }
                ],
                True,
                goal,
            )

        return await self._mutate(goal.get("tenant_id", "default"), goal["goal_id"], change)

    async def claim(
        self,
        goal_id: str,
        tenant: str,
        owner: str,
        lease_ms: int,
        now: float | None = None,
    ) -> dict[str, Any] | None:
        instant = timestamp(now)

        def change(goal):
            if not goal or goal.get("status") not in {"queued", "running", "pausing"}:
                return GoalMutation()
            if (
                goal.get("execution_owner")
                and float(goal.get("execution_lease_until", 0)) > instant
            ):
                return GoalMutation()
            old_owner = goal.get("execution_owner")
            if goal["status"] == "pausing":
                goal.update(status="paused", execution_owner=None, execution_lease_until=0)
                return GoalMutation(
                    goal,
                    [{"type": "goal_status", "status": "paused"}],
                    release_slot=f"goal:{goal_id}:{old_owner}" if old_owner else None,
                )
            goal.update(
                status="running",
                execution_owner=owner,
                execution_lease_until=instant + lease_ms / 1_000,
                execution_fence=int(goal.get("execution_fence", 0)) + 1,
                updated_at=utc_now(),
            )
            return GoalMutation(
                goal,
                [{"type": "goal_status", "status": "running"}],
                [
                    {
                        "action": "goal.execute",
                        "status": "started",
                        "goal_id": goal_id,
                        "agent_id": goal.get("agent_id"),
                        "owner": owner,
                    }
                ],
                result=goal,
                release_slot=f"goal:{goal_id}:{old_owner}" if old_owner else None,
            )

        return await self._mutate(tenant, goal_id, change)

    async def renew(
        self,
        goal_id: str,
        tenant: str,
        owner: str,
        lease_ms: int,
        now: float | None = None,
    ) -> bool:
        instant = timestamp(now)

        def change(goal):
            if not self._owned(goal, owner, instant):
                return GoalMutation(result=False)
            goal["execution_lease_until"] = instant + lease_ms / 1_000
            return GoalMutation(goal, result=True)

        return await self._mutate(tenant, goal_id, change)

    async def renew_run_slot(self, tenant: str, slot_id: str, ttl_seconds: int) -> bool:
        """Refresh a live slot and retain the set until its latest member expires."""
        if self.client is None:
            adapter = getattr(self.runtime, "goal_execution_renew_run_slot", None)
            if adapter is None:
                raise RuntimeError("Goal quota heartbeat requires an atomic Redis store")
            return await adapter(tenant, slot_id, ttl_seconds)
        now = time.time()
        script = """
        if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
        redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
        local latest = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES')
        local ttl = math.max(1, math.ceil(tonumber(latest[2]) - tonumber(ARGV[2])))
        redis.call('EXPIRE', KEYS[1], ttl)
        return 1
        """
        return bool(
            await self.client.eval(
                script,
                1,
                f"tenant:{tenant}:quota:active-runs",
                slot_id,
                now,
                now + ttl_seconds,
            )
        )

    @staticmethod
    def _owned(goal: dict[str, Any] | None, owner: str, now: float | None = None) -> bool:
        return bool(
            goal
            and goal.get("execution_owner") == owner
            and float(goal.get("execution_lease_until", 0)) > timestamp(now)
            and goal.get("status") in {"running", "pausing"}
        )

    async def save_owned(
        self,
        goal: dict[str, Any],
        owner: str,
        event: dict[str, Any] | None = None,
    ) -> bool:
        def change(current):
            if not self._owned(current, owner):
                return GoalMutation(result=False)
            merged = {**goal, "execution_lease_until": current["execution_lease_until"]}
            if current["status"] == "pausing":
                merged["status"] = "pausing"
            merged["updated_at"] = utc_now()
            return GoalMutation(merged, [event] if event else [], result=True)

        return await self._mutate(goal.get("tenant_id", "default"), goal["goal_id"], change)

    async def finish(self, goal: dict[str, Any], owner: str, event: dict[str, Any]) -> bool:
        def change(current):
            if not self._owned(current, owner):
                return GoalMutation(result=False)
            merged = {
                **goal,
                "execution_owner": None,
                "execution_lease_until": 0,
                "updated_at": utc_now(),
            }
            # Pause wins a concurrent completion; resume can finalize the remaining boundary.
            if current["status"] == "pausing" and merged["status"] != "cancelled":
                merged["status"] = "paused"
                event_to_write = {"type": "goal_status", "status": "paused"}
            else:
                event_to_write = event
            audits = [
                {
                    "action": "goal.execute",
                    "status": merged["status"],
                    "goal_id": goal["goal_id"],
                    "agent_id": goal.get("agent_id"),
                }
            ]
            if merged["status"] == "waiting_approval":
                pending = merged.get("pending_approval", {})
                audits.append(
                    {
                        "action": "goal.approval",
                        "status": "requested",
                        "goal_id": goal["goal_id"],
                        "approval_id": pending.get("approval_id"),
                        "action_id": pending.get("action_id"),
                        "tool": pending.get("tool"),
                    }
                )
            return GoalMutation(
                merged,
                [event_to_write],
                audits,
                result=True,
            )

        return await self._mutate(goal.get("tenant_id", "default"), goal["goal_id"], change)

    async def control(
        self,
        tenant: str,
        goal_id: str,
        action: str,
        subject: str,
    ) -> dict[str, Any] | None:
        def change(goal):
            if not goal:
                return GoalMutation()
            status = goal["status"]
            if status in TERMINAL:
                raise ValueError("Goal is already terminal")
            if action == "cancel":
                goal.update(status="cancelled", execution_owner=None, execution_lease_until=0)
            elif action == "pause":
                if status == "waiting_approval":
                    raise ValueError("Resolve or cancel the pending approval before pausing")
                goal["status"] = "pausing" if status in {"running", "pausing"} else "paused"
            elif action == "resume":
                if status != "paused":
                    raise ValueError("Only paused goals can be resumed")
                goal.update(status="queued", execution_owner=None, execution_lease_until=0)
            else:
                raise ValueError("Unknown goal control")
            goal["updated_at"] = utc_now()
            return GoalMutation(
                goal,
                [{"type": "goal_status", "status": goal["status"]}],
                [
                    {
                        "action": f"goal.{action}",
                        "status": goal["status"],
                        "goal_id": goal_id,
                        "subject": subject,
                    }
                ],
                action == "resume",
                goal,
            )

        result = await self._mutate(tenant, goal_id, change)
        if result and result.get("status") == "cancelled":
            await self.reconcile_budget(result)
        return result

    async def decide_approval(
        self,
        tenant: str,
        goal_id: str,
        approval_id: str,
        approved: bool,
        reason: str,
        subject: str,
        now: float | datetime | None = None,
    ) -> dict[str, Any] | None:
        instant = timestamp(now)

        def change(goal):
            if not goal:
                return GoalMutation()
            pending = goal.get("pending_approval") or {}
            if (
                goal.get("status") != "waiting_approval"
                or pending.get("approval_id") != approval_id
            ):
                raise ValueError("Goal is not waiting for this approval")
            expires = pending.get("expires_at", 0)
            expiry = (
                datetime.fromisoformat(expires).timestamp() if isinstance(expires, str) else expires
            )
            expired = float(expiry) <= instant
            accepted = approved and not expired
            goal.update(
                status="queued" if accepted else "cancelled",
                execution_owner=None,
                execution_lease_until=0,
                updated_at=utc_now(),
                approval_decision={
                    "approval_id": approval_id,
                    "approved": accepted,
                    "reason": "approval_expired" if expired else reason,
                    "subject": subject,
                    "decided_at": utc_now(),
                },
            )
            event = {"type": "approval_decided", **goal["approval_decision"]}
            return GoalMutation(
                goal,
                [event, {"type": "goal_status", "status": goal["status"]}],
                [
                    {
                        "action": "goal.approval",
                        "status": "expired"
                        if expired
                        else ("approved" if accepted else "rejected"),
                        "goal_id": goal_id,
                        "approval_id": approval_id,
                        "subject": subject,
                        "reason": reason,
                    }
                ],
                accepted,
                goal,
            )

        result = await self._mutate(tenant, goal_id, change)
        if result and result.get("status") == "cancelled":
            await self.reconcile_budget(result)
        return result

    async def expire_approvals(self, now: float | datetime | None = None) -> int:
        instant = timestamp(now)
        count = 0
        for tenant in await self.runtime.list_goal_tenants():
            for goal_id in await self.runtime.list_goal_ids(tenant):
                goal = await self.runtime.get_goal(goal_id, tenant)
                pending = (goal or {}).get("pending_approval") or {}
                expires = pending.get("expires_at", 0)
                expiry = (
                    datetime.fromisoformat(expires).timestamp()
                    if isinstance(expires, str)
                    else float(expires)
                )
                if (goal or {}).get("status") == "waiting_approval" and expiry <= instant:
                    try:
                        await self.decide_approval(
                            tenant,
                            goal_id,
                            pending.get("approval_id", ""),
                            False,
                            "approval_expired",
                            "system",
                            instant,
                        )
                        count += 1
                    except ValueError:
                        pass  # A concurrent human decision already resolved it.
        return count

    async def recover_expired(self, now: float | None = None) -> int:
        instant = timestamp(now)
        recovered = 0
        for tenant in await self.runtime.list_goal_tenants():
            for goal_id in await self.runtime.list_goal_ids(tenant):

                def change(goal, goal_id=goal_id):
                    if (
                        not goal
                        or goal.get("status") not in {"running", "pausing"}
                        or float(goal.get("execution_lease_until", 0)) > instant
                    ):
                        return GoalMutation(result=False)
                    old_owner = goal.get("execution_owner")
                    goal.update(
                        status="paused" if goal["status"] == "pausing" else "queued",
                        execution_owner=None,
                        execution_lease_until=0,
                        updated_at=utc_now(),
                    )
                    return GoalMutation(
                        goal,
                        [{"type": "goal_recovered", "status": goal["status"]}],
                        [
                            {
                                "action": "goal.recover",
                                "status": goal["status"],
                                "goal_id": goal_id,
                                "subject": "system",
                                "prior_owner": old_owner,
                            }
                        ],
                        enqueue=goal["status"] == "queued",
                        result=True,
                        release_slot=f"goal:{goal_id}:{old_owner}" if old_owner else None,
                    )

                recovered += bool(await self._mutate(tenant, goal_id, change))
        await self.reconcile_terminals()
        return recovered

    async def reconcile_terminals(self) -> int:
        """Retry accounting if a process exited after committing terminal state.

        Reconciliation itself is a fenced transaction, so concurrent schedulers
        or repeated scans cannot refund the same reservation twice. The return
        value counts candidates inspected, not a monetary/token refund amount.
        """
        candidates = 0
        for tenant in await self.runtime.list_goal_tenants():
            for goal_id in await self.runtime.list_goal_ids(tenant):
                goal = await self.runtime.get_goal(goal_id, tenant)
                reservation = (goal or {}).get("token_reservation") or {}
                if (
                    (goal or {}).get("status") in TERMINAL
                    and reservation
                    and not reservation.get("reconciled")
                ):
                    await self.reconcile_budget(goal)
                    candidates += 1
        return candidates

    async def reserve_budget(
        self,
        goal: dict[str, Any],
        owner: str,
        limit: int,
        usage: dict[str, Any] | None = None,
    ) -> None:
        """Reserve the entire bounded job budget once, including approval resumes."""
        if self.client is None:
            adapter = getattr(self.runtime, "goal_execution_reserve_budget", None)
            if adapter is None:
                raise RuntimeError("Goal quota requires an atomic Redis store")
            await adapter(goal, owner, limit)
            return
        tenant = goal.get("tenant_id", "default")
        key = self.key(tenant, goal["goal_id"])
        day = datetime.now(UTC).strftime("%Y-%m-%d")
        quota_key = f"tenant:{tenant}:quota:tokens:{day}"
        budget = int(goal.get("policy", {}).get("token_budget", 20_000))
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    current = json.loads(await pipe.get(key) or "null")
                    if not self._owned(current, owner):
                        raise RuntimeError("Goal execution ownership lost")
                    prior = current.get("token_reservation")
                    charged = self._charged(current.get("usage", {}))
                    reservation = dict(
                        prior
                        or {
                            "day": day,
                            "reserved": budget,
                            "base_usage": 0,
                            "reconciled": False,
                        }
                    )
                    adjustments: dict[str, int] = {}
                    if not prior:
                        adjustments[quota_key] = budget
                    elif prior["day"] != day:
                        old_key = f"tenant:{tenant}:quota:tokens:{prior['day']}"
                        adjustments[old_key] = max(
                            0, charged - int(prior.get("base_usage", 0))
                        ) - int(prior["reserved"])
                        remaining = max(0, budget - charged)
                        adjustments[quota_key] = remaining
                        reservation = {
                            "day": day,
                            "reserved": remaining,
                            "base_usage": charged,
                            "reconciled": False,
                            "carried_from_day": prior["day"],
                        }
                    # A call can start before midnight and finish afterwards. Its
                    # pre-call conservative charge belongs to the old day; if the
                    # provider later lowers it, correct both days' reservation.
                    actual = self._charged(usage) if usage is not None else charged
                    base = int(reservation.get("base_usage", 0))
                    if usage is not None and actual < base:
                        correction = base - actual
                        previous_day = reservation.get("carried_from_day")
                        if previous_day:
                            old_key = f"tenant:{tenant}:quota:tokens:{previous_day}"
                            adjustments[old_key] = adjustments.get(old_key, 0) - correction
                        reservation["base_usage"] = actual
                        reservation["reserved"] += correction
                        adjustments[quota_key] = adjustments.get(quota_key, 0) + correction
                    if prior == reservation:
                        goal["token_reservation"] = reservation
                        return
                    await pipe.watch(*adjustments)
                    balances = {q: await pipe.get(q) for q in adjustments}
                    consumed = int(balances.get(quota_key) or 0)
                    if consumed + adjustments.get(quota_key, 0) > limit:
                        raise RuntimeError("tenant daily token budget exceeded")
                    current["token_reservation"] = reservation
                    pipe.multi()
                    for q, delta in adjustments.items():
                        if delta >= 0 or balances[q] is not None:
                            pipe.incrby(q, delta)
                            pipe.expire(q, 172_800)
                    pipe.set(key, json.dumps(current, ensure_ascii=False))
                    await pipe.execute()
                    goal["token_reservation"] = reservation
                    return
                except WatchError:
                    continue

    @staticmethod
    def _charged(usage: dict[str, Any]) -> int:
        return max(int(usage.get("total_tokens", 0)), int(usage.get("policy_charged_tokens", 0)))

    async def resize_budget(self, goal: dict[str, Any], owner: str, new_budget: int) -> None:
        """Reduce a Chief's reservation before bounded child Goals reserve their slices.

        The reservation and persisted policy change together. Retrying the same
        resize is a no-op, so a crash cannot refund the same tokens twice.
        """
        if self.client is None:
            adapter = getattr(self.runtime, "goal_execution_resize_budget", None)
            if adapter is None:
                raise RuntimeError("Goal budget resizing requires an atomic Redis store")
            await adapter(goal, owner, new_budget)
            return
        tenant = goal.get("tenant_id", "default")
        key = self.key(tenant, goal["goal_id"])
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    current = json.loads(await pipe.get(key) or "null")
                    if not self._owned(current, owner):
                        raise RuntimeError("Goal execution ownership lost")
                    current_budget = int(current.get("policy", {}).get("token_budget", 20_000))
                    charged = self._charged(current.get("usage", {}))
                    if not charged <= new_budget <= current_budget or new_budget < 1:
                        raise ValueError("Chief budget may only shrink and must cover spent tokens")
                    reservation = dict(current.get("token_reservation") or {})
                    quota_key, delta = None, 0
                    if reservation:
                        quota_key = f"tenant:{tenant}:quota:tokens:{reservation['day']}"
                        await pipe.watch(quota_key)
                        target = max(0, new_budget - int(reservation.get("base_usage", 0)))
                        delta = target - int(reservation["reserved"])
                        reservation["reserved"] = target
                    current["policy"] = {**current.get("policy", {}), "token_budget": new_budget}
                    current["token_budget"] = new_budget
                    current["chief_budget"] = new_budget
                    current["token_reservation"] = reservation
                    exists = bool(quota_key and await pipe.exists(quota_key))
                    pipe.multi()
                    if exists and delta:
                        pipe.incrby(quota_key, delta)
                    pipe.set(key, json.dumps(current, ensure_ascii=False))
                    if new_budget != current_budget:
                        pipe.xadd(
                            f"tenant:{tenant}:audit",
                            {
                                "payload": json.dumps(
                                    {
                                        "action": "goal.budget.resize",
                                        "goal_id": goal["goal_id"],
                                        "old_budget": current_budget,
                                        "token_budget": new_budget,
                                        "subject": goal.get("agent_id"),
                                        "time": utc_now(),
                                    }
                                )
                            },
                        )
                    await pipe.execute()
                    goal.update(
                        policy=current["policy"],
                        token_budget=new_budget,
                        chief_budget=new_budget,
                        token_reservation=reservation,
                    )
                    return
                except WatchError:
                    continue

    async def reconcile_budget(self, goal: dict[str, Any], owner: str = "") -> None:
        del owner  # Reconciliation is terminal/idempotent, independent of former lease ownership.
        if self.client is None:
            adapter = getattr(self.runtime, "goal_execution_reconcile_budget", None)
            if adapter:
                await adapter(goal)
            return
        tenant = goal.get("tenant_id", "default")
        key = self.key(tenant, goal["goal_id"])
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    current = json.loads(await pipe.get(key) or "null")
                    reservation = (current or {}).get("token_reservation")
                    if (
                        not reservation
                        or reservation.get("reconciled")
                        or current.get("status") not in TERMINAL
                    ):
                        return
                    usage = current.get("usage", {})
                    actual = max(0, self._charged(usage) - int(reservation.get("base_usage", 0)))
                    delta = actual - int(reservation["reserved"])
                    quota_key = f"tenant:{tenant}:quota:tokens:{reservation['day']}"
                    await pipe.watch(quota_key)
                    quota_exists = await pipe.exists(quota_key)
                    reservation["reconciled"] = True
                    pipe.multi()
                    if quota_exists:
                        pipe.incrby(quota_key, delta)
                        pipe.expire(quota_key, 172_800)
                    pipe.set(key, json.dumps(current, ensure_ascii=False))
                    await pipe.execute()
                    return
                except WatchError:
                    continue
