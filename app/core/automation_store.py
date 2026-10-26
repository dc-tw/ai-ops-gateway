"""Atomic automation dispatch on the existing, single logical Redis instance.

Occurrence markers expire after 30 days. They prevent duplicate dispatch inside
that window; external tools still need their own idempotency for side effects.
The queue is intentionally untrimmed so a backlog cannot discard pending work.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from redis.exceptions import WatchError

from app.core.redis_runtime import RedisRuntime

OCCURRENCE_TTL_SECONDS = 30 * 86_400
_SAFE_EVENT_FIELDS = {
    "event_id",
    "event_type",
    "source",
    "trigger_type",
    "scheduled_at",
    "causation_id",
    "status",
    "goal_id",
    "coalesced_count",
    "time",
    "count",
    "type",
    "http_status",
    "approval_id",
    "schedule_due_at",
    "schedule_next_at",
}

_DISPATCH = """
local jobraw = redis.call('GET', KEYS[1])
if not jobraw then return cjson.encode({status='disabled', goal_id=cjson.null}) end
local job = cjson.decode(jobraw)
local mode = ARGV[1]
if not job.enabled then
  redis.call('DEL', KEYS[3])
  return cjson.encode({status='disabled', goal_id=cjson.null})
end
if mode == 'submit' then
  local seen = redis.call('GET', KEYS[4])
  if seen then
    return cjson.encode({status='duplicate', goal_id=seen ~= '' and seen or cjson.null})
  end
  local submitted = cjson.decode(ARGV[2])
  if submitted.automation_version ~= job.version then
    return cjson.encode({status='stale', goal_id=cjson.null})
  end
  local event = cjson.decode(ARGV[3])
  if event.trigger_type == 'schedule' and event.schedule_due_at then
    if job.trigger.kind ~= 'schedule'
      or tonumber(job.next_run_at) ~= tonumber(event.schedule_due_at)
      or not event.schedule_next_at
      or tonumber(event.schedule_next_at) <= tonumber(event.schedule_due_at) then
      return cjson.encode({status='stale', goal_id=cjson.null})
    end
  end
end
local active = redis.call('GET', KEYS[2])
local busy = false
if active then
  local raw = redis.call('GET', ARGV[7] .. 'agent:goal:' .. active)
  if raw then
    local status = cjson.decode(raw).status
    busy = not (status == 'completed' or status == 'failed' or status == 'cancelled'
      or status == 'rejected' or status == 'timed_out')
  end
end
local pendingraw = redis.call('GET', KEYS[3])
local pending = pendingraw and cjson.decode(pendingraw) or nil
local item
if mode == 'flush' then
  if not pending then return cjson.encode({status='no_pending', goal_id=cjson.null}) end
  item = pending
else
  item = {goal_json=ARGV[2], event=cjson.decode(ARGV[3]), count=tonumber(ARGV[4])}
  if pending then item.count = item.count + pending.count end
end
local function record(status, goalid, count, reason, skillrefs)
  local event = {type='automation_' .. status,
    automation_id=job.automation_id, event_id=item.event.event_id,
    goal_id=goalid, status=status, coalesced_count=count, time=ARGV[5], reason=reason,
    skills=skillrefs}
  redis.call('XADD', KEYS[7], '*', 'payload', cjson.encode(event))
  local audit = {action='automation.' .. status, automation_id=job.automation_id,
    event_id=item.event.event_id, goal_id=goalid, status=status,
    coalesced_count=count, time=ARGV[5], agent_id=job.agent_id, reason=reason,
    skills=skillrefs}
  redis.call('XADD', ARGV[7] .. 'audit', '*', 'payload', cjson.encode(audit))
end
local function accept_schedule()
  if mode == 'submit' and item.event.trigger_type == 'schedule'
      and item.event.schedule_due_at then
    -- The pre-encoded configuration preserves empty arrays that Lua cjson
    -- would otherwise rewrite as objects. Version + due guards above fence it.
    redis.call('SET', KEYS[1], ARGV[10])
    redis.call('ZADD', KEYS[9], item.event.schedule_next_at, ARGV[9])
  end
end
local function decode_record(key)
  local raw = redis.call('GET', key)
  if not raw then return nil end
  local ok, value = pcall(cjson.decode, raw)
  if not ok or type(value) ~= 'table' then return nil end
  return value
end
local function check_skills(goal)
  if type(goal) ~= 'table' then return false, 'invalid_snapshot', nil end
  local config = goal.agent_config
  if config == nil then return true, nil, nil end
  if type(config) ~= 'table' then return false, 'invalid_snapshot', nil end
  local snapshots, bindings = config.skill_snapshots or {}, config.skills or {}
  if type(snapshots) ~= 'table' or type(bindings) ~= 'table' then
    return false, 'invalid_snapshot', nil
  end
  if #snapshots ~= #bindings then return false, 'invalid_binding', nil end
  if #snapshots == 0 then
    if next(snapshots) or next(bindings) then return false, 'invalid_binding', nil end
    return true, nil, nil
  end
  if ARGV[7] ~= 'tenant:' .. job.tenant_id .. ':' then
    return false, 'invalid_tenant', nil
  end
  local seen, refs = {}, {}
  for index, snapshot in ipairs(snapshots) do
    local binding = bindings[index]
    if type(snapshot) ~= 'table' or type(binding) ~= 'table'
      or type(snapshot.skill_id) ~= 'string' or snapshot.skill_id == ''
      or type(snapshot.revision) ~= 'number' or snapshot.revision < 1
      or snapshot.revision ~= math.floor(snapshot.revision)
      or type(snapshot.revocation_epoch) ~= 'number' or snapshot.revocation_epoch < 0
      or snapshot.revocation_epoch ~= math.floor(snapshot.revocation_epoch)
      or type(snapshot.content_sha256) ~= 'string'
      or snapshot.tenant_id ~= job.tenant_id
      or binding.skill_id ~= snapshot.skill_id or binding.revision ~= snapshot.revision
      or seen[snapshot.skill_id] then
      return false, 'invalid_binding', refs
    end
    seen[snapshot.skill_id] = true
    table.insert(refs, {skill_id=snapshot.skill_id, revision=snapshot.revision,
      content_sha256=snapshot.content_sha256, revocation_epoch=snapshot.revocation_epoch})
    local skill = decode_record(ARGV[7] .. 'skills:record:' .. snapshot.skill_id)
    if not skill or skill.tenant_id ~= job.tenant_id or skill.skill_id ~= snapshot.skill_id
      or skill.enabled ~= true or skill.revocation_epoch ~= snapshot.revocation_epoch then
      return false, 'skill_revoked', refs
    end
    local revision = decode_record(ARGV[7] .. 'skills:revision:' .. snapshot.skill_id
      .. ':' .. tostring(snapshot.revision))
    if not revision or revision.tenant_id ~= job.tenant_id
      or revision.skill_id ~= snapshot.skill_id or revision.revision ~= snapshot.revision
      or revision.content_sha256 ~= snapshot.content_sha256 then
      return false, 'revision_unavailable', refs
    end
  end
  return true, nil, refs
end
local goal = cjson.decode(item.goal_json)
local valid_skills, skill_reason, skillrefs = check_skills(goal)
if not valid_skills then
  -- Invalid pinned work cannot become runnable after a later re-enable. Drop
  -- revoked pending work even while another Goal is active. A rejected new
  -- occurrence must not discard an older pending Goal whose Skills remain valid.
  if mode == 'submit' and pending and check_skills(cjson.decode(pending.goal_json)) then
    item.count = tonumber(ARGV[4])
  else
    redis.call('DEL', KEYS[3])
  end
  if mode == 'submit' then redis.call('SET', KEYS[4], '', 'EX', ARGV[8]) end
  record('skills_revoked', cjson.null, item.count, skill_reason, skillrefs)
  accept_schedule()
  return cjson.encode({status='skills_revoked', goal_id=cjson.null, reason=skill_reason})
end
if mode == 'flush' and busy then return cjson.encode({status='busy', goal_id=active}) end
if busy then
  redis.call('SET', KEYS[3], cjson.encode(item))
  redis.call('SET', KEYS[4], active, 'EX', ARGV[8])
  record('coalesced', active, item.count, nil, skillrefs)
  accept_schedule()
  return cjson.encode({status='coalesced', goal_id=active, coalesced_count=item.count})
end
local cap = tonumber(job.policy.max_runs_per_day or 24)
if tonumber(redis.call('GET', KEYS[5]) or '0') >= cap then
  -- Quota exhaustion defers accepted work in the same single pending slot.
  -- Mark it seen so a provider retry cannot inflate the coalesced count.
  redis.call('SET', KEYS[3], cjson.encode(item))
  if mode == 'submit' then redis.call('SET', KEYS[4], '', 'EX', ARGV[8]) end
  record('rate_limited', cjson.null, item.count, nil, skillrefs)
  accept_schedule()
  return cjson.encode({status='rate_limited', goal_id=cjson.null, coalesced_count=item.count})
end
-- Keep the original JSON opaque: Redis Lua cjson turns empty arrays into objects.
local goalraw = string.sub(item.goal_json, 1, -2) .. ',"automation_id":'
  .. cjson.encode(job.automation_id) .. ',"tenant_id":' .. cjson.encode(job.tenant_id)
  .. ',"coalesced_count":' .. tostring(item.count)
  .. ',"trigger_metadata":' .. cjson.encode(item.event) .. '}'
local goalkey = ARGV[7] .. 'agent:goal:' .. goal.goal_id
if redis.call('EXISTS', goalkey) == 1 then
  return redis.error_reply('automation goal_id already exists')
end
redis.call('SET', goalkey, goalraw)
redis.call('SADD', ARGV[7] .. 'agent:goals', goal.goal_id)
redis.call('SADD', 'agent:goal:tenants', job.tenant_id)
redis.call('ZADD', KEYS[8], ARGV[6], goal.goal_id)
redis.call('XADD', KEYS[6], '*', 'goal_id', goal.goal_id, 'tenant_id', job.tenant_id)
redis.call('SET', KEYS[2], goal.goal_id)
redis.call('DEL', KEYS[3])
redis.call('INCR', KEYS[5])
redis.call('EXPIRE', KEYS[5], 259200)
if mode == 'submit' then
  redis.call('SET', KEYS[4], goal.goal_id, 'EX', ARGV[8])
end
local queued = {type='goal_queued', goal_id=goal.goal_id,
  automation_id=job.automation_id, coalesced_count=item.count, time=ARGV[5]}
redis.call('XADD', ARGV[7] .. 'agent:goal:events:' .. goal.goal_id,
  '*', 'payload', cjson.encode(queued))
record('queued', goal.goal_id, item.count, nil, skillrefs)
accept_schedule()
return cjson.encode({status='queued', goal_id=goal.goal_id, coalesced_count=item.count})
"""


class AutomationStore:
    def __init__(self, runtime: RedisRuntime) -> None:
        self.runtime = runtime
        self._client = runtime._client

    def _key(self, tenant_id: str, automation_id: str, suffix: str = "") -> str:
        return self.runtime._tenant_key(tenant_id, f"automation:{automation_id}{suffix}")

    @staticmethod
    def _member(tenant_id: str, automation_id: str) -> str:
        return json.dumps([tenant_id, automation_id], separators=(",", ":"))

    @staticmethod
    def _safe_event(event: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in event.items()
            if key in _SAFE_EVENT_FIELDS and isinstance(value, (str, int, float, bool))
        }

    async def create(self, job: dict[str, Any]) -> dict[str, Any]:
        job = deepcopy(job)
        job.setdefault("version", 1)
        job.setdefault("next_run_at", None)
        tenant_id, automation_id = job["tenant_id"], job["automation_id"]
        member = self._member(tenant_id, automation_id)
        script = """
        if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
        redis.call('SET', KEYS[1], ARGV[1])
        redis.call('SADD', KEYS[2], ARGV[2])
        redis.call('SADD', KEYS[3], ARGV[3])
        if ARGV[5] ~= '' then redis.call('ZADD', KEYS[4], ARGV[5], ARGV[4]) end
        return 1
        """
        next_at = job.get("next_run_at") if job.get("enabled") else None
        created = await self._client.eval(
            script,
            4,
            self._key(tenant_id, automation_id),
            self.runtime._tenant_key(tenant_id, "automations"),
            "automation:tenants",
            "automation:due",
            json.dumps(job, ensure_ascii=False),
            automation_id,
            tenant_id,
            member,
            "" if next_at is None else str(next_at),
        )
        if not created:
            raise ValueError("Automation already exists")
        return job

    async def get(self, tenant_id: str, automation_id: str) -> dict[str, Any] | None:
        raw = await self._client.get(self._key(tenant_id, automation_id))
        return json.loads(raw) if raw else None

    async def list(self, tenant_id: str) -> list[dict[str, Any]]:
        index = self.runtime._tenant_key(tenant_id, "automations")
        ids = sorted(await self._client.smembers(index))
        if not ids:
            return []
        rows = await self._client.mget([self._key(tenant_id, item) for item in ids])
        return sorted(
            [json.loads(row) for row in rows if row], key=lambda row: row.get("created_at", "")
        )

    async def tenants(self) -> list[str]:
        return sorted(await self._client.smembers("automation:tenants"))

    async def update(self, job: dict[str, Any], expected_version: int) -> dict[str, Any] | None:
        """Replace validated configuration, rejecting stale edits atomically.

        Pending work was approved against the previous template/policy and is
        discarded with an audit entry. Already dispatched Goals retain their
        immutable policy snapshot and remain subject to Goal cancellation.
        """
        job = deepcopy(job)
        tenant_id, automation_id = job["tenant_id"], job["automation_id"]
        key = self._key(tenant_id, automation_id)
        pending_key = self._key(tenant_id, automation_id, ":pending")
        while True:
            async with self._client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key, pending_key)
                    raw = await pipe.get(key)
                    if not raw:
                        return None
                    current = json.loads(raw)
                    if current.get("version", 1) != expected_version:
                        return None
                    pendingraw = await pipe.get(pending_key)
                    pending_count = json.loads(pendingraw)["count"] if pendingraw else 0
                    job["version"] = expected_version + 1
                    for immutable in ("created_by", "created_at"):
                        job[immutable] = current[immutable]
                    if not job["enabled"]:
                        job["next_run_at"] = None
                    event = {
                        "automation_id": automation_id,
                        "status": "updated",
                        "time": job["updated_at"],
                        "coalesced_count": pending_count,
                        "version": job["version"],
                        "pending_discarded": bool(pending_count),
                    }
                    pipe.multi()
                    pipe.set(key, json.dumps(job, ensure_ascii=False))
                    pipe.delete(pending_key)
                    member = self._member(tenant_id, automation_id)
                    next_at = job.get("next_run_at")
                    if next_at is None:
                        pipe.zrem("automation:due", member)
                    else:
                        pipe.zadd("automation:due", {member: next_at})
                    pipe.xadd(
                        self._key(tenant_id, automation_id, ":events"),
                        {"payload": json.dumps(event, ensure_ascii=False)},
                    )
                    pipe.xadd(
                        self.runtime._tenant_key(tenant_id, "audit"),
                        {"payload": json.dumps({**event, "action": "automation.update"})},
                    )
                    await pipe.execute()
                    return job
                except WatchError:
                    continue

    async def set_enabled(
        self, tenant_id: str, automation_id: str, enabled: bool, now: str
    ) -> dict[str, Any] | None:
        key = self._key(tenant_id, automation_id)
        while True:
            async with self._client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if not raw:
                        return None
                    job = json.loads(raw)
                    if job["enabled"] == enabled:
                        return job
                    job.update(enabled=enabled, updated_at=now, version=job.get("version", 1) + 1)
                    if not enabled:
                        job["next_run_at"] = None
                    pipe.multi()
                    pipe.set(key, json.dumps(job, ensure_ascii=False))
                    if not enabled:
                        pipe.delete(self._key(tenant_id, automation_id, ":pending"))
                        pipe.zrem("automation:due", self._member(tenant_id, automation_id))
                    await pipe.execute()
                    return job
                except WatchError:
                    continue

    async def due(self, now: float, limit: int = 100) -> list[tuple[str, str]]:
        rows = await self._client.zrangebyscore("automation:due", "-inf", now, start=0, num=limit)
        return [tuple(json.loads(row)) for row in rows]

    async def _update_next(
        self,
        tenant_id: str,
        automation_id: str,
        next_at: float | None,
        expected: float | None = None,
        expected_version: int | None = None,
    ) -> bool:
        key = self._key(tenant_id, automation_id)
        while True:
            async with self._client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if not raw:
                        return False
                    job = json.loads(raw)
                    if (
                        not job["enabled"]
                        or (expected is not None and job.get("next_run_at") != expected)
                        or (expected_version is not None and job.get("version") != expected_version)
                    ):
                        return False
                    job["next_run_at"] = next_at
                    pipe.multi()
                    pipe.set(key, json.dumps(job, ensure_ascii=False))
                    member = self._member(tenant_id, automation_id)
                    if next_at is None:
                        pipe.zrem("automation:due", member)
                    else:
                        pipe.zadd("automation:due", {member: next_at})
                    await pipe.execute()
                    return True
                except WatchError:
                    continue

    async def advance_schedule(
        self,
        tenant_id: str,
        automation_id: str,
        expected: float,
        next_at: float,
        expected_version: int | None = None,
    ) -> bool:
        return await self._update_next(
            tenant_id, automation_id, next_at, expected, expected_version
        )

    async def set_next_run(
        self,
        tenant_id: str,
        automation_id: str,
        next_at: float | None,
        expected_version: int | None = None,
    ) -> None:
        await self._update_next(
            tenant_id, automation_id, next_at, expected_version=expected_version
        )

    async def _dispatch(
        self,
        tenant_id: str,
        automation_id: str,
        *,
        mode: str,
        now: float,
        occurrence_id: str = "",
        goal: dict[str, Any] | None = None,
        event: dict[str, Any] | None = None,
        coalesced_count: int = 1,
    ) -> dict[str, Any]:
        if mode == "submit":
            from app.core.skill_store import validate_skill_snapshots

            config = (goal or {}).get("agent_config") or {}
            validate_skill_snapshots(
                config.get("skill_snapshots", []),
                config.get("skills", []),
                (goal or {}).get("policy") or {},
                config.get("role", "worker"),
            )
        job = await self.get(tenant_id, automation_id)
        timezone = ZoneInfo((job or {}).get("trigger", {}).get("timezone", "Asia/Taipei"))
        local_day = datetime.fromtimestamp(now, timezone).strftime("%Y-%m-%d")
        digest = hashlib.sha256(occurrence_id.encode()).hexdigest()
        prepared_goal = {
            key: value
            for key, value in (goal or {}).items()
            if key not in {"automation_id", "tenant_id", "coalesced_count", "trigger_metadata"}
        }
        safe_event = self._safe_event(event or {})
        advanced_job = deepcopy(job or {})
        if safe_event.get("trigger_type") == "schedule" and "schedule_due_at" in safe_event:
            advanced_job["next_run_at"] = safe_event.get("schedule_next_at")
        raw = await self._client.eval(
            _DISPATCH,
            9,
            self._key(tenant_id, automation_id),
            self._key(tenant_id, automation_id, ":active"),
            self._key(tenant_id, automation_id, ":pending"),
            self._key(tenant_id, automation_id, f":occurrence:{digest}"),
            self._key(tenant_id, automation_id, f":runs:{local_day}"),
            "agent:goal:queue",
            self._key(tenant_id, automation_id, ":events"),
            self._key(tenant_id, automation_id, ":goals"),
            "automation:due",
            mode,
            json.dumps(prepared_goal, ensure_ascii=False),
            json.dumps(safe_event, ensure_ascii=False),
            max(1, coalesced_count),
            datetime.fromtimestamp(now, UTC).isoformat(),
            now,
            self.runtime._tenant_key(tenant_id, ""),
            OCCURRENCE_TTL_SECONDS,
            self._member(tenant_id, automation_id),
            json.dumps(advanced_job, ensure_ascii=False),
        )
        return json.loads(raw)

    async def submit(
        self,
        tenant_id: str,
        automation_id: str,
        occurrence_id: str,
        goal: dict[str, Any],
        event: dict[str, Any],
        now: float,
        coalesced_count: int = 1,
    ) -> dict[str, Any]:
        """Accept one occurrence, or report a duplicate/disabled/stale job.

        ``rate_limited`` is durable deferral: the latest occurrence remains in
        the pending slot until a future daily budget permits dispatch.
        """
        if not occurrence_id:
            raise ValueError("Occurrence ID must not be empty")
        if not goal.get("goal_id") or goal.get("status") != "queued":
            raise ValueError("Automation submission requires a queued Goal with an ID")
        return await self._dispatch(
            tenant_id,
            automation_id,
            mode="submit",
            now=now,
            occurrence_id=occurrence_id,
            goal=goal,
            event=event,
            coalesced_count=coalesced_count,
        )

    async def flush_pending(self, tenant_id: str, automation_id: str, now: float) -> dict[str, Any]:
        return await self._dispatch(tenant_id, automation_id, mode="flush", now=now)

    async def append_event(self, tenant_id: str, automation_id: str, event: dict[str, Any]) -> str:
        safe = {**self._safe_event(event), "automation_id": automation_id}
        return str(
            await self._client.xadd(
                self._key(tenant_id, automation_id, ":events"),
                {"payload": json.dumps(safe, ensure_ascii=False)},
            )
        )

    async def events(
        self, tenant_id: str, automation_id: str, last_id: str = "0-0", block_ms: int = 10_000
    ) -> list[tuple[str, dict[str, Any]]]:
        rows = await self._client.xread(
            {self._key(tenant_id, automation_id, ":events"): last_id},
            count=100,
            block=block_ms if block_ms > 0 else None,
        )
        return [
            (event_id, json.loads(fields["payload"]))
            for _, items in rows
            for event_id, fields in items
        ]

    async def list_goals(self, tenant_id: str, automation_id: str) -> list[dict[str, Any]]:
        ids = await self._client.zrange(self._key(tenant_id, automation_id, ":goals"), 0, -1)
        result = [await self.runtime.get_goal(goal_id, tenant_id) for goal_id in ids]
        return [goal for goal in result if goal is not None]
