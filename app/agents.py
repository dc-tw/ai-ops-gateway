"""Tenant-scoped Agent personas and server-owned delegated execution policy."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import uuid4
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field, model_validator
from redis.exceptions import WatchError

from app.api.schemas import GoalCreateRequest
from app.automations import AutomationPolicy, validate_goal_input
from app.core.goal_execution import GoalExecutionStore
from app.core.resource_access import filter_resources, guard_owner, queue_owner
from app.core.security import authorize, redact_pii
from app.core.skill_selection import SkillSelectionRule, select_skills, selection_candidates
from app.core.skill_store import SkillBinding, resolve_skill_bindings, skill_refs


class AgentRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def handle(request):
            try:
                return await original(request)
            except RequestValidationError:
                return JSONResponse(status_code=422, content={"detail": "Invalid Agent request"})

        return handle


router = APIRouter(prefix="/v1/agents", tags=["agents"], route_class=AgentRoute)


class AgentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=2_000)
    persona: str = Field(min_length=1, max_length=8_000)
    role: Literal["chief", "worker"] = "worker"
    model: str | None = Field(default=None, min_length=1, max_length=200)
    policy: AutomationPolicy = Field(default_factory=AutomationPolicy)
    enabled: bool = True
    delegate_agent_ids: list[str] = Field(default_factory=list, max_length=8)
    skills: list[SkillBinding] = Field(default_factory=list, max_length=8)
    planning_skills: list[SkillBinding] = Field(default_factory=list, max_length=8)
    skill_selection_rules: list[SkillSelectionRule] = Field(default_factory=list, max_length=8)

    @model_validator(mode="after")
    def finite_delegation(self) -> AgentCreate:
        if len({binding.skill_id for binding in self.skills}) != len(self.skills):
            raise ValueError("An Agent cannot bind the same Skill more than once")
        if len({binding.skill_id for binding in self.planning_skills}) != len(self.planning_skills):
            raise ValueError("An Agent cannot bind a planning Skill more than once")
        selection_candidates(self.skills, self.skill_selection_rules)
        self.delegate_agent_ids = list(dict.fromkeys(self.delegate_agent_ids))
        if any(not item or len(item) > 128 for item in self.delegate_agent_ids):
            raise ValueError("Delegate Agent IDs must contain 1-128 characters")
        if self.role == "worker" and self.delegate_agent_ids:
            raise ValueError("Only Chief Agents can delegate")
        if self.role == "chief":
            if "allowed_tools" not in self.policy.model_fields_set:
                self.policy.allowed_tools = []
            if self.policy.allowed_tools:
                raise ValueError(
                    "Chief Agents plan and aggregate; their allowed_tools must be empty"
                )
        return self


class AgentUpdate(AgentCreate):
    expected_version: int = Field(ge=1)


class AgentEnabled(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool


class AgentStore:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self._client = runtime._client

    def _key(self, tenant: str, agent_id: str) -> str:
        return self.runtime._tenant_key(tenant, f"agents:record:{agent_id}")

    async def get(self, tenant: str, agent_id: str) -> dict[str, Any] | None:
        raw = await self._client.get(self._key(tenant, agent_id))
        return json.loads(raw) if raw else None

    async def admit_goal(self, tenant: str, agent_id: str, goal_id: str, owner: str) -> bool:
        """Count a Goal once against its Agent's Taipei calendar-day cap.

        Approval resumes and owner recovery retain the durable admission marker;
        a new Goal still requires its own admission, across every trigger type.
        """
        agent_key = self._key(tenant, agent_id)
        goal_key = GoalExecutionStore.key(tenant, goal_id)
        while True:
            now = datetime.now(UTC)
            local_day = now.astimezone(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d")
            counter = self.runtime._tenant_key(tenant, f"agents:daily-runs:{agent_id}:{local_day}")
            async with self._client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(agent_key, goal_key, counter)
                    agent_raw, goal_raw, count_raw = await pipe.mget([agent_key, goal_key, counter])
                    agent = json.loads(agent_raw) if agent_raw else None
                    goal = json.loads(goal_raw) if goal_raw else None
                    if (
                        not agent
                        or not agent.get("enabled")
                        or agent.get("tenant_id") != tenant
                        or not GoalExecutionStore._owned(goal, owner)
                        or goal.get("agent_id") != agent_id
                        or goal.get("tenant_id") != tenant
                    ):
                        return False
                    if goal.get("agent_admitted") is True:
                        return True
                    limit = int(agent.get("policy", {}).get("max_runs_per_day", 24))
                    if int(count_raw or 0) >= limit:
                        return False
                    goal.update(
                        agent_admitted=True,
                        agent_admitted_at=now.isoformat(),
                        agent_admitted_day=local_day,
                    )
                    pipe.multi()
                    pipe.incr(counter)
                    pipe.expire(counter, 3 * 86_400)
                    pipe.set(goal_key, json.dumps(goal, ensure_ascii=False))
                    await pipe.execute()
                    return True
                except WatchError:
                    continue

    async def list(self, tenant: str) -> list[dict[str, Any]]:
        ids = sorted(await self._client.smembers(self.runtime._tenant_key(tenant, "agents")))
        if not ids:
            return []
        rows = await self._client.mget([self._key(tenant, item) for item in ids])
        return sorted(
            [json.loads(row) for row in rows if row], key=lambda row: row.get("created_at", "")
        )

    @staticmethod
    def _check_delegates(record: dict[str, Any], rows: list[Any]) -> None:
        delegates = record.get("delegate_agent_ids", [])
        if record["role"] != "chief" and delegates:
            raise ValueError("Worker Agents cannot delegate")
        if record["role"] == "chief" and record["policy"].get("allowed_tools"):
            raise ValueError("Chief Agents cannot execute tools directly")
        if len(delegates) > 8 or record["agent_id"] in delegates:
            raise ValueError("Chief delegation is bounded to eight distinct worker Agents")
        for raw in rows:
            delegate = json.loads(raw) if raw else None
            if not delegate or not delegate["enabled"] or delegate["role"] != "worker":
                raise ValueError("Every delegate must be an enabled worker in the same tenant")
            if delegate["tenant_id"] != record["tenant_id"]:
                raise ValueError("Delegate tenant does not match")

    async def _write(
        self, record: dict[str, Any], expected_version: int | None
    ) -> dict[str, Any] | None:
        record = deepcopy(record)
        tenant, agent_id = record["tenant_id"], record["agent_id"]
        key = self._key(tenant, agent_id)
        delegate_keys = [self._key(tenant, item) for item in record.get("delegate_agent_ids", [])]
        while True:
            async with self._client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key, *delegate_keys)
                    current_raw = await pipe.get(key)
                    current = json.loads(current_raw) if current_raw else None
                    if expected_version is None:
                        if current:
                            raise ValueError("Agent already exists")
                        record["version"] = 1
                    else:
                        if not current or current["version"] != expected_version:
                            return None
                        record["version"] = expected_version + 1
                        for field in ("created_by", "created_at"):
                            record[field] = current[field]
                    rows = await pipe.mget(delegate_keys) if delegate_keys else []
                    self._check_delegates(record, rows)
                    skill_snapshots = []
                    candidates = selection_candidates(
                        record.get("skills", []), record.get("skill_selection_rules", [])
                    )
                    if candidates:
                        skill_snapshots = await resolve_skill_bindings(
                            self.runtime,
                            tenant,
                            candidates,
                            record["policy"],
                            record["role"],
                            client=pipe,
                            watch=True,
                        )
                    planning_snapshots = []
                    if record.get("planning_skills"):
                        planning_snapshots = await resolve_skill_bindings(
                            self.runtime,
                            tenant,
                            record["planning_skills"],
                            record["policy"],
                            record["role"],
                            client=pipe,
                            watch=True,
                            mode="plan",
                        )
                    audit = {
                        "action": "agent.create" if expected_version is None else "agent.update",
                        "agent_id": agent_id,
                        "version": record["version"],
                        "subject": record.get("updated_by", record["created_by"]),
                        "role": record["role"],
                        "skills": skill_refs(skill_snapshots),
                        "time": record["updated_at"],
                    }
                    if planning_snapshots:
                        audit["planning_skills"] = skill_refs(planning_snapshots)
                    if expected_version is None:
                        await guard_owner(pipe, tenant, "agent", agent_id, record["created_by"])
                    pipe.multi()
                    pipe.set(key, json.dumps(record, ensure_ascii=False))
                    if expected_version is None:
                        queue_owner(pipe, tenant, "agent", agent_id, record["created_by"])
                    pipe.sadd(self.runtime._tenant_key(tenant, "agents"), agent_id)
                    pipe.xadd(
                        self.runtime._tenant_key(tenant, "audit"),
                        {"payload": json.dumps(audit, ensure_ascii=False)},
                    )
                    await pipe.execute()
                    return record
                except WatchError:
                    continue

    async def create(self, record: dict[str, Any]) -> dict[str, Any]:
        return await self._write(record, None)

    async def update(self, record: dict[str, Any], expected_version: int) -> dict[str, Any] | None:
        return await self._write(record, expected_version)

    async def set_enabled(self, tenant: str, agent_id: str, enabled: bool) -> dict[str, Any] | None:
        if not enabled:
            key = self._key(tenant, agent_id)
            while True:
                async with self._client.pipeline(transaction=True) as pipe:
                    try:
                        await pipe.watch(key)
                        raw = await pipe.get(key)
                        current = json.loads(raw) if raw else None
                        if current is None or not current["enabled"]:
                            return current
                        current.update(
                            enabled=False,
                            version=current["version"] + 1,
                            updated_at=datetime.now(UTC).isoformat(),
                        )
                        pipe.multi()
                        pipe.set(key, json.dumps(current, ensure_ascii=False))
                        pipe.xadd(
                            self.runtime._tenant_key(tenant, "audit"),
                            {
                                "payload": json.dumps(
                                    {
                                        "action": "agent.disable",
                                        "agent_id": agent_id,
                                        "version": current["version"],
                                    }
                                )
                            },
                        )
                        await pipe.execute()
                        return current
                    except WatchError:
                        continue
        while True:
            current = await self.get(tenant, agent_id)
            if current is None or current["enabled"] == enabled:
                return current
            updated = {**current, "enabled": enabled, "updated_at": datetime.now(UTC).isoformat()}
            result = await self.update(updated, current["version"])
            if result is not None:
                return result


def _runtime_store(runtime: Any) -> AgentStore:
    return getattr(runtime, "agent_store", None) or AgentStore(runtime)


def _request_store(request: Request) -> AgentStore:
    runtime = request.app.state.redis
    if runtime is None:
        raise HTTPException(status_code=503, detail="Agent configuration requires Redis")
    return getattr(request.app.state, "agent_store", None) or _runtime_store(runtime)


async def apply_agent(
    runtime: Any,
    tenant: str,
    agent_id: str,
    goal: dict[str, Any],
    *,
    job_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve trusted identity/persona; a job may narrow, never expand, its scope."""
    agent = await _runtime_store(runtime).get(tenant, agent_id)
    if agent is None:
        raise HTTPException(status_code=404, detail="Agent not found")
    if agent.get("tenant_id") != tenant:
        raise HTTPException(status_code=404, detail="Agent not found")
    if not agent["enabled"]:
        raise HTTPException(status_code=409, detail="Agent is disabled")
    policy = deepcopy(agent["policy"])
    if job_policy is not None:
        policy["allowed_tools"] = sorted(
            set(policy.get("allowed_tools", [])) & set(job_policy.get("allowed_tools", []))
        )
        agent_domains = set(policy.get("allowed_domains", []))
        job_domains = set(job_policy.get("allowed_domains", []))
        if agent_domains and job_domains:
            intersection = agent_domains & job_domains
            if not intersection:
                raise HTTPException(status_code=409, detail="Agent and job domains do not overlap")
            policy["allowed_domains"] = sorted(intersection)
        else:
            policy["allowed_domains"] = sorted(agent_domains or job_domains)
        for field in ("token_budget", "max_runs_per_day", "approval_timeout_seconds"):
            policy[field] = min(int(policy[field]), int(job_policy.get(field, policy[field])))
    requested_budget = goal.get("token_budget")
    if requested_budget is not None:
        policy["token_budget"] = min(policy["token_budget"], int(requested_budget))
    requested_approval = goal.get("approval_timeout_seconds")
    if requested_approval is not None:
        policy["approval_timeout_seconds"] = min(
            policy["approval_timeout_seconds"], int(requested_approval)
        )
    bindings = deepcopy(agent.get("skills", []))
    selection = None
    if agent.get("skill_selection_rules"):
        bindings, selection = select_skills(bindings, agent["skill_selection_rules"], goal)
    try:
        skill_snapshots = (
            await resolve_skill_bindings(runtime, tenant, bindings, policy, agent["role"])
            if bindings
            else []
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    snapshot = {
        field: deepcopy(agent[field])
        for field in (
            "agent_id",
            "name",
            "description",
            "persona",
            "role",
            "model",
            "version",
            "delegate_agent_ids",
            "policy",
        )
    }
    snapshot.update(skills=bindings, skill_snapshots=skill_snapshots)
    if selection is not None:
        snapshot["skill_selection"] = selection
    trusted_goal = deepcopy(goal)
    trusted_goal.pop("skills", None)
    trusted_goal.pop("skill_snapshots", None)
    return {
        **trusted_goal,
        "tenant_id": tenant,
        "agent_id": agent["agent_id"],
        "agent_config": snapshot,
        "agent_role": agent["role"],
        "model": agent["model"],
        "policy": policy,
        "token_budget": policy["token_budget"],
        "approval_timeout_seconds": policy["approval_timeout_seconds"],
        "delegate_agent_ids": list(agent.get("delegate_agent_ids", [])),
    }


async def build_agent_goal(
    runtime: Any, tenant: str, agent_id: str, payload: GoalCreateRequest, *, created_by: str
) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    goal = {
        "goal_id": str(uuid4()),
        "tenant_id": tenant,
        "created_by": created_by,
        "objective": payload.objective,
        "model": payload.model,
        "status": "queued",
        "current_step": 0,
        "max_retries": payload.max_retries,
        "max_tool_rounds": payload.max_tool_rounds,
        "step_deadline_seconds": payload.step_deadline_seconds,
        "created_at": now,
        "updated_at": now,
        "steps": [
            {
                "id": str(uuid4()),
                "query": step.query,
                "target_url": str(step.target_url) if step.target_url else None,
                "status": "pending",
                "answer": "",
                "error": "",
            }
            for step in payload.steps
        ],
    }
    return await apply_agent(runtime, tenant, agent_id, goal)


def _record(payload: AgentCreate, principal: Any, request: Request) -> dict[str, Any]:
    if payload.planning_skills and not request.app.state.settings.skill_v2_enabled:
        raise HTTPException(status_code=409, detail="Planning Skill management is disabled")
    if (
        payload.skill_selection_rules
        and not request.app.state.settings.skill_auto_selection_enabled
    ):
        raise HTTPException(status_code=409, detail="Automatic Skill selection is disabled")
    model = payload.model or request.app.state.model_router.default_model
    try:
        request.app.state.model_router.get(model)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    now = datetime.now(UTC).isoformat()
    return payload.model_dump(mode="json", exclude={"expected_version"}) | {
        "agent_id": str(uuid4()),
        "tenant_id": principal.tenant_id,
        "model": model,
        "created_by": principal.subject,
        "created_at": now,
        "updated_at": now,
        "version": 1,
    }


@router.post("")
async def create_agent(payload: AgentCreate, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    try:
        return await _request_store(request).create(_record(payload, principal, request))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("")
async def list_agents(request: Request) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    rows = await _request_store(request).list(principal.tenant_id)
    return {"agents": await filter_resources(request, principal, "agent", rows, "agent_id")}


@router.get("/{agent_id}")
async def get_agent(agent_id: str, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    agent = await _request_store(request).get(principal.tenant_id, agent_id)
    if agent is None:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent


@router.put("/{agent_id}")
async def update_agent(agent_id: str, payload: AgentUpdate, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    store = _request_store(request)
    current = await store.get(principal.tenant_id, agent_id)
    if current is None:
        raise HTTPException(status_code=404, detail="Agent not found")
    updated = _record(payload, principal, request) | {
        "agent_id": agent_id,
        "created_at": current["created_at"],
        "created_by": current["created_by"],
        "updated_by": principal.subject,
    }
    try:
        saved = await store.update(updated, payload.expected_version)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if saved is None:
        raise HTTPException(status_code=409, detail="Agent version conflict")
    return saved


@router.post("/{agent_id}/enabled")
async def enable_agent(agent_id: str, payload: AgentEnabled, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    try:
        agent = await _request_store(request).set_enabled(
            principal.tenant_id, agent_id, payload.enabled
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if agent is None:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent


@router.post("/{agent_id}/goals")
async def create_agent_goal(
    agent_id: str, payload: GoalCreateRequest, request: Request
) -> dict[str, Any]:
    principal = await authorize(request, "operator")
    _request_store(request)
    runtime, settings = request.app.state.redis, request.app.state.settings
    goal = await build_agent_goal(
        runtime, principal.tenant_id, agent_id, payload, created_by=principal.subject
    )
    if settings.pii_redaction:
        goal["objective"] = redact_pii(goal["objective"])
        for step in goal["steps"]:
            step["query"] = redact_pii(step["query"])
    await validate_goal_input(
        runtime,
        settings,
        request.app.state.model_router,
        request.app.state.threat_detector,
        {**goal, "automation_id": goal.get("automation_id")},
    )
    goal["history_search_approved"] = True
    try:
        return await GoalExecutionStore(runtime).submit_manual(goal)
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
