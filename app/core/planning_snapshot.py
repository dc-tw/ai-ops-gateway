"""Trusted planning configuration snapshots, with no inference or execution side effects.

The caller owns the Redis transaction. With ``watch=True``, every configuration
key read here remains watched until that caller commits its session/revision.
Plan text and completion are deliberately separate from configuration identity.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt

from app.automations import AutomationPolicy
from app.core.security import TENANT_PATTERN
from app.core.skill_selection import SkillSelectionRule, selection_candidates
from app.core.skill_store import SkillBinding, resolve_skill_bindings

_CONFIG_FIELDS = ("schema_version", "tenant_id", "agent_id", "root_agent", "workers")
_SNAPSHOT_FIELDS = {*_CONFIG_FIELDS, "snapshot_fingerprint"}
_UNAVAILABLE = "Planning configuration is unavailable or invalid"
_INVALID_PLAN = "Plan does not match the configured Agent and approved Workers"


class PlanningSnapshotNotFound(ValueError):
    """The requested root Agent is not available within the principal's tenant."""


class PlanningSnapshotInvalid(ValueError):
    """Configuration changed, is disabled, or cannot be safely snapshotted."""


class _StoredAgent(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True, hide_input_in_errors=True)
    tenant_id: str
    agent_id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=2_000)
    persona: str = Field(min_length=1, max_length=8_000)
    role: Literal["chief", "worker"]
    model: str = Field(min_length=1, max_length=200)
    policy: AutomationPolicy
    enabled: StrictBool
    version: StrictInt = Field(ge=1)
    delegate_agent_ids: list[str] = Field(default_factory=list, max_length=8)
    skills: list[SkillBinding] = Field(default_factory=list, max_length=8)
    planning_skills: list[SkillBinding] = Field(default_factory=list, max_length=8)
    skill_selection_rules: list[SkillSelectionRule] = Field(default_factory=list, max_length=8)


def _decode_agent(raw: Any, tenant: str, agent_id: str, *, root: bool = False) -> _StoredAgent:
    if not raw:
        if root:
            raise PlanningSnapshotNotFound("Agent not found")
        raise PlanningSnapshotInvalid(_UNAVAILABLE)
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get("tenant_id") != tenant:
        if root:
            raise PlanningSnapshotNotFound("Agent not found")
        raise PlanningSnapshotInvalid(_UNAVAILABLE)
    agent = _StoredAgent.model_validate(value)
    delegates = agent.delegate_agent_ids
    if (
        agent.agent_id != agent_id
        or not agent.enabled
        or not agent.persona.strip()
        or not agent.model.strip()
        or any(not item.strip() or len(item) > 128 for item in delegates)
        or len(set(delegates)) != len(delegates)
        or agent_id in delegates
        or (agent.role == "worker" and delegates)
        or (agent.role == "chief" and agent.policy.allowed_tools)
        or len({binding.skill_id for binding in agent.skills}) != len(agent.skills)
        or len({binding.skill_id for binding in agent.planning_skills})
        != len(agent.planning_skills)
    ):
        raise PlanningSnapshotInvalid(_UNAVAILABLE)
    return agent


def _safe_route(router: Any, model: str) -> dict[str, Any]:
    # Never call router.get(): snapshot creation cannot instantiate an inference
    # client, probe an endpoint, or fall back to a different model.
    value = router.planning_route(model)
    if (
        not isinstance(value, dict)
        or value.get("alias") != model.strip()
        or not isinstance(value.get("resolved_model"), str)
        or not value["resolved_model"].strip()
        or len(value["resolved_model"]) > 200
        or not isinstance(value.get("route_fingerprint"), str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", value["route_fingerprint"])
    ):
        raise PlanningSnapshotInvalid(_UNAVAILABLE)
    result = {field: value[field] for field in ("alias", "resolved_model", "route_fingerprint")}
    # These fixed labels disclose the limitation without copying arbitrary
    # helper fields, configured URLs, credentials, or provider response data.
    result["version_disclosure"] = {
        "actual_model_version": "unknown",
        "identity_scope": "configured_endpoint_model_and_inference_settings",
        "limitation": "Weights, deployment, tokenizer and runtime versions are unverified.",
    }
    return result


async def _published_route(
    runtime: Any,
    router: Any,
    tenant: str,
    model: str,
    client: Any,
    watch: bool,
) -> dict[str, Any]:
    result = _safe_route(router, model)
    # A server-side publication is optional for existing planning sessions, but
    # when present it is part of the watched snapshot and therefore consistent
    # across replicas.  Absence never turns an alias into a known model version.
    from app.core.model_registry import ModelRegistry

    publication = None
    if getattr(router, "publication_enabled", False):
        publication = await ModelRegistry(runtime).get(
            tenant, result["alias"], client=client, watch=watch
        )
    if publication is not None:
        if (
            publication["route_fingerprint"] != result["route_fingerprint"]
            or publication["resolved_model"] != result["resolved_model"]
        ):
            raise PlanningSnapshotInvalid(_UNAVAILABLE)
        result["publication"] = {
            field: publication[field]
            for field in (
                "revision",
                "publication_sha256",
                "actual_model_version",
                "deployment_id",
                "tokenizer_version",
                "runtime_version",
            )
        }
        result["version_disclosure"] = {
            "actual_model_version": publication["actual_model_version"] or "unknown",
            "identity_scope": "administrator_published_provider_disclosure",
            "limitation": (
                "Published identity is operator-supplied; the Gateway does not probe or attest "
                "weights, tokenizer, deployment, or runtime."
            ),
        }
    return result


async def _snapshot_agent(
    runtime: Any, router: Any, tenant: str, agent: _StoredAgent, client: Any, watch: bool
) -> dict[str, Any]:
    value = agent.model_dump(mode="json", exclude={"enabled"})
    if not agent.skill_selection_rules:
        value.pop("skill_selection_rules", None)
    if not agent.planning_skills:
        value.pop("planning_skills", None)  # Existing v1 planning fingerprints stay unchanged.
    value["persona_sha256"] = hashlib.sha256(agent.persona.encode("utf-8")).hexdigest()
    value["model_route"] = await _published_route(
        runtime, router, tenant, agent.model, client, watch
    )
    value["skill_snapshots"] = await resolve_skill_bindings(
        runtime,
        tenant,
        value["skills"],
        value["policy"],
        agent.role,
        client=client,
        watch=watch,
    )
    if agent.planning_skills:
        value["planning_skill_snapshots"] = await resolve_skill_bindings(
            runtime,
            tenant,
            value["planning_skills"],
            value["policy"],
            agent.role,
            client=client,
            watch=watch,
            mode="plan",
        )
    if agent.skill_selection_rules:
        candidates = selection_candidates(value["skills"], value["skill_selection_rules"])
        value["skill_selection_candidates"] = await resolve_skill_bindings(
            runtime, tenant, candidates, value["policy"], agent.role, client=client, watch=watch
        )
    return value


def planning_snapshot_fingerprint(snapshot: dict[str, Any]) -> str:
    """Hash configuration only; session mode/version and Plan content are separate."""
    try:
        value = {field: snapshot[field] for field in _CONFIG_FIELDS}
        if type(value["schema_version"]) is not int or value["schema_version"] != 1:
            raise ValueError
        canonical = json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    except (ValueError, KeyError, TypeError, UnicodeError):
        raise PlanningSnapshotInvalid(_UNAVAILABLE) from None


def validate_planning_plan(snapshot: dict[str, Any], plan: dict[str, Any] | None) -> bool:
    """Check role/assignment compatibility after the API validates Plan content.

    Empty work is an incomplete draft, not permission to execute free planning.
    This function does not resolve current configuration or mutate the snapshot.
    """
    if plan is None:
        return False
    try:
        if not isinstance(plan, dict):
            raise ValueError
        if plan.get("schema_version") == 2:
            from app.core.workflow_plan import compile_workflow

            compile_workflow(plan, snapshot, check_schemas=False)
            return True
        steps, assignments = plan.get("steps", []), plan.get("assignments", [])
        if not isinstance(steps, list) or not isinstance(assignments, list):
            raise ValueError
        root = snapshot["root_agent"]
        if root["role"] == "worker":
            if assignments:
                raise ValueError
            return bool(steps)
        if root["role"] != "chief" or steps or len(assignments) > 8:
            raise ValueError
        workers = {worker["agent_id"] for worker in snapshot["workers"]}
        seen = set()
        complete = bool(assignments)
        for assignment in assignments:
            if not isinstance(assignment, dict):
                raise ValueError
            worker_id = assignment.get("agent_id")
            worker_steps = assignment.get("steps", [])
            if (
                not isinstance(worker_id, str)
                or worker_id not in workers
                or worker_id not in root["delegate_agent_ids"]
                or worker_id in seen
                or not isinstance(worker_steps, list)
            ):
                raise ValueError
            seen.add(worker_id)
            complete = complete and bool(worker_steps)
        return complete
    except (ValueError, KeyError, TypeError):
        raise ValueError(_INVALID_PLAN) from None


async def build_planning_snapshot(
    runtime: Any,
    router: Any,
    tenant: str,
    agent_id: str,
    plan: dict[str, Any] | None = None,
    *,
    client: Any,
    watch: bool = True,
) -> dict[str, Any]:
    """Resolve root, every approved Worker, and their pinned Skills in one read set.

    This function neither starts nor commits a transaction. The caller must keep
    the supplied WATCH pipeline open through its session/revision write and retry
    the entire build on WatchError. Passing a plain client requires watch=False.
    Route identity describes this router's configured state; it does not establish
    a shared deployment epoch or discover the backend's actual model version.
    """
    try:
        if (
            not isinstance(tenant, str)
            or not TENANT_PATTERN.fullmatch(tenant)
            or not isinstance(agent_id, str)
            or not 1 <= len(agent_id) <= 128
            or client is None
        ):
            raise PlanningSnapshotInvalid(_UNAVAILABLE)
        root_key = runtime._tenant_key(tenant, f"agents:record:{agent_id}")
        if watch:
            await client.watch(root_key)
        root = _decode_agent(await client.get(root_key), tenant, agent_id, root=True)
        ids = root.delegate_agent_ids
        keys = [runtime._tenant_key(tenant, f"agents:record:{item}") for item in ids]
        if watch and keys:
            await client.watch(*keys)
        rows = await client.mget(keys) if keys else []
        workers = []
        for worker_id, raw in zip(ids, rows, strict=True):
            worker = _decode_agent(raw, tenant, worker_id)
            if worker.role != "worker" or worker.delegate_agent_ids:
                raise PlanningSnapshotInvalid(_UNAVAILABLE)
            workers.append(worker)
        snapshot = {
            "schema_version": 1,
            "tenant_id": tenant,
            "agent_id": agent_id,
            "root_agent": await _snapshot_agent(runtime, router, tenant, root, client, watch),
            "workers": [
                await _snapshot_agent(runtime, router, tenant, worker, client, watch)
                for worker in workers
            ],
        }
        snapshot["snapshot_fingerprint"] = planning_snapshot_fingerprint(snapshot)
        validate_planning_plan(snapshot, plan)
        return snapshot
    except PlanningSnapshotNotFound:
        raise
    except (ValueError, KeyError, TypeError, AttributeError, UnicodeError):
        raise PlanningSnapshotInvalid(_UNAVAILABLE) from None


async def guard_planning_snapshot(
    runtime: Any,
    router: Any,
    tenant: str,
    snapshot: dict[str, Any],
    *,
    client: Any,
    watch: bool = True,
) -> None:
    """Verify a stored snapshot and WATCH all live sources without refreshing it."""
    try:
        if (
            not isinstance(snapshot, dict)
            or set(snapshot) != _SNAPSHOT_FIELDS
            or snapshot["tenant_id"] != tenant
            or snapshot["snapshot_fingerprint"] != planning_snapshot_fingerprint(snapshot)
        ):
            raise PlanningSnapshotInvalid(_UNAVAILABLE)
        current = await build_planning_snapshot(
            runtime, router, tenant, snapshot["agent_id"], client=client, watch=watch
        )
        if current["snapshot_fingerprint"] != snapshot["snapshot_fingerprint"]:
            raise PlanningSnapshotInvalid(_UNAVAILABLE)
    except (ValueError, KeyError, TypeError):
        raise PlanningSnapshotInvalid(_UNAVAILABLE) from None
