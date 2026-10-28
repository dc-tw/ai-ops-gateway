"""Deterministic, plan-only forecasts built from an immutable compiled Workflow."""

from __future__ import annotations

import asyncio
import json
import math
from datetime import UTC, datetime
from typing import Any

from redis.exceptions import WatchError

from app.core.model_registry import ModelRegistry
from app.core.planning_store import (
    PlanningConflict,
    PlanningIntegrityError,
    PlanningStore,
    canonical_hash,
)
from app.core.skill_store import skill_text
from app.tools.registry import TOOL_SPECS


def _json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )


def _planning_forecast_bound(snapshot: dict[str, Any]) -> bool:
    return any(
        item.get("name") == "plan-forecast"
        and item.get("schema_version") == 2
        and item.get("execution_modes") == ["plan"]
        for item in snapshot["root_agent"].get("planning_skill_snapshots", [])
    )


def _agent(snapshot: dict[str, Any], agent_id: str) -> dict[str, Any]:
    for item in [snapshot["root_agent"], *snapshot.get("workers", [])]:
        if item["agent_id"] == agent_id:
            return item
    raise PlanningIntegrityError("Compiled Workflow references an unknown Agent")


def _schedule(compiled: dict[str, Any], durations: dict[str, float], included: set[str]) -> float:
    """Deterministic list scheduling with DAG, global and per-Agent capacity."""
    lanes = [0.0] * compiled["parallelism"]
    finished: dict[str, float] = {}
    agents: dict[str, float] = {}
    for node in compiled["nodes"]:  # compiler stores topological order
        node_id = node["node_id"]
        if node_id not in included:
            finished[node_id] = max(
                (finished.get(parent, 0.0) for parent in node["dependencies"]), default=0.0
            )
            continue
        dependency = max(
            (finished.get(parent, 0.0) for parent in node["dependencies"]), default=0.0
        )
        lane = min(range(len(lanes)), key=lambda index: lanes[index])
        start = max(dependency, lanes[lane], agents.get(node["agent_id"], 0.0))
        end = start + durations[node_id]
        lanes[lane], agents[node["agent_id"]], finished[node_id] = end, end, end
    return round(max(finished.values(), default=0.0), 3)


async def build_forecast(
    runtime: Any,
    tenant: str,
    session: dict[str, Any],
    revision: dict[str, Any],
    compiled: dict[str, Any],
    *,
    history_tokens_cap: int,
    max_output_tokens: int,
) -> tuple[dict[str, Any], dict[str, str | None]]:
    snapshot = revision["snapshot"]
    if not _planning_forecast_bound(snapshot):
        raise PermissionError("The root Agent has no explicit plan-forecast planning Skill")
    registry = ModelRegistry(runtime)
    profiles: dict[str, dict[str, Any] | None] = {}
    sources: dict[str, str | None] = {}
    routes: dict[str, dict[str, Any]] = {}
    for agent in [snapshot["root_agent"], *snapshot.get("workers", [])]:
        route = agent["model_route"]
        routes[agent["agent_id"]] = route
        publication = route.get("publication")
        if publication is None:
            profiles[agent["agent_id"]] = None
            sources[agent["agent_id"]] = None
            continue
        current = await registry.get(tenant, route["alias"])
        if (
            current is None
            or current["publication_sha256"] != publication["publication_sha256"]
            or current["route_fingerprint"] != route["route_fingerprint"]
        ):
            raise PlanningConflict("Model publication changed; refresh the planning session")
        profile = await registry.calibration(tenant, publication["publication_sha256"])
        profiles[agent["agent_id"]] = profile
        sources[agent["agent_id"]] = profile["calibration_sha256"] if profile is not None else None

    calls = []
    estimated = True
    has_tools = False
    has_approval = False
    durations = {scenario: {} for scenario in ("low", "base", "high")}
    node_tokens = {scenario: {} for scenario in ("low", "base", "high")}
    node_input_tokens = {scenario: {} for scenario in ("low", "base", "high")}
    node_output_tokens = {scenario: {} for scenario in ("low", "base", "high")}
    mandatory: set[str] = set()
    conditional_or_downstream: set[str] = set()
    for node in compiled["nodes"]:
        agent = _agent(snapshot, node["agent_id"])
        tools = sorted(agent["policy"].get("allowed_tools", []))
        has_tools = has_tools or bool(tools)
        has_approval = has_approval or any(
            TOOL_SPECS.get(name) and TOOL_SPECS[name].requires_approval for name in tools
        )
        if node.get("condition") or any(
            parent in conditional_or_downstream for parent in node["dependencies"]
        ):
            conditional_or_downstream.add(node["node_id"])
        else:
            mandatory.add(node["node_id"])
        skills = node.get("skill_snapshots", [])
        context = "\n".join(
            [
                agent.get("persona", ""),
                *(skill_text(item) for item in skills),
                compiled.get("objective", ""),
                compiled.get("context", ""),
                node.get("query", ""),
                _json(node.get("inputs", {})),
                _json(node.get("output_schema")),
                _json(tools),
            ]
        )
        input_bytes = len(context.encode("utf-8")) + 256
        call_counts = {
            "low": 1,
            "base": 1,
            "high": (compiled["max_retries"] + 1)
            * ((compiled["max_tool_rounds"] + 1) if tools else 1),
        }
        profile = profiles[node["agent_id"]]
        token_estimates: dict[str, int | None] = {}
        input_estimates: dict[str, int | None] = {}
        output_estimates: dict[str, int | None] = {}
        duration_estimates: dict[str, float | None] = {}
        for scenario in ("low", "base", "high"):
            if profile is None:
                estimated = False
                token_estimates[scenario] = None
                input_estimates[scenario] = None
                output_estimates[scenario] = None
                duration_estimates[scenario] = None
                durations[scenario][node["node_id"]] = 0.0
                continue
            input_per_call = math.ceil(input_bytes * profile["input_tokens_per_byte"][scenario])
            output_per_call = min(max_output_tokens, profile["output_tokens_per_call"][scenario])
            # Workflow nodes start with a fresh conversation. Only the high
            # tool/retry scenario reserves the configured history ceiling for
            # subsequent calls; it is an explicit bound, not a median guess.
            history = (
                history_tokens_cap * max(0, call_counts[scenario] - 1) if scenario == "high" else 0
            )
            input_estimates[scenario] = input_per_call * call_counts[scenario] + history
            output_estimates[scenario] = output_per_call * call_counts[scenario]
            token_estimates[scenario] = input_estimates[scenario] + output_estimates[scenario]
            duration_estimates[scenario] = round(
                profile["latency_seconds_per_call"][scenario] * call_counts[scenario],
                3,
            )
            durations[scenario][node["node_id"]] = duration_estimates[scenario]
            node_tokens[scenario][node["node_id"]] = token_estimates[scenario]
            node_input_tokens[scenario][node["node_id"]] = input_estimates[scenario]
            node_output_tokens[scenario][node["node_id"]] = output_estimates[scenario]
        route = routes[node["agent_id"]]
        calls.append(
            {
                "node_id": node["node_id"],
                "agent_id": node["agent_id"],
                "kind": node["kind"],
                "dependencies": node["dependencies"],
                "conditional": node["node_id"] in conditional_or_downstream,
                "route": {
                    "alias": route["alias"],
                    "resolved_model": route["resolved_model"],
                    "route_fingerprint": route["route_fingerprint"],
                    "publication": route.get("publication"),
                },
                "context_manifest": {
                    "persona_bytes": len(agent.get("persona", "").encode("utf-8")),
                    "skill_resource_bytes": sum(
                        len(skill_text(item).encode("utf-8")) for item in skills
                    ),
                    "plan_and_node_bytes": input_bytes,
                    "history_tokens_cap": history_tokens_cap,
                    "tools": tools,
                    "max_retries": compiled["max_retries"],
                    "max_tool_rounds": compiled["max_tool_rounds"],
                    "node_token_budget": node["token_budget"],
                },
                "model_call_count": call_counts,
                "input_token_estimate": input_estimates,
                "output_token_estimate": output_estimates,
                "token_estimate": token_estimates,
                "node_budget_risk": {
                    scenario: (
                        token_estimates[scenario] > node["token_budget"]
                        if token_estimates[scenario] is not None
                        else None
                    )
                    for scenario in ("low", "base", "high")
                },
                "model_latency_seconds": duration_estimates,
                "calibration_sha256": sources[node["agent_id"]],
            }
        )

    scenarios: dict[str, Any] = {}
    for scenario in ("low", "base", "high"):
        included = (
            mandatory if scenario == "low" else {item["node_id"] for item in compiled["nodes"]}
        )
        scenarios[scenario] = {
            "tokens": (
                {
                    "input": sum(node_input_tokens[scenario][node_id] for node_id in included),
                    "output": sum(node_output_tokens[scenario][node_id] for node_id in included),
                    "total": sum(node_tokens[scenario][node_id] for node_id in included),
                }
                if estimated
                else {"input": None, "output": None, "total": None}
            ),
            "token_estimate": (
                sum(node_tokens[scenario][node_id] for node_id in included) if estimated else None
            ),
            "model_only_makespan_seconds": (
                _schedule(compiled, durations[scenario], included) if estimated else None
            ),
            "included_nodes": sorted(included),
            "team_token_budget": compiled["token_budget"],
            "budget_risk": (
                sum(node_tokens[scenario][node_id] for node_id in included)
                > compiled["token_budget"]
                if estimated
                else None
            ),
            "deadline_risk_nodes": (
                sorted(
                    node_id
                    for node_id in included
                    if durations[scenario][node_id] > compiled["step_deadline_seconds"]
                )
                if estimated
                else None
            ),
        }
    source = {
        "session_id": session["session_id"],
        "plan_revision": revision["revision"],
        "plan_sha256": revision["content_sha256"],
        "snapshot_sha256": revision["snapshot_sha256"],
        "compiled_sha256": compiled["compiled_sha256"],
        "calibrations": sources,
    }
    artifact = {
        "schema_version": 1,
        "tenant_id": tenant,
        "session_id": session["session_id"],
        "plan_revision": revision["revision"],
        "status": "estimated" if estimated else "insufficient_calibration",
        "source": source,
        "call_manifest": calls,
        "scenarios": scenarios,
        "waits": {
            "queue": {"status": "unknown", "included_in_makespan": False},
            "tool": {
                "status": "unknown" if has_tools else "not_applicable",
                "included_in_makespan": False,
            },
            "approval": {
                "status": "unknown" if has_approval else "not_applicable",
                "included_in_makespan": False,
            },
        },
        "assumptions": {
            "low": "Only unconditional nodes, one model call, empirical sample q10.",
            "base": (
                "All possible branches, no retry/tool-loop calls, fresh Workflow conversation, "
                "empirical sample median."
            ),
            "high": (
                "All branches and configured retry/tool-loop caps, configured history ceiling "
                "for subsequent calls, empirical sample q90."
            ),
            "scheduling": "DAG dependencies, global parallelism, and one active node per Agent.",
        },
        "provider_calls_started": 0,
    }
    artifact["forecast_sha256"] = canonical_hash(artifact)
    return artifact, sources


class ForecastStore:
    def __init__(self, planning: PlanningStore) -> None:
        self.planning = planning
        self.runtime = planning.runtime
        self.client = planning.client

    def key(self, tenant: str, forecast_sha256: str) -> str:
        return self.runtime._tenant_key(tenant, f"planning:forecast:{forecast_sha256}")

    def latest_key(self, tenant: str, session_id: str) -> str:
        return self.runtime._tenant_key(tenant, f"planning:forecast-latest:{session_id}")

    @staticmethod
    def _verified_record(
        raw: str, tenant: str, session_id: str, expected_digest: str
    ) -> dict[str, Any]:
        try:
            value = json.loads(raw)
        except (TypeError, ValueError, RecursionError):
            raise PlanningIntegrityError("Forecast record is not valid JSON") from None
        if not isinstance(value, dict):
            raise PlanningIntegrityError("Forecast record has an invalid shape")
        record_body = {name: item for name, item in value.items() if name != "record_sha256"}
        if value.get("record_sha256") != canonical_hash(record_body):
            raise PlanningIntegrityError("Forecast record failed integrity verification")
        content = {
            name: item
            for name, item in value.items()
            if name not in {"forecast_sha256", "record_sha256", "created_at", "created_by"}
        }
        if (
            value.get("forecast_sha256") != expected_digest
            or value.get("forecast_sha256") != canonical_hash(content)
        ):
            raise PlanningIntegrityError("Forecast artifact failed integrity verification")
        if value.get("tenant_id") != tenant or value.get("session_id") != session_id:
            raise PlanningIntegrityError("Forecast artifact identity is invalid")
        return value

    async def publish(
        self,
        tenant: str,
        session_id: str,
        expected_version: int,
        subject: str,
        artifact: dict[str, Any],
        calibration_sources: dict[str, str | None],
        snapshot_builder: Any,
    ) -> dict[str, Any]:
        digest = artifact["forecast_sha256"]
        key = self.key(tenant, digest)
        session_key = self.planning.session_key(tenant, session_id)
        latest = self.latest_key(tenant, session_id)
        registry = ModelRegistry(self.runtime)
        for _ in range(8):
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(session_key, key, latest)
                    session = self.planning._session(
                        await pipe.get(session_key), tenant, session_id
                    )
                    self.planning._expected(session, expected_version)
                    if session["mode"] != "plan":
                        raise PlanningConflict("Forecast creation requires plan mode")
                    revision = await self.planning._current_revision(pipe, session, watch=True)
                    if revision is None or revision["revision"] != artifact["plan_revision"]:
                        raise PlanningConflict("Forecast source Plan changed")
                    current_snapshot = await snapshot_builder(pipe)
                    if (
                        current_snapshot["snapshot_fingerprint"]
                        != revision["snapshot"]["snapshot_fingerprint"]
                    ):
                        raise PlanningConflict(
                            "Planning configuration changed; refresh before forecasting"
                        )
                    for agent in [
                        revision["snapshot"]["root_agent"],
                        *revision["snapshot"].get("workers", []),
                    ]:
                        publication = agent["model_route"].get("publication")
                        if publication is None:
                            continue
                        current = await registry.get(
                            tenant, agent["model_route"]["alias"], client=pipe, watch=True
                        )
                        if (
                            not current
                            or current["publication_sha256"] != publication["publication_sha256"]
                        ):
                            raise PlanningConflict("Model publication changed; refresh and retry")
                        current_calibration = await registry.calibration(
                            tenant,
                            publication["publication_sha256"],
                            client=pipe,
                            watch=True,
                        )
                        current_hash = (
                            current_calibration["calibration_sha256"]
                            if current_calibration
                            else None
                        )
                        if current_hash != calibration_sources[agent["agent_id"]]:
                            raise PlanningConflict("Calibration changed; regenerate the forecast")
                    existing = await pipe.get(key)
                    if existing is not None:
                        saved = self._verified_record(existing, tenant, session_id, digest)
                        pipe.multi()
                        pipe.set(latest, digest)
                        await pipe.execute()
                        return saved | {"deduplicated": True}
                    saved = artifact | {
                        "created_at": datetime.now(UTC).isoformat(),
                        "created_by": subject,
                    }
                    saved["record_sha256"] = canonical_hash(saved)
                    # created metadata is intentionally outside the deterministic content hash.
                    pipe.multi()
                    pipe.set(key, _json(saved))
                    pipe.set(latest, digest)
                    pipe.xadd(
                        self.planning.events_key(tenant, session_id),
                        {
                            "payload": _json(
                                {
                                    "type": "forecast.published",
                                    "session_id": session_id,
                                    "plan_revision": artifact["plan_revision"],
                                    "forecast_sha256": digest,
                                    "status": artifact["status"],
                                    "subject": subject,
                                    "time": saved["created_at"],
                                }
                            )
                        },
                    )
                    pipe.xadd(
                        self.runtime._tenant_key(tenant, "audit"),
                        {
                            "payload": _json(
                                {
                                    "action": "forecast.published",
                                    "session_id": session_id,
                                    "forecast_sha256": digest,
                                    "subject": subject,
                                    "time": saved["created_at"],
                                }
                            )
                        },
                    )
                    await pipe.execute()
                    return saved | {"deduplicated": False}
                except WatchError:
                    await asyncio.sleep(0)
        raise PlanningConflict("Forecast inputs changed too often; retry")

    async def get(self, tenant: str, session_id: str, digest: str | None = None):
        if digest is None:
            digest = await self.client.get(self.latest_key(tenant, session_id))
        if not isinstance(digest, str) or len(digest) != 64:
            return None
        raw = await self.client.get(self.key(tenant, digest))
        if raw is None:
            return None
        return self._verified_record(raw, tenant, session_id, digest)
