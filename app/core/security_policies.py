"""Immutable, server-approved MCP and ATR policy references for Agents."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator
from redis.exceptions import WatchError

from app.core.threat_detection import SEVERITY_RANK
from app.tools.registry import TOOL_SPECS

ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class SecurityPolicyRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    policy_id: str = Field(min_length=1, max_length=128, pattern=ID.pattern)
    revision: int = Field(ge=1)


class MCPPolicyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["mcp"] = "mcp"
    transport_ref: Literal["configured-mcp"] = "configured-mcp"
    allowed_tools: list[str] = Field(min_length=1, max_length=30)
    allowed_domains: list[str] = Field(default_factory=list, max_length=100)

    @field_validator("allowed_tools")
    @classmethod
    def known_tools(cls, values: list[str]) -> list[str]:
        if any(value not in TOOL_SPECS for value in values):
            raise ValueError("Unknown tool in MCP policy")
        return list(dict.fromkeys(values))

    @field_validator("allowed_domains")
    @classmethod
    def valid_domains(cls, values: list[str]) -> list[str]:
        result = [value.lower().rstrip(".") for value in values]
        if any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", v) for v in result):
            raise ValueError("MCP policy domains must be hostnames")
        return list(dict.fromkeys(result))


class ATRPolicyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["atr"] = "atr"
    mode: Literal["detection_only", "blocking"] = "detection_only"
    minimum_severity: Literal["informational", "low", "medium", "high", "critical"] = (
        "medium"
    )
    block_severity: Literal["low", "medium", "high", "critical"] = "high"
    required_rule_ids: list[str] = Field(default_factory=list, max_length=100)

    @field_validator("required_rule_ids")
    @classmethod
    def valid_rules(cls, values: list[str]) -> list[str]:
        if any(not ID.fullmatch(value) for value in values):
            raise ValueError("Invalid ATR rule ID")
        return list(dict.fromkeys(values))


PolicyConfig = Annotated[MCPPolicyConfig | ATRPolicyConfig, Field(discriminator="kind")]


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def server_fingerprint(config: dict[str, Any], settings: Any, detector: Any) -> str:
    kind = config.get("kind")
    if kind == "mcp":
        if getattr(settings, "agent_tool_transport", "") != "mcp":
            raise ValueError("Configured MCP transport is not enabled")
        state = {
            "kind": "mcp",
            "transport_ref": config.get("transport_ref"),
            "agent_tool_transport": settings.agent_tool_transport,
            "allowed_tools": sorted(config.get("allowed_tools", [])),
            "allowed_domains": sorted(config.get("allowed_domains", [])),
        }
    elif kind == "atr":
        if not getattr(settings, "atr_enabled", False) or detector is None:
            raise ValueError("ATR is not enabled")
        if config.get("mode") == "blocking" and not getattr(detector, "blocking", False):
            raise ValueError("ATR blocking policy requires blocking mode")
        if SEVERITY_RANK.get(str(getattr(detector, "min_severity", "critical")), 99) > (
            SEVERITY_RANK[str(config.get("minimum_severity"))]
        ):
            raise ValueError("Current ATR minimum severity is weaker than policy")
        if config.get("mode") == "blocking" and SEVERITY_RANK.get(
            str(getattr(detector, "block_severity", "critical")), 99
        ) > SEVERITY_RANK[str(config.get("block_severity"))]:
            raise ValueError("Current ATR block severity is weaker than policy")
        rules = {
            str(rule.id)
            for rule in getattr(getattr(detector, "engine", None), "rules", [])
            if getattr(rule, "status", "") not in {"draft", "deprecated"}
        }
        required = set(config.get("required_rule_ids", []))
        if not required <= rules:
            raise ValueError("Required ATR rules are not active")
        state = {
            "kind": "atr",
            "mode": config.get("mode"),
            "minimum_severity": config.get("minimum_severity"),
            "block_severity": config.get("block_severity"),
            "required_rule_ids": sorted(required),
            "detector_blocking": bool(detector.blocking),
            "detector_min_severity": detector.min_severity,
            "detector_block_severity": detector.block_severity,
            "ruleset_fingerprint": detector.ruleset_fingerprint,
        }
    else:
        raise ValueError("Unknown security policy kind")
    return hashlib.sha256(_canonical(state).encode()).hexdigest()


class SecurityPolicyStore:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self.client = runtime._client

    def record_key(self, tenant: str, policy_id: str) -> str:
        return self.runtime._tenant_key(tenant, f"security-policies:record:{policy_id}")

    def revision_key(self, tenant: str, policy_id: str, revision: int) -> str:
        return self.runtime._tenant_key(
            tenant, f"security-policies:revision:{policy_id}:{revision}"
        )

    def index_key(self, tenant: str) -> str:
        return self.runtime._tenant_key(tenant, "security-policies")

    def audit_key(self, tenant: str) -> str:
        return self.runtime._tenant_key(tenant, "security:policy-audit")

    async def get(self, tenant: str, policy_id: str) -> dict[str, Any] | None:
        raw = await self.client.get(self.record_key(tenant, policy_id))
        return json.loads(raw) if raw else None

    async def list(self, tenant: str) -> list[dict[str, Any]]:
        ids = sorted(await self.client.smembers(self.index_key(tenant)))
        rows = (
            await self.client.mget([self.record_key(tenant, item) for item in ids])
            if ids
            else []
        )
        return sorted(
            [json.loads(row) for row in rows if row], key=lambda row: row["created_at"]
        )

    async def write(
        self,
        tenant: str,
        *,
        name: str,
        config: dict[str, Any],
        fingerprint: str,
        subject: str,
        policy_id: str | None = None,
        expected_version: int | None = None,
    ) -> dict[str, Any] | None:
        policy_id = policy_id or uuid4().hex
        key = self.record_key(tenant, policy_id)
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    current = json.loads(raw) if raw else None
                    if expected_version is None:
                        if current:
                            raise ValueError("Security policy already exists")
                        version = 1
                    else:
                        if not current or current["version"] != expected_version:
                            return None
                        if current["kind"] != config["kind"]:
                            raise ValueError("Security policy kind is immutable")
                        version = expected_version + 1
                    now = datetime.now(UTC).isoformat()
                    record = {
                        "policy_id": policy_id,
                        "tenant_id": tenant,
                        "name": name,
                        "kind": config["kind"],
                        "config": config,
                        "server_fingerprint": fingerprint,
                        "version": version,
                        "enabled": current.get("enabled", True) if current else True,
                        "created_at": current.get("created_at", now) if current else now,
                        "created_by": current.get("created_by", subject) if current else subject,
                        "updated_at": now,
                        "updated_by": subject,
                    }
                    revision_key = self.revision_key(tenant, policy_id, version)
                    await pipe.watch(revision_key)
                    if await pipe.exists(revision_key):
                        raise RuntimeError("Security policy revision already exists")
                    pipe.multi()
                    pipe.set(key, json.dumps(record, ensure_ascii=False))
                    pipe.set(revision_key, json.dumps(record, ensure_ascii=False), nx=True)
                    pipe.sadd(self.index_key(tenant), policy_id)
                    pipe.xadd(
                        self.audit_key(tenant),
                        {
                            "payload": _canonical(
                                {
                                    "action": (
                                        "security_policy.create"
                                        if expected_version is None
                                        else "security_policy.update"
                                    ),
                                    "policy_id": policy_id,
                                    "kind": config["kind"],
                                    "revision": version,
                                    "subject": subject,
                                }
                            )
                        },
                    )
                    await pipe.execute()
                    return record
                except WatchError:
                    continue

    async def set_enabled(
        self, tenant: str, policy_id: str, enabled: bool, subject: str
    ) -> dict[str, Any] | None:
        key = self.record_key(tenant, policy_id)
        while True:
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    record = json.loads(raw) if raw else None
                    if not record:
                        return None
                    if record["enabled"] == enabled:
                        return record
                    record["enabled"] = enabled
                    record["updated_at"] = datetime.now(UTC).isoformat()
                    record["updated_by"] = subject
                    pipe.multi()
                    pipe.set(key, json.dumps(record, ensure_ascii=False))
                    pipe.xadd(
                        self.audit_key(tenant),
                        {
                            "payload": _canonical(
                                {
                                    "action": (
                                        "security_policy.enable"
                                        if enabled
                                        else "security_policy.disable"
                                    ),
                                    "policy_id": policy_id,
                                    "subject": subject,
                                }
                            )
                        },
                    )
                    await pipe.execute()
                    return record
                except WatchError:
                    continue

    async def resolve(
        self,
        tenant: str,
        reference: SecurityPolicyRef | dict[str, Any],
        expected_kind: str,
        *,
        client: Any | None = None,
        watch: bool = False,
    ) -> dict[str, Any]:
        ref = SecurityPolicyRef.model_validate(reference)
        client = client or self.client
        record_key = self.record_key(tenant, ref.policy_id)
        revision_key = self.revision_key(tenant, ref.policy_id, ref.revision)
        if watch:
            await client.watch(record_key, revision_key)
        current_raw, revision_raw = await client.mget([record_key, revision_key])
        current = json.loads(current_raw) if current_raw else None
        revision = json.loads(revision_raw) if revision_raw else None
        if (
            not current
            or not revision
            or not current.get("enabled")
            or revision.get("kind") != expected_kind
            or revision.get("tenant_id") != tenant
        ):
            raise ValueError(f"{expected_kind.upper()} security policy is unavailable")
        return revision


async def resolve_agent_security_policies(
    runtime: Any,
    tenant: str,
    record: dict[str, Any],
    *,
    client: Any | None = None,
    watch: bool = False,
) -> list[dict[str, Any]]:
    bindings = [
        (field, kind)
        for field, kind in (("mcp_policy_ref", "mcp"), ("atr_policy_ref", "atr"))
        if record.get(field)
    ]
    if not bindings:
        return []
    store = SecurityPolicyStore(runtime)
    snapshots: list[dict[str, Any]] = []
    for field, kind in bindings:
        reference = record.get(field)
        snapshot = await store.resolve(
            tenant, reference, kind, client=client, watch=watch
        )
        snapshots.append(snapshot)
        if kind == "mcp":
            agent_policy = record.get("policy", {})
            allowed_tools = set(snapshot["config"].get("allowed_tools", []))
            if not set(agent_policy.get("allowed_tools", [])) <= allowed_tools:
                raise ValueError("Agent tools exceed the referenced MCP policy")
            policy_domains = set(snapshot["config"].get("allowed_domains", []))
            agent_domains = set(agent_policy.get("allowed_domains", []))
            if policy_domains and (not agent_domains or not agent_domains <= policy_domains):
                raise ValueError("Agent domains exceed the referenced MCP policy")
    return snapshots


async def guard_agent_security_policies(
    runtime: Any,
    tenant: str,
    agent_config: dict[str, Any],
    settings: Any,
    detector: Any,
) -> None:
    expected = agent_config.get("security_policy_snapshots", [])
    resolved = await resolve_agent_security_policies(runtime, tenant, agent_config)
    def identity(row):
        return (
            row.get("policy_id"),
            row.get("version"),
            row.get("kind"),
            row.get("server_fingerprint"),
        )
    if [identity(row) for row in resolved] != [identity(row) for row in expected]:
        raise PermissionError("Agent security policy snapshot is no longer valid")
    for snapshot in resolved:
        try:
            current = server_fingerprint(snapshot["config"], settings, detector)
        except ValueError as exc:
            raise PermissionError(str(exc)) from exc
        if not hmac.compare_digest(current, snapshot["server_fingerprint"]):
            raise PermissionError("Server security configuration differs from Agent policy")
