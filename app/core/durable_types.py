"""Explicit allowlist for archival Redis keys and safe history-search metadata.

Archive records are evidence, never an alternative execution-authority store.
Unknown keys (especially credentials, quotas, leases and checkpoints) fail closed.
"""

from __future__ import annotations

import re
from typing import Any, Literal

from app.core.security import TENANT_PATTERN

ArchiveMode = Literal["record", "events", "messages"]
RecordKind = Literal[
    "agent",
    "skill",
    "skill_revision",
    "planning_session",
    "planning_revision",
    "goal",
    "run",
    "finding",
    "automation",
    "tool_receipt",
    "conversation",
]
EventKind = Literal["run", "goal", "planning", "automation", "audit", "conversation"]

RECORD_KINDS = frozenset(RecordKind.__args__)
EVENT_KINDS = frozenset(EventKind.__args__)
_RELATION_KINDS = frozenset(
    {"agent", "skill", "planning_session", "goal", "run", "automation", "conversation", "finding"}
)

# Persisted resource identifiers are bounded slugs/UUIDs. Conversation IDs also
# allow ':', so their entire body must be retained rather than split by position.
_ID = r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}"
_CONVERSATION = re.compile(
    r"^conversation:([A-Za-z0-9][A-Za-z0-9._:-]{0,127}):(messages|summary|memory)$"
)
_RECORDS = (
    ("agent", re.compile(rf"^agents:record:({_ID})$")),
    ("skill", re.compile(rf"^skills:record:({_ID})$")),
    ("skill_revision", re.compile(rf"^skills:revision:({_ID}:[1-9][0-9]*)$")),
    ("planning_session", re.compile(rf"^planning:session:({_ID})$")),
    ("planning_revision", re.compile(rf"^planning:revision:({_ID}:[1-9][0-9]*)$")),
    ("goal", re.compile(rf"^agent:goal:({_ID})$")),
    ("goal", re.compile(rf"^workflow:v1:goal:({_ID})$")),
    ("run", re.compile(rf"^agent:run:({_ID})$")),
    ("finding", re.compile(rf"^security:threat-finding:({_ID})$")),
    ("automation", re.compile(rf"^automation:({_ID})$")),
    ("tool_receipt", re.compile(rf"^goal:({_ID}):tool-receipt:({_ID})$")),
)
_EVENTS = (
    ("run", re.compile(rf"^agent:events:({_ID})$")),
    ("goal", re.compile(rf"^agent:goal:events:({_ID})$")),
    ("planning", re.compile(rf"^planning:events:({_ID})$")),
    ("automation", re.compile(rf"^automation:({_ID}):events$")),
)


def tenant_from_key(key: str) -> str | None:
    """Return a validated tenant, without accepting a lookalike global key."""
    if not isinstance(key, str) or not key.startswith("tenant:"):
        return None
    tenant, separator, _ = key[len("tenant:") :].partition(":")
    return tenant if separator and TENANT_PATTERN.fullmatch(tenant) else None


def _suffix(key: str) -> str | None:
    tenant = tenant_from_key(key)
    return key[len(f"tenant:{tenant}:") :] if tenant is not None else None


def conversation_id_from_key(key: str) -> str | None:
    """Recognize all deleted conversation components for archive tombstones."""
    suffix = _suffix(key)
    match = _CONVERSATION.fullmatch(suffix) if suffix else None
    return match[1] if match else None


def classify_key(key: str) -> tuple[str, str, ArchiveMode] | None:
    """Classify only supported durable records/streams, not Redis internals."""
    suffix = _suffix(key)
    if suffix is None:
        return None
    if suffix == "audit":
        return "audit", "audit", "events"
    for kind, expression in _RECORDS:
        match = expression.fullmatch(suffix)
        if match:
            resource = ":".join(match.groups())
            return kind, resource, "record"
    for kind, expression in _EVENTS:
        match = expression.fullmatch(suffix)
        if match:
            return kind, match[1], "events"
    conversation = _CONVERSATION.fullmatch(suffix)
    if conversation and conversation[2] == "messages":
        return "conversation", conversation[1], "messages"
    return None


def sanitize_record(kind: str, data: Any) -> Any:
    """Receipts contain only reconciliation metadata, never tool arguments."""
    if kind != "tool_receipt":
        return data
    if not isinstance(data, dict):
        return {}
    return {
        name: data[name]
        for name in ("status", "tool", "action_id")
        if isinstance(data.get(name), str)
    }


_SEARCH_FIELDS = (
    "agent_id",
    "skill_id",
    "session_id",
    "goal_id",
    "request_id",
    "finding_id",
    "automation_id",
    "name",
    "display_name",
    "title",
    "role",
    "status",
    "mode",
    "rule_id",
    "severity",
    "review_status",
    "event_type",
    "type",
    "location",
    "tool",
    "action_id",
    "action",
    "subject",
    "node_id",
)


def _bounded_text(values: list[str]) -> str:
    return " ".join(value[:2_048] for value in values if value.strip())[:32_768]


def _metadata_text(data: dict[str, Any]) -> list[str]:
    return [data[name] for name in _SEARCH_FIELDS if isinstance(data.get(name), str)]


def _goal_body(data: dict[str, Any]) -> list[str]:
    """Return only content that passed the live input/output safety boundary."""
    if data.get("history_search_approved") is not True:
        return []
    values = [data.get("objective", "")]
    for step in data.get("steps", []):
        if not isinstance(step, dict):
            continue
        values.append(step.get("query", ""))
        if step.get("status") == "completed":
            values.append(step.get("answer", ""))
    return [value for value in values if isinstance(value, str)]


def _provenance_text(data: dict[str, Any]) -> list[str]:
    provenance = data.get("provenance")
    if not isinstance(provenance, dict):
        return []
    # Provenance is operator-approved package identity, never package content.
    return [
        value
        for name in ("source", "repository", "commit", "package_sha256")
        if isinstance((value := provenance.get(name)), str)
    ]


def search_text(kind: str, data: Any) -> str:
    """Index safe metadata plus explicitly approved, bounded record content."""
    if kind not in RECORD_KINDS or not isinstance(data, dict):
        return ""
    safe = sanitize_record(kind, data)
    values = _metadata_text(safe)
    if kind == "goal":
        values.extend(_goal_body(safe))
    if kind in {"skill", "skill_revision"}:
        values.extend(_provenance_text(safe))
    return _bounded_text(values)


def event_search_text(kind: str, data: Any) -> str:
    """Index safe event summaries without raw ATR input, prompts or tool payloads."""
    if kind not in EVENT_KINDS or not isinstance(data, dict):
        return ""
    values = _metadata_text(data)
    if kind == "goal":
        event_type = data.get("type")
        if event_type == "goal_completed" and isinstance(data.get("goal"), dict):
            values.extend(_goal_body(data["goal"]))
        elif event_type == "goal_step" and data.get("history_search_approved") is True:
            step = data.get("step")
            if isinstance(step, dict):
                values.extend(
                    value
                    for name in ("query", "answer")
                    if isinstance((value := step.get(name)), str)
                )
        elif event_type == "agent_event" and data.get("history_search_approved") is True:
            event = data.get("event")
            if isinstance(event, dict) and event.get("type") == "completed":
                if isinstance(event.get("answer"), str):
                    values.append(event["answer"])
                report = event.get("evidence_report")
                if isinstance(report, dict):
                    for source in report.get("evidence_sources", []):
                        if not isinstance(source, dict):
                            continue
                        values.extend(
                            value
                            for name in ("url", "content_sha256")
                            if isinstance((value := source.get(name)), str)
                        )
    return _bounded_text(values)


def relation_refs(
    document_type: str, kind: str, resource_id: str, data: Any
) -> list[dict[str, str]]:
    """Extract bounded declared relationships; authorization is intentionally external."""
    if not isinstance(data, dict) or document_type not in {"record", "event"}:
        return []
    candidates: list[tuple[str, str, str]] = []

    def add(field: str, target_kind: str, relation: str, source: dict[str, Any] = data):
        value = source.get(field)
        if isinstance(value, str) and re.fullmatch(_ID, value):
            candidates.append((target_kind, value, relation))

    for field, target_kind, relation in (
        ("goal_id", "goal", "goal"),
        ("parent_goal_id", "goal", "parent_goal"),
        ("root_goal_id", "goal", "root_goal"),
        ("agent_id", "agent", "agent"),
        ("automation_id", "automation", "automation"),
        ("session_id", "planning_session", "planning_session"),
        ("finding_id", "finding", "finding"),
    ):
        add(field, target_kind, relation)
    nested_goal = data.get("goal")
    if isinstance(nested_goal, dict):
        for field, target_kind, relation in (
            ("goal_id", "goal", "goal"),
            ("parent_goal_id", "goal", "parent_goal"),
            ("root_goal_id", "goal", "root_goal"),
            ("agent_id", "agent", "agent"),
            ("automation_id", "automation", "automation"),
        ):
            add(field, target_kind, relation, nested_goal)
    if document_type == "record" and kind == "goal":
        candidates.append(("conversation", resource_id, "conversation"))
    if document_type == "record" and kind == "skill_revision":
        candidates.append(("skill", resource_id.split(":", 1)[0], "skill"))
    if document_type == "record" and kind == "planning_revision":
        candidates.append(
            ("planning_session", resource_id.split(":", 1)[0], "planning_session")
        )
    unique = []
    seen = set()
    for target_kind, target_id, relation in candidates:
        identity = (target_kind, target_id, relation)
        if target_kind in _RELATION_KINDS and identity not in seen:
            unique.append(
                {"kind": target_kind, "resource_id": target_id, "relation": relation}
            )
            seen.add(identity)
        if len(unique) == 32:
            break
    return unique
