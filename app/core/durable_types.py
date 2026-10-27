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
    "location",
    "tool",
    "action_id",
)


def search_text(kind: str, data: Any) -> str:
    """Index bounded metadata only; private prompts and raw content stay out."""
    if kind not in RECORD_KINDS or not isinstance(data, dict):
        return ""
    safe = sanitize_record(kind, data)
    values = [safe[name] for name in _SEARCH_FIELDS if isinstance(safe.get(name), str)]
    return " ".join(value[:512] for value in values)[:8_192]
