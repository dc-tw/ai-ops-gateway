"""EventEnvelope v1 and lossless adapters for legacy Redis/SSE payloads."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

EVENT_TYPE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)+$")
ActorType = Literal["human", "agent", "system", "service", "unknown"]
ACTOR_TYPES = {"human", "agent", "system", "service", "unknown"}

EVENT_TYPE_CATALOG = {
    "accepted": "run.accepted",
    "agent_event": "agent.event",
    "approval_decided": "approval.decided",
    "approval_required": "approval.required",
    "cancelled": "run.cancelled",
    "completed": "run.completed",
    "draft_token": "model.draft_token",
    "error": "run.failed",
    "goal_cancelled": "goal.cancelled",
    "goal_completed": "goal.completed",
    "goal_failed": "goal.failed",
    "goal_planned": "goal.planned",
    "goal_queued": "goal.queued",
    "goal_recovered": "goal.recovered",
    "goal_status": "goal.status",
    "goal_step": "goal.step",
    "goal_waiting_approval": "approval.waiting",
    "model_usage": "model.usage",
    "threat_detection": "threat.detected",
    "token": "model.token",
}


class UnsupportedEventVersion(ValueError):
    pass


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    event_id: str = Field(min_length=1, max_length=200)
    event_type: str = Field(min_length=3, max_length=200, pattern=EVENT_TYPE.pattern)
    tenant_id: str = Field(min_length=1, max_length=64)
    actor_type: ActorType = "unknown"
    actor_id: str = Field(default="unknown", min_length=1, max_length=200)
    conversation_id: str | None = Field(default=None, max_length=128)
    run_id: str | None = Field(default=None, max_length=128)
    parent_event_id: str | None = Field(default=None, max_length=200)
    correlation_id: str = Field(min_length=1, max_length=200)
    idempotency_key: str = Field(min_length=1, max_length=300)
    created_at: datetime
    payload: dict[str, Any]
    prev_hash: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    hash: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")

    @field_validator("created_at")
    @classmethod
    def timezone_required(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("EventEnvelope timestamp must include a timezone")
        return value

    @model_validator(mode="after")
    def distinct_idempotency_identity(self) -> EventEnvelope:
        if self.event_id == self.idempotency_key:
            raise ValueError("Event ID and idempotency key must be distinct")
        return self

    @classmethod
    def from_legacy(
        cls,
        event: dict[str, Any],
        *,
        tenant_id: str,
        category: str,
        resource_id: str,
        actor_type: str = "unknown",
        actor_id: str = "unknown",
        conversation_id: str | None = None,
        parent_event_id: str | None = None,
        event_id: str | None = None,
        idempotency_key: str | None = None,
        created_at: str | None = None,
    ) -> EventEnvelope:
        legacy_type = str(event.get("type") or event.get("event_type") or "event")
        event_type = EVENT_TYPE_CATALOG.get(legacy_type)
        if event_type is None and EVENT_TYPE.fullmatch(legacy_type):
            event_type = legacy_type
        if event_type is None:
            normalized = re.sub(r"[^a-z0-9]+", ".", legacy_type.lower()).strip(".")[:193]
            event_type = f"legacy.{normalized or 'event'}"
        event_id = event_id or str(uuid4())
        if actor_type not in ACTOR_TYPES:
            actor_type = "unknown"
        return cls(
            event_id=event_id,
            event_type=event_type,
            tenant_id=tenant_id,
            actor_type=actor_type,
            actor_id=actor_id or "unknown",
            conversation_id=conversation_id,
            run_id=resource_id if category == "run" else None,
            parent_event_id=parent_event_id,
            correlation_id=resource_id,
            idempotency_key=idempotency_key or f"{category}:{resource_id}:{uuid4()}",
            created_at=created_at or datetime.now(UTC),
            payload=event,
        )


def is_envelope(value: Any) -> bool:
    return isinstance(value, dict) and {"schema_version", "event_type", "payload"} <= set(value)


def encode_event(
    event: dict[str, Any],
    *,
    tenant_id: str,
    category: str,
    resource_id: str,
    actor_type: str = "unknown",
    actor_id: str = "unknown",
    conversation_id: str | None = None,
    parent_event_id: str | None = None,
    event_id: str | None = None,
    idempotency_key: str | None = None,
) -> str:
    envelope = EventEnvelope.from_legacy(
        event,
        tenant_id=tenant_id,
        category=category,
        resource_id=resource_id,
        actor_type=actor_type,
        actor_id=actor_id,
        conversation_id=conversation_id,
        parent_event_id=parent_event_id,
        event_id=event_id,
        idempotency_key=idempotency_key,
    )
    return envelope.model_dump_json()


def decode_event(
    raw: str | bytes | dict[str, Any],
    *,
    tenant_id: str,
    category: str,
    resource_id: str,
    cursor: str,
) -> tuple[EventEnvelope, dict[str, Any]]:
    value = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
    if not isinstance(value, dict):
        raise ValueError("Event payload must be an object")
    candidate = "schema_version" in value and ("event_type" in value or "payload" in value)
    if candidate:
        if value.get("schema_version") != 1:
            raise UnsupportedEventVersion("Unsupported EventEnvelope version")
        envelope = EventEnvelope.model_validate(value)
        if envelope.tenant_id != tenant_id:
            raise ValueError("EventEnvelope tenant does not match its stream")
        payload_tenant = envelope.payload.get("tenant_id")
        if payload_tenant is not None and payload_tenant != tenant_id:
            raise ValueError("EventEnvelope payload tenant does not match its stream")
        return envelope, envelope.payload
    stable_id = str(uuid5(NAMESPACE_URL, f"{tenant_id}:{category}:{resource_id}:{cursor}"))
    envelope = EventEnvelope.from_legacy(
        value,
        tenant_id=tenant_id,
        category=category,
        resource_id=resource_id,
        event_id=stable_id,
        idempotency_key=f"legacy:{category}:{resource_id}:{cursor}",
    )
    return envelope, value
