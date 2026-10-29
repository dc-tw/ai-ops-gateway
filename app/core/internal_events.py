"""Pure, privacy-safe payloads for trusted server-origin automation events."""

from __future__ import annotations

from typing import Any

from app.core.event_envelope import decode_event, encode_event

INTERNAL_STREAM = "automation:internal-events"


def _metadata(data: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {
        key: data[key]
        for key in fields
        if key in data and isinstance(data[key], (str, int, float, bool))
    }


def build_internal_event(
    event_type: str,
    event_id: str,
    data: dict[str, Any],
    *,
    source_automation_id: str | None = None,
) -> dict[str, Any] | None:
    if source_automation_id:
        return None
    if event_type == "threat.detected":
        findings = data.get("findings", [data] if data.get("rule_id") else [])
        safe = {
            "findings": [
                _metadata(
                    finding,
                    ("finding_id", "rule_id", "severity", "enforcement_action", "review_status"),
                )
                for finding in findings
                if isinstance(finding, dict)
            ]
        }
    elif event_type == "goal.failed":
        safe = _metadata(data, ("goal_id", "status", "error_type"))
    else:
        raise ValueError("Unsupported internal automation event")
    return {"event_id": event_id, "type": event_type, "data": safe}


def encode_internal_event(event: dict[str, Any], tenant_id: str) -> str:
    return encode_event(
        event,
        tenant_id=tenant_id,
        category="automation",
        resource_id=str(event["event_id"]),
        actor_type="system",
        actor_id="gateway",
        event_id=str(event["event_id"]),
        idempotency_key=f"internal:{tenant_id}:{event['event_id']}",
    )


def decode_internal_event(raw: str, tenant_id: str, cursor: str) -> dict[str, Any]:
    return decode_event(
        raw,
        tenant_id=tenant_id,
        category="automation",
        resource_id="internal-events",
        cursor=cursor,
    )[1]
