"""Commit safe decisions before graph state advances, not in its stream consumer.

This journal is execution metadata in Redis, never an alternative authority in
Postgres. Public streams/audit/findings participate in the existing PG outbox.
No scanned content or unkeyed content fingerprint is persisted or published.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from redis.exceptions import WatchError

from app.core.internal_events import (
    INTERNAL_STREAM,
    build_internal_event,
    encode_internal_event,
)


def journal_key(tenant: str, decision_id: str) -> str:
    return f"tenant:{tenant}:safety:decision:{decision_id}"


def encode(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


async def journal_event_id(runtime, tenant, event, public_event, *, kind, resource_id):
    decision_id = event.get("safety_event_id")
    if not decision_id:
        return None
    raw = await runtime._client.get(journal_key(tenant, decision_id))
    record = json.loads(raw) if raw else {}
    if (
        record.get("public_event") != public_event
        or not record.get("stream_id")
        or record.get("kind") != kind
        or record.get("resource_id") != resource_id
    ):
        raise RuntimeError("Safety event failed journal verification")
    return record["stream_id"]


class SafetyJournal:
    def __init__(
        self,
        runtime,
        settings,
        tenant,
        resource_id,
        *,
        model,
        kind="run",
        subject="agent",
        step_index=None,
        source_automation_id=None,
    ):
        self.runtime, self.settings = runtime, settings
        self.tenant, self.resource_id = tenant, resource_id
        self.kind, self.model, self.subject = kind, model, subject
        self.step_index, self.source_automation_id = step_index, source_automation_id

    async def commit(self, scan, identity, content_fingerprint):
        client = self.runtime._client
        secret_key = f"tenant:{self.tenant}:safety:hmac-secret"
        await client.set(secret_key, secrets.token_hex(32), nx=True)
        secret = await client.get(secret_key)
        if not secret:
            raise RuntimeError("Safety journal identity unavailable")
        outcome = scan.to_event()
        outcome["findings"] = [
            {key: value for key, value in item.items() if key != "finding_id"}
            for item in outcome["findings"]
        ]
        message = encode([self.kind, self.resource_id, identity, content_fingerprint, outcome])
        decision_id = hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()
        key = journal_key(self.tenant, decision_id)
        suffix = (
            f"agent:goal:events:{self.resource_id}"
            if self.kind == "goal"
            else f"agent:events:{self.resource_id}"
        )
        stream_key = f"tenant:{self.tenant}:{suffix}"
        ttl = max(
            self.settings.checkpoint_ttl_minutes * 60 + 3600, self.settings.atr_findings_ttl_seconds
        )
        event = scan.to_event() | {"safety_event_id": decision_id}
        event["findings"] = [
            item
            | {"finding_id": str(uuid5(NAMESPACE_URL, f"{decision_id}:{index}:{item['rule_id']}"))}
            for index, item in enumerate(event["findings"])
        ]
        public = (
            {"type": "agent_event", "step_index": self.step_index, "event": event}
            if self.kind == "goal"
            else event
        )
        records = self._findings(event)
        index_key = f"tenant:{self.tenant}:security:threat-findings"
        audit_key = f"tenant:{self.tenant}:audit"
        expected_types = {key: "string", stream_key: "stream", audit_key: "stream"}
        if records:
            expected_types[index_key] = "zset"
            if not self.source_automation_id:
                expected_types[INTERNAL_STREAM] = "stream"
            for record in records:
                expected_types[
                    f"tenant:{self.tenant}:security:threat-finding:{record['finding_id']}"
                ] = "string"
        while True:
            async with client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(*expected_types)
                    # MULTI/EXEC is atomic, but does not roll back a WRONGTYPE
                    # command. Validate and WATCH every destination before writes.
                    for destination, expected in expected_types.items():
                        if await pipe.type(destination) not in {"none", expected}:
                            raise RuntimeError("Safety journal destination unavailable")
                    existing = await pipe.get(key)
                    if existing:
                        return json.loads(existing)["event"]
                    rows = await pipe.xrevrange(stream_key, count=1)
                    last_ms, last_sequence = map(int, rows[0][0].split("-")) if rows else (0, 0)
                    milliseconds = max(time.time_ns() // 1_000_000, last_ms)
                    sequence = last_sequence + 1 if milliseconds == last_ms else 0
                    cursor = f"{milliseconds}-{sequence}" if scan.needs_record else None
                    pipe.multi()
                    if scan.needs_record:
                        pipe.xadd(stream_key, {"payload": encode(public)}, id=cursor)
                    pipe.xadd(
                        audit_key,
                        {
                            "payload": encode(
                                {
                                    "action": f"{self.kind}.safety_decision",
                                    "subject": self.subject,
                                    "context_id": self.resource_id,
                                    "event": event,
                                }
                            )
                        },
                    )
                    for record in records:
                        pipe.set(
                            f"tenant:{self.tenant}:security:threat-finding:{record['finding_id']}",
                            encode(record),
                            ex=self.settings.atr_findings_ttl_seconds,
                        )
                        pipe.zadd(index_key, {record["finding_id"]: time.time()})
                        internal = build_internal_event(
                            "threat.detected",
                            record["finding_id"],
                            record,
                            source_automation_id=self.source_automation_id,
                        )
                        if internal:
                            pipe.xadd(
                                INTERNAL_STREAM,
                                {
                                    "tenant_id": self.tenant,
                                    "payload": encode_internal_event(internal, self.tenant),
                                },
                            )
                    if records:
                        pipe.expire(index_key, self.settings.atr_findings_ttl_seconds)
                        pipe.zremrangebyscore(
                            index_key, "-inf", time.time() - self.settings.atr_findings_ttl_seconds
                        )
                        pipe.zremrangebyrank(
                            index_key, 0, -self.settings.atr_findings_max_records - 1
                        )
                    # Success marker is last. A backend failure never grants
                    # output/tool release; replay consumers still deduplicate IDs.
                    pipe.set(
                        key,
                        encode(
                            {
                                "event": event,
                                "public_event": public,
                                "stream_id": cursor,
                                "kind": self.kind,
                                "resource_id": self.resource_id,
                            }
                        ),
                        ex=ttl,
                    )
                    await pipe.execute()
                    return event
                except WatchError:
                    continue

    def _findings(self, event):
        now = datetime.now(UTC).isoformat()
        return [
            {
                "finding_id": item["finding_id"],
                "context_id": self.resource_id,
                "model": self.model,
                "rule_id": item["rule_id"],
                "title": item["title"],
                "severity": item["severity"],
                "confidence": item["confidence"],
                "event_type": event["event_type"],
                "location": event["location"],
                "block_candidate": item["block_candidate"],
                "enforcement_status": item["enforcement_action"],
                "review_status": "unreviewed",
                "review_reason": "",
                "reviewed_by": "",
                "reviewed_at": "",
                "created_at": now,
            }
            for item in event["findings"]
        ]


def graph_journal(runtime, settings, tenant, resource_id, **kwargs):
    # Lightweight injected test runtimes have no atomic Redis client. Production
    # checkpointed paths always use RedisRuntime, hence always use this journal.
    if runtime is None or not hasattr(runtime, "_client"):
        return None
    return SafetyJournal(runtime, settings, tenant, resource_id, **kwargs).commit
