"""Durable, secret-free notification outbox and signed HTTPS delivery."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

import httpx
from redis.exceptions import ResponseError

TENANTS = "gateway:notifications:tenants"
GROUP = "notification-delivery-v1"
EVENT_TYPES = {"goal.completed", "goal.failed", "approval.waiting", "approval.timeout"}


def outbox(tenant: str) -> str:
    return f"tenant:{tenant}:notifications:outbox"


def dead_letter(tenant: str) -> str:
    return f"tenant:{tenant}:notifications:dead-letter"


def transition_event(
    tenant: str, goal_id: str, prior_status: str | None, goal: dict[str, Any]
) -> dict[str, Any] | None:
    status = goal.get("status")
    event_type = None
    data: dict[str, Any] = {"goal_id": goal_id, "status": status}
    if status == "completed" and prior_status != "completed":
        event_type = "goal.completed"
    elif status == "failed" and prior_status != "failed":
        event_type = "goal.failed"
        data["error_type"] = str(goal.get("error_type", "GoalExecutionFailed"))[:128]
    elif status == "waiting_approval" and prior_status != "waiting_approval":
        event_type = "approval.waiting"
        pending = goal.get("pending_approval") or {}
        data.update(
            approval_id=str(pending.get("approval_id", ""))[:128],
            expires_at=pending.get("expires_at"),
        )
    elif status == "cancelled":
        decision = goal.get("approval_decision") or {}
        if decision.get("reason") == "approval_expired" and prior_status != "cancelled":
            event_type = "approval.timeout"
            data["approval_id"] = str(decision.get("approval_id", ""))[:128]
    if event_type is None:
        return None
    return {
        "schema_version": 1,
        "event_id": str(uuid4()),
        "type": event_type,
        "tenant_id": tenant,
        "occurred_at": datetime.now(UTC).isoformat(),
        "data": data,
    }


def queue_transition(pipe: Any, tenant: str, event: dict[str, Any] | None) -> None:
    if event is None:
        return
    pipe.sadd(TENANTS, tenant)
    pipe.xadd(outbox(tenant), {"payload": json.dumps(event, separators=(",", ":"))})


def destinations(settings: Any, tenant: str) -> list[dict[str, Any]]:
    try:
        configured = json.loads(settings.notification_webhooks_json or "{}")
    except (TypeError, ValueError):
        raise ValueError("Notification webhook configuration is invalid") from None
    rows = configured.get(tenant, []) if isinstance(configured, dict) else []
    if not isinstance(rows, list):
        raise ValueError("Notification tenant configuration must be a list")
    allowed = {host.lower() for host in settings.notification_allowed_hosts}
    result = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError("Notification destination must be an object")
        url, secret = row.get("url"), row.get("secret")
        parsed = urlparse(url) if isinstance(url, str) else None
        if (
            parsed is None
            or parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or parsed.hostname.lower() not in allowed
            or not isinstance(secret, str)
            or len(secret) < 32
        ):
            raise ValueError("Notification destination is not HTTPS allowlisted")
        events = row.get("events", sorted(EVENT_TYPES))
        if not isinstance(events, list) or not set(events) <= EVENT_TYPES:
            raise ValueError("Notification event filter is invalid")
        result.append(
            {
                "destination_id": str(row.get("id", f"destination-{index + 1}"))[:128],
                "url": url,
                "secret": secret,
                "events": set(events),
            }
        )
    return result


class NotificationDelivery:
    def __init__(
        self, runtime: Any, settings: Any, client: httpx.AsyncClient | None = None
    ) -> None:
        self.runtime = runtime
        self.redis = runtime._client
        self.settings = settings
        self.http = client

    async def _ensure_group(self, tenant: str) -> None:
        try:
            await self.redis.xgroup_create(outbox(tenant), GROUP, id="0-0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _deliver(self, tenant: str, event: dict[str, Any]) -> list[str]:
        failures = []
        body = dict(event)
        if self.settings.gateway_public_url:
            body["console_url"] = self.settings.gateway_public_url.rstrip("/") + "/console/#goals"
        encoded = json.dumps(
            body, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        timestamp = str(int(time.time()))
        client = self.http or httpx.AsyncClient(timeout=10.0, follow_redirects=False)
        owned = self.http is None
        try:
            for target in destinations(self.settings, tenant):
                if event["type"] not in target["events"]:
                    continue
                signature = hmac.new(
                    target["secret"].encode("utf-8"),
                    timestamp.encode("ascii") + b"." + encoded,
                    hashlib.sha256,
                ).hexdigest()
                try:
                    response = await client.post(
                        target["url"],
                        content=encoded,
                        headers={
                            "content-type": "application/json",
                            "x-gateway-event-id": event["event_id"],
                            "x-gateway-timestamp": timestamp,
                            "x-gateway-signature": f"sha256={signature}",
                        },
                    )
                    if not 200 <= response.status_code < 300:
                        failures.append(target["destination_id"])
                except httpx.HTTPError:
                    failures.append(target["destination_id"])
        finally:
            if owned:
                await client.aclose()
        return failures

    async def _handle(self, tenant: str, stream_id: str, fields: dict[str, str]) -> bool:
        try:
            event = json.loads(fields.get("payload", ""))
        except (TypeError, ValueError):
            event = None
        valid = (
            isinstance(event, dict)
            and event.get("schema_version") == 1
            and event.get("tenant_id") == tenant
            and event.get("type") in EVENT_TYPES
            and isinstance(event.get("event_id"), str)
        )
        failures = ["invalid_event"] if not valid else await self._deliver(tenant, event)
        if not failures:
            await self.redis.xack(outbox(tenant), GROUP, stream_id)
            await self.redis.delete(f"gateway:notifications:attempt:{tenant}:{stream_id}")
            return True
        attempt_key = f"gateway:notifications:attempt:{tenant}:{stream_id}"
        attempts = await self.redis.incr(attempt_key)
        await self.redis.expire(attempt_key, 86_400)
        if attempts < self.settings.notification_max_attempts:
            return False
        failure = {
            "schema_version": 1,
            "stream_id": stream_id,
            "attempts": attempts,
            "failed_destination_ids": failures,
            "event": event if valid else None,
            "failed_at": datetime.now(UTC).isoformat(),
        }
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.xadd(
                dead_letter(tenant),
                {"payload": json.dumps(failure, separators=(",", ":"))},
            )
            pipe.xack(outbox(tenant), GROUP, stream_id)
            pipe.delete(attempt_key)
            await pipe.execute()
        return True

    async def process_once(self, consumer: str) -> int:
        processed = 0
        tenants = sorted(await self.redis.smembers(TENANTS))
        for tenant in tenants:
            await self._ensure_group(tenant)
            claimed = await self.redis.xautoclaim(
                outbox(tenant),
                GROUP,
                consumer,
                self.settings.notification_retry_seconds * 1_000,
                "0-0",
                count=20,
            )
            records = list(claimed[1])
            fresh = await self.redis.xreadgroup(
                GROUP, consumer, {outbox(tenant): ">"}, count=20, block=1
            )
            records.extend(item for _, batch in fresh for item in batch)
            seen = set()
            for stream_id, fields in records:
                if stream_id in seen:
                    continue
                seen.add(stream_id)
                await self._handle(tenant, stream_id, fields)
                processed += 1
        return processed

    async def failures(self, tenant: str, limit: int = 100) -> list[dict[str, Any]]:
        rows = await self.redis.xrevrange(dead_letter(tenant), count=limit)
        result = []
        for stream_id, fields in rows:
            try:
                value = json.loads(fields["payload"])
            except (KeyError, TypeError, ValueError):
                continue
            if isinstance(value, dict):
                result.append({"failure_id": stream_id, **value})
        return result

    async def redrive(self, tenant: str, failure_id: str, subject: str) -> dict[str, Any] | None:
        rows = await self.redis.xrange(
            dead_letter(tenant), min=failure_id, max=failure_id, count=1
        )
        if not rows:
            return None
        try:
            failure = json.loads(rows[0][1]["payload"])
            event = failure["event"]
        except (KeyError, TypeError, ValueError):
            return None
        if (
            not isinstance(event, dict)
            or event.get("tenant_id") != tenant
            or event.get("type") not in EVENT_TYPES
        ):
            return None
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.xadd(outbox(tenant), {"payload": json.dumps(event, separators=(",", ":"))})
            pipe.xadd(
                f"tenant:{tenant}:audit",
                {
                    "payload": json.dumps(
                        {
                            "action": "notification.redrive",
                            "failure_id": failure_id,
                            "event_id": event["event_id"],
                            "subject": subject,
                            "time": datetime.now(UTC).isoformat(),
                        },
                        separators=(",", ":"),
                    )
                },
            )
            await pipe.execute()
        return event
