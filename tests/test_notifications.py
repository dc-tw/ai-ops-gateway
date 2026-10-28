import hashlib
import hmac
import json

import httpx
import pytest

from app.core.config import Settings
from app.core.outbound_notifications import (
    NotificationDelivery,
    dead_letter,
    destinations,
    transition_event,
)


def settings(**changes):
    return Settings(
        _env_file=None,
        app_env="production",
        gateway_public_url="https://gateway.example",
        notification_allowed_hosts=["hooks.example"],
        notification_webhooks_json=json.dumps(
            {
                "acme": [
                    {
                        "id": "operations",
                        "url": "https://hooks.example/gateway",
                        "secret": "s" * 32,
                    }
                ]
            }
        ),
        **changes,
    )


def test_transition_events_are_safe_and_only_emit_once_per_transition():
    goal = {"status": "failed", "error_type": "ProviderUnavailable", "objective": "PRIVATE"}
    event = transition_event("acme", "goal-1", "running", goal)
    assert event["type"] == "goal.failed"
    assert event["data"] == {
        "goal_id": "goal-1",
        "status": "failed",
        "error_type": "ProviderUnavailable",
    }
    assert "PRIVATE" not in json.dumps(event)
    assert transition_event("acme", "goal-1", "failed", goal) is None


@pytest.mark.asyncio
async def test_https_delivery_is_allowlisted_signed_and_has_login_link_without_credentials():
    captured = {}

    async def receive(request: httpx.Request):
        captured["request"] = request
        return httpx.Response(204)

    client = httpx.AsyncClient(transport=httpx.MockTransport(receive))
    runtime = type("Runtime", (), {"_client": object()})()
    delivery = NotificationDelivery(runtime, settings(), client)
    event = transition_event(
        "acme",
        "goal-1",
        "running",
        {
            "status": "waiting_approval",
            "pending_approval": {"approval_id": "approval-1", "expires_at": "soon"},
        },
    )
    assert await delivery._deliver("acme", event) == []
    request = captured["request"]
    body = request.content
    timestamp = request.headers["x-gateway-timestamp"]
    expected = hmac.new(
        ("s" * 32).encode(), timestamp.encode() + b"." + body, hashlib.sha256
    ).hexdigest()
    assert request.headers["x-gateway-signature"] == f"sha256={expected}"
    assert request.headers["x-gateway-event-id"] == event["event_id"]
    payload = json.loads(body)
    assert payload["console_url"] == "https://gateway.example/console/#goals"
    assert "secret" not in json.dumps(payload).lower()
    await client.aclose()


def test_destination_rejects_non_https_or_non_allowlisted_urls():
    configured = settings()
    configured.notification_webhooks_json = json.dumps(
        {"acme": [{"url": "http://127.0.0.1/hook", "secret": "s" * 32}]}
    )
    with pytest.raises(ValueError, match="allowlisted"):
        destinations(configured, "acme")


class Pipeline:
    def __init__(self, client):
        self.client = client
        self.commands = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    def __getattr__(self, name):
        def queue(*args, **kwargs):
            self.commands.append((name, args, kwargs))
            return self

        return queue

    async def execute(self):
        for name, args, kwargs in self.commands:
            await getattr(self.client, name)(*args, **kwargs)


class Redis:
    def __init__(self):
        self.streams = {}
        self.acked = []

    def pipeline(self, **kwargs):
        return Pipeline(self)

    async def incr(self, key):
        return 1

    async def expire(self, key, ttl):
        return True

    async def xadd(self, stream, fields):
        self.streams.setdefault(stream, []).append(fields)
        return "1-0"

    async def xack(self, stream, group, stream_id):
        self.acked.append((stream, group, stream_id))
        return 1

    async def delete(self, key):
        return 1


@pytest.mark.asyncio
async def test_invalid_event_reaches_bounded_failure_queue_without_retrying_forever():
    redis = Redis()
    runtime = type("Runtime", (), {"_client": redis})()
    delivery = NotificationDelivery(runtime, settings(notification_max_attempts=1))
    assert await delivery._handle("acme", "1-0", {"payload": "PRIVATE-NOT-JSON"})
    row = redis.streams[dead_letter("acme")][0]
    failure = json.loads(row["payload"])
    assert failure["attempts"] == 1
    assert failure["failed_destination_ids"] == ["invalid_event"]
    assert "PRIVATE-NOT-JSON" not in row["payload"]
