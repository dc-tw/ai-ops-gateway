"""EventEnvelope schema, compatibility, ordering and privacy contracts."""

from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import uuid4

import jsonschema
import pytest

from app.core.event_envelope import (
    EVENT_TYPE_CATALOG,
    UnsupportedEventVersion,
    decode_event,
    encode_event,
)
from app.core.internal_events import build_internal_event, encode_internal_event
from app.core.redis_runtime import RedisRuntime


def test_v1_matches_published_json_schema_and_separates_identifiers():
    raw = encode_event(
        {"type": "completed", "answer": "safe"},
        tenant_id="acme",
        category="run",
        resource_id="run-1",
        actor_type="agent",
        actor_id="researcher",
        conversation_id="conversation-1",
        idempotency_key="request:run-1:completed",
    )
    value = json.loads(raw)
    schema = json.loads(
        (Path(__file__).parents[1] / "schemas/event-envelope-v1.schema.json").read_text()
    )
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(
        value
    )
    assert value["event_type"] == "run.completed"
    assert value["event_id"] != value["idempotency_key"]
    assert "cursor" not in value
    assert value["payload"] == {"type": "completed", "answer": "safe"}
    catalog = json.loads(
        (Path(__file__).parents[1] / "schemas/event-types-v1.json").read_text()
    )
    assert catalog["legacy_mappings"] == EVENT_TYPE_CATALOG


def test_legacy_adapter_is_lossless_deterministic_and_rejects_unknown_envelopes():
    legacy = {"type": "goal_step", "index": 2, "status": "completed"}
    first, public = decode_event(
        json.dumps(legacy),
        tenant_id="acme",
        category="goal",
        resource_id="goal-1",
        cursor="10-0",
    )
    second, _ = decode_event(
        legacy,
        tenant_id="acme",
        category="goal",
        resource_id="goal-1",
        cursor="10-0",
    )
    assert public == legacy
    assert first.event_id == second.event_id
    assert first.idempotency_key == "legacy:goal:goal-1:10-0"
    invalid = first.model_dump(mode="json") | {"schema_version": 2}
    with pytest.raises(UnsupportedEventVersion):
        decode_event(
            invalid,
            tenant_id="acme",
            category="goal",
            resource_id="goal-1",
            cursor="10-0",
        )
    foreign = first.model_dump(mode="json") | {"tenant_id": "other"}
    with pytest.raises(ValueError, match="tenant"):
        decode_event(
            foreign,
            tenant_id="acme",
            category="goal",
            resource_id="goal-1",
            cursor="10-0",
        )


def test_internal_threat_envelope_excludes_unapproved_raw_content():
    attack = "PRIVATE RAW ATTACK TEXT"
    event = build_internal_event(
        "threat.detected",
        "finding-1",
        {
            "rule_id": "ATR-1",
            "severity": "high",
            "enforcement_action": "block",
            "raw_content": attack,
        },
    )
    encoded = encode_internal_event(event, "acme")
    assert attack not in encoded
    assert json.loads(encoded)["payload"]["data"]["findings"][0]["rule_id"] == "ATR-1"


@pytest.mark.asyncio
async def test_real_redis_mixed_legacy_and_v1_replay_preserves_cursor_order():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("EventEnvelope Redis compatibility requires TEST_REDIS_URL")
    runtime = RedisRuntime(url)
    tenant, run_id = "events-" + uuid4().hex, "run-" + uuid4().hex
    stream = runtime._tenant_key(tenant, f"agent:events:{run_id}")
    try:
        legacy_cursor = await runtime._client.xadd(
            stream, {"payload": json.dumps({"type": "token", "content": "old"})}
        )
        new_cursor = await runtime.append_event(
            run_id, {"type": "completed", "answer": "new"}, tenant
        )
        public = await runtime.read_agent_events(run_id, tenant_id=tenant, block_ms=1)
        envelopes = await runtime.read_agent_event_envelopes(
            run_id, tenant_id=tenant, block_ms=1
        )
        assert [cursor for cursor, _ in public] == [legacy_cursor, new_cursor]
        assert [event["type"] for _, event in public] == ["token", "completed"]
        assert [cursor for cursor, _ in envelopes] == [legacy_cursor, new_cursor]
        assert envelopes[0][1]["event_type"] == "model.token"
        assert envelopes[1][1]["event_type"] == "run.completed"
        assert envelopes[0][1]["event_id"] != legacy_cursor
    finally:
        await runtime._client.delete(stream)
        await runtime.close()
