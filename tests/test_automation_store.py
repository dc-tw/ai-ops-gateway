"""Real Redis checks. Set TEST_REDIS_URL to an isolated disposable Redis instance."""

import asyncio
import json
import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.core.automation_store import OCCURRENCE_TTL_SECONDS, AutomationStore
from app.core.redis_runtime import RedisRuntime

NOW = datetime(2026, 10, 25, tzinfo=UTC).timestamp()


@pytest.fixture
async def storage():
    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("Real Redis integration requires TEST_REDIS_URL")
    runtime = RedisRuntime(url, stream_maxlen=1)
    try:
        await runtime.ping()
    except Exception as exc:
        await runtime.close()
        pytest.fail(f"TEST_REDIS_URL is configured but Redis is unavailable: {exc}")
    tenant = f"automation-test-{uuid4().hex}"
    store = AutomationStore(runtime)
    try:
        yield store, tenant
    finally:
        jobs = await store.list(tenant)
        for job in jobs:
            await runtime._client.zrem(
                "automation:due", store._member(tenant, job["automation_id"])
            )
        for queue in ("agent:goal:queue",):
            rows = await runtime._client.xrange(queue)
            ids = [event_id for event_id, fields in rows if fields.get("tenant_id") == tenant]
            if ids:
                await runtime._client.xdel(queue, *ids)
        keys = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}:*")]
        if keys:
            await runtime._client.delete(*keys)
        await runtime._client.srem("automation:tenants", tenant)
        await runtime._client.srem("agent:goal:tenants", tenant)
        await runtime.close()


def job(tenant, automation_id="job-1", cap=24):
    return {
        "automation_id": automation_id,
        "tenant_id": tenant,
        "name": "Example automation",
        "enabled": True,
        "trigger": {"kind": "schedule", "timezone": "Asia/Taipei", "interval_seconds": 60},
        "goal": {"objective": "Review deployment findings", "steps": []},
        "task_type": "agent",
        "policy": {
            "allowed_tools": [],
            "allowed_domains": [],
            "max_runs_per_day": cap,
            "token_budget": 20_000,
            "approval_timeout_seconds": 86_400,
        },
        "agent_id": "ops-agent",
        "created_by": "test-operator",
        "next_run_at": NOW,
        "created_at": datetime.fromtimestamp(NOW, UTC).isoformat(),
        "updated_at": datetime.fromtimestamp(NOW, UTC).isoformat(),
    }


def goal(index=0):
    return {
        "goal_id": f"goal-{index}",
        "objective": f"Task {index}",
        "status": "queued",
        "automation_version": 1,
        "steps": [],
        "policy": {"allowed_tools": []},
        "current_step": 0,
        "created_at": datetime.fromtimestamp(NOW, UTC).isoformat(),
    }


async def test_dispatch_ownership_uses_current_acl_not_payload_and_has_no_implicit_shares(storage):
    from app.core.resource_access import AccessStore, AccessUpdate, acl_key
    from app.core.security import Principal

    store, tenant = storage
    saved = await store.create(job(tenant))
    access = AccessStore(store.runtime)
    # Simulate a legacy job that an admin explicitly adopts under a known owner.
    await store._client.delete(acl_key(tenant, "automation", saved["automation_id"]))
    await access.update(
        Principal("admin", tenant, "admin", "api_key"),
        "automation",
        "job-1",
        AccessUpdate(
            expected_version=0,
            legacy_owner={"subject": "verified-owner"},
            grants=[{"subject": "reader", "permission": "read"}],
        ),
    )
    candidate = goal(99) | {"created_by": "attacker"}
    result = await store.submit(
        tenant,
        "job-1",
        "ownership-event",
        candidate,
        {"trigger_type": "webhook", "created_by": "attacker"},
        NOW,
    )
    assert result["status"] == "queued"
    for kind in ("goal", "conversation"):
        assert await access.can(
            Principal("verified-owner", tenant, "operator", "api_key"), kind, "goal-99"
        )
        assert not await access.can(
            Principal("attacker", tenant, "operator", "api_key"), kind, "goal-99"
        )
        assert not await access.can(
            Principal("reader", tenant, "viewer", "api_key"), kind, "goal-99"
        )


async def test_bad_access_index_cannot_leave_half_created_or_queued_goal(storage):
    from app.core.resource_access import actor_index

    store, tenant = storage
    await store.create(job(tenant))
    index = actor_index(tenant, {"actor_type": "human", "subject": "test-operator"})
    await store._client.delete(index)
    await store._client.set(index, "WRONGTYPE")
    from redis.exceptions import ResponseError

    with pytest.raises(ResponseError, match="Access index unavailable"):
        await store.submit(tenant, "job-1", "bad-index", goal(98), {}, NOW)
    assert await store.runtime.get_goal("goal-98", tenant) is None
    assert not await queue_for(store, tenant)
    assert await store._client.get(store._key(tenant, "job-1", ":active")) is None


async def queue_for(store, tenant):
    rows = await store._client.xrange("agent:goal:queue")
    return [fields for _, fields in rows if fields.get("tenant_id") == tenant]


async def finish(store, tenant, goal_id):
    saved = await store.runtime.get_goal(goal_id, tenant)
    saved["status"] = "completed"
    await store.runtime.save_goal(goal_id, saved, tenant)


async def test_concurrent_duplicate_is_one_durable_goal(storage):
    store, tenant = storage
    await store.create(job(tenant))
    results = await asyncio.gather(
        *[
            store.submit(tenant, "job-1", "same-event", goal(index), {"event_id": "e-1"}, NOW)
            for index in range(20)
        ]
    )
    assert [r["status"] for r in results].count("queued") == 1
    assert [r["status"] for r in results].count("duplicate") == 19
    assert len({r["goal_id"] for r in results}) == 1
    assert len(await queue_for(store, tenant)) == 1
    saved = (await store.list_goals(tenant, "job-1"))[0]
    assert saved["steps"] == []
    assert saved["policy"]["allowed_tools"] == []
    keys = [
        key async for key in store._client.scan_iter(store._key(tenant, "job-1", ":occurrence:*"))
    ]
    assert (
        OCCURRENCE_TTL_SECONDS - 5 <= await store._client.ttl(keys[0]) <= (OCCURRENCE_TTL_SECONDS)
    )


async def test_overlap_is_exactly_one_pending_and_counts_all_occurrences(storage):
    store, tenant = storage
    await store.create(job(tenant))
    first = await store.submit(tenant, "job-1", "first", goal(), {}, NOW)
    results = await asyncio.gather(
        *[
            store.submit(tenant, "job-1", f"event-{i}", goal(i), {"event_id": f"e-{i}"}, NOW)
            for i in range(1, 21)
        ]
    )
    assert all(result["status"] == "coalesced" for result in results)
    assert len(await queue_for(store, tenant)) == 1
    assert (await store.flush_pending(tenant, "job-1", NOW))["status"] == "busy"
    await finish(store, tenant, first["goal_id"])
    flushed = await store.flush_pending(tenant, "job-1", NOW)
    assert flushed["status"] == "queued"
    assert flushed["coalesced_count"] == 20
    assert len(await queue_for(store, tenant)) == 2
    assert len(await store.list_goals(tenant, "job-1")) == 2
    assert (await store.flush_pending(tenant, "job-1", NOW))["status"] == "no_pending"


async def test_daily_limit_preserves_pending_until_local_day_changes(storage):
    store, tenant = storage
    await store.create(job(tenant, cap=1))
    first = await store.submit(tenant, "job-1", "first", goal(), {}, NOW)
    await store.submit(tenant, "job-1", "second", goal(1), {}, NOW)
    await finish(store, tenant, first["goal_id"])
    assert (await store.flush_pending(tenant, "job-1", NOW))["status"] == "rate_limited"
    assert len(await queue_for(store, tenant)) == 1
    # Taipei's next midnight is 16:00 UTC, not UTC midnight.
    result = await store.flush_pending(tenant, "job-1", NOW + 16 * 3600)
    assert result["status"] == "queued"
    assert result["goal_id"] == "goal-1"


async def test_quota_exhaustion_durably_coalesces_new_events_without_retry_inflation(storage):
    store, tenant = storage
    await store.create(job(tenant, cap=1))
    first = await store.submit(tenant, "job-1", "first", goal(), {}, NOW)
    await finish(store, tenant, first["goal_id"])
    for index in range(1, 4):
        deferred = await store.submit(tenant, "job-1", f"event-{index}", goal(index), {}, NOW)
        assert deferred["status"] == "rate_limited"
        repeated = await store.submit(tenant, "job-1", f"event-{index}", goal(index), {}, NOW)
        assert repeated["status"] == "duplicate"
        assert repeated["goal_id"] is None
    assert len(await queue_for(store, tenant)) == 1
    flushed = await store.flush_pending(tenant, "job-1", NOW + 86400)
    assert flushed["status"] == "queued"
    assert flushed["goal_id"] == "goal-3"
    assert flushed["coalesced_count"] == 3


async def test_disabled_removes_due_and_pending_without_stale_schedule_resurrection(storage):
    store, tenant = storage
    await store.create(job(tenant))
    assert (tenant, "job-1") in await store.due(NOW)
    await store.submit(tenant, "job-1", "first", goal(), {}, NOW)
    await store.submit(tenant, "job-1", "second", goal(1), {}, NOW)
    changed = await store.set_enabled(tenant, "job-1", False, "disabled-now")
    assert changed["policy"]["allowed_tools"] == []
    assert changed["goal"]["steps"] == []
    assert not await store.advance_schedule(tenant, "job-1", NOW, NOW + 60)
    assert (tenant, "job-1") not in await store.due(NOW + 3600)
    assert (await store.flush_pending(tenant, "job-1", NOW))["status"] == "disabled"
    await store.set_enabled(tenant, "job-1", True, "enabled-now")
    await store.set_next_run(tenant, "job-1", NOW + 120)
    assert (await store.flush_pending(tenant, "job-1", NOW))["status"] == "no_pending"
    assert (tenant, "job-1") not in await store.due(NOW + 60)
    assert (tenant, "job-1") in await store.due(NOW + 120)


async def test_schedule_advance_compare_and_swap_has_one_winner(storage):
    store, tenant = storage
    await store.create(job(tenant))
    results = await asyncio.gather(
        *[store.advance_schedule(tenant, "job-1", NOW, NOW + 60) for _ in range(20)]
    )
    assert results.count(True) == 1
    saved = await store.get(tenant, "job-1")
    assert saved["next_run_at"] == NOW + 60
    assert saved["enabled"] is True
    assert saved["policy"]["allowed_tools"] == []


async def test_automation_audit_and_sse_do_not_embed_untrusted_payload(storage):
    store, tenant = storage
    await store.create(job(tenant))
    event = {"event_id": "safe-id", "payload": "SECRET_WEBHOOK", "body": {"password": "SECRET"}}
    await store.submit(tenant, "job-1", "event", goal(), event, NOW)
    await store.append_event(tenant, "job-1", {**event, "status": "completed"})
    events = await store.events(tenant, "job-1", block_ms=0)
    assert len(events) == 2
    audit = await store._client.xrange(store.runtime._tenant_key(tenant, "audit"))
    assert "SECRET" not in json.dumps(events)
    assert "SECRET" not in json.dumps(audit)
    saved = (await store.list_goals(tenant, "job-1"))[0]
    assert saved["trigger_metadata"] == {"event_id": "safe-id"}


async def test_occurrence_keys_are_scoped_to_automation(storage):
    store, tenant = storage
    await store.create(job(tenant, "job-1"))
    await store.create(job(tenant, "job-2"))
    first = await store.submit(tenant, "job-1", "shared-event", goal(1), {}, NOW)
    second = await store.submit(tenant, "job-2", "shared-event", goal(2), {}, NOW)
    assert first["status"] == second["status"] == "queued"
    assert len(await queue_for(store, tenant)) == 2
    assert tenant in await store.tenants()
    assert len(await store.list(tenant)) == 2
    foreign_tenant = f"{tenant}-foreign"
    assert await store.get(foreign_tenant, "job-1") is None
    assert await store.list_goals(foreign_tenant, "job-1") == []
    rejected = await store.submit(foreign_tenant, "job-1", "shared-event", goal(3), {}, NOW)
    assert rejected["status"] == "disabled"
    assert rejected["goal_id"] is None


async def test_concurrent_disable_and_schedule_advance_cannot_reenable_job(storage):
    store, tenant = storage
    await store.create(job(tenant))
    await asyncio.gather(
        store.advance_schedule(tenant, "job-1", NOW, NOW + 60),
        store.set_enabled(tenant, "job-1", False, "disabled-now"),
    )
    saved = await store.get(tenant, "job-1")
    assert saved["enabled"] is False
    assert saved["next_run_at"] is None
    assert (tenant, "job-1") not in await store.due(NOW + 120)


async def test_configuration_update_cas_discards_pending_and_rejects_stale_policy(storage):
    store, tenant = storage
    original = await store.create(job(tenant))
    first = await store.submit(tenant, "job-1", "first", goal(), {}, NOW)
    await store.submit(tenant, "job-1", "second", goal(1), {}, NOW)
    changed = {
        **original,
        "updated_at": "updated-now",
        "policy": {
            **original["policy"],
            "max_runs_per_day": 1,
        },
    }
    results = await asyncio.gather(*[store.update(changed, 1) for _ in range(10)])
    assert sum(result is not None for result in results) == 1
    updated = await store.get(tenant, "job-1")
    assert updated["version"] == 2
    assert updated["policy"]["allowed_tools"] == []
    assert not await store.advance_schedule(tenant, "job-1", NOW, NOW + 60, expected_version=1)
    await store.set_next_run(tenant, "job-1", NOW + 60, expected_version=1)
    assert (await store.get(tenant, "job-1"))["next_run_at"] == NOW
    assert (await store.flush_pending(tenant, "job-1", NOW))["status"] == "no_pending"
    await finish(store, tenant, first["goal_id"])
    stale = await store.submit(tenant, "job-1", "third", goal(2), {}, NOW)
    assert stale["status"] == "stale"
    fresh = {**goal(2), "automation_version": 2}
    accepted = await store.submit(tenant, "job-1", "third", fresh, {}, NOW + 86400)
    assert accepted["status"] == "queued"
    events = await store.events(tenant, "job-1", block_ms=0)
    update_event = next(event for _, event in events if event["status"] == "updated")
    assert update_event["pending_discarded"] is True
    assert update_event["coalesced_count"] == 1


async def test_new_occurrence_merges_pending_when_previous_goal_finished(storage):
    store, tenant = storage
    await store.create(job(tenant))
    first = await store.submit(tenant, "job-1", "first", goal(), {}, NOW)
    await store.submit(tenant, "job-1", "second", goal(1), {}, NOW)
    await finish(store, tenant, first["goal_id"])
    latest = await store.submit(tenant, "job-1", "third", goal(2), {}, NOW)
    assert latest["status"] == "queued"
    assert latest["goal_id"] == "goal-2"
    assert latest["coalesced_count"] == 2
    assert len(await queue_for(store, tenant)) == 2
    assert (await store.flush_pending(tenant, "job-1", NOW))["status"] == "no_pending"
