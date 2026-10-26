"""Skill revocation checks at each durable Goal queue admission boundary."""

import json
import os
import time
from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.core import skill_store
from app.core.automation_store import AutomationStore
from app.core.delegation import DelegationStore, _scope_child
from app.core.goal_execution import GoalExecutionStore
from app.core.redis_runtime import RedisRuntime

NOW = datetime(2026, 10, 26, tzinfo=UTC).timestamp()


def snapshot(tenant, skill_id="review-pack", **changes):
    content = skill_store.SkillContent(
        name=skill_id,
        display_name="Private review procedure",
        description="A reusable review procedure",
        instructions="PRIVATE PACK TEXT: cite evidence and identify uncertainty.",
    ).model_dump()
    content.update(changes)
    return content | {
        "skill_id": skill_id,
        "tenant_id": tenant,
        "revision": 1,
        "revocation_epoch": 0,
        "content_sha256": skill_store.snapshot_content_hash(content),
    }


async def seed(runtime, value, **head_changes):
    head = {**value, "enabled": True, **head_changes}
    tenant, skill_id, revision = value["tenant_id"], value["skill_id"], value["revision"]
    await runtime._client.set(
        runtime._tenant_key(tenant, f"skills:record:{skill_id}"), json.dumps(head)
    )
    await runtime._client.set(
        runtime._tenant_key(tenant, f"skills:revision:{skill_id}:{revision}"), json.dumps(value)
    )


async def change_head(runtime, value, **changes):
    key = runtime._tenant_key(value["tenant_id"], f"skills:record:{value['skill_id']}")
    head = json.loads(await runtime._client.get(key))
    await runtime._client.set(key, json.dumps(head | changes))


def goal(tenant, value=None, *, agent_id="worker", role="worker"):
    policy = {"allowed_tools": [], "token_budget": 1_000, "approval_timeout_seconds": 86_400}
    bindings = [{"skill_id": value["skill_id"], "revision": value["revision"]}] if value else []
    return {
        "goal_id": str(uuid4()),
        "tenant_id": tenant,
        "agent_id": agent_id,
        "agent_config": {
            "agent_id": agent_id,
            "role": role,
            "model": "local",
            "delegate_agent_ids": [],
            "policy": deepcopy(policy),
            "skills": bindings,
            "skill_snapshots": [deepcopy(value)] if value else [],
        },
        "model": "local",
        "policy": deepcopy(policy),
        "token_budget": 1_000,
        "objective": "Review the available evidence",
        "status": "queued",
        "steps": [],
        "current_step": 0,
        "automation_version": 1,
    }


def job(tenant, *, cap=24):
    return {
        "automation_id": "skill-job",
        "tenant_id": tenant,
        "enabled": True,
        "agent_id": "worker",
        "trigger": {"kind": "schedule", "timezone": "Asia/Taipei", "interval_seconds": 60},
        "policy": {"max_runs_per_day": cap},
        "next_run_at": NOW,
    }


async def queued(runtime, tenant):
    return [
        fields
        for _, fields in await runtime._client.xrange("agent:goal:queue")
        if fields.get("tenant_id") == tenant
    ]


@pytest.fixture
async def admission():
    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("Skill admission transactions require TEST_REDIS_URL")
    runtime = RedisRuntime(url)
    await runtime.ping()
    tenant = f"skill-admission-{uuid4().hex}"
    try:
        yield runtime, tenant
    finally:
        rows = await runtime._client.xrange("agent:goal:queue")
        ids = [entry for entry, fields in rows if fields.get("tenant_id") == tenant]
        if ids:
            await runtime._client.xdel("agent:goal:queue", *ids)
        await runtime._client.zrem("automation:due", AutomationStore._member(tenant, "skill-job"))
        keys = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}:*")]
        if keys:
            await runtime._client.delete(*keys)
        await runtime._client.srem("agent:goal:tenants", tenant)
        await runtime._client.srem("automation:tenants", tenant)
        await runtime.close()


async def test_manual_skill_admission_pins_revision_even_after_new_publication(admission):
    runtime, tenant = admission
    value = snapshot(tenant)
    await seed(runtime, value)
    await change_head(runtime, value, revision=2, content_sha256="f" * 64)
    submitted = goal(tenant, value)
    await GoalExecutionStore(runtime).submit_manual(submitted)
    saved = await runtime.get_goal(submitted["goal_id"], tenant)
    assert saved["agent_config"]["skill_snapshots"] == [value]
    assert len(await queued(runtime, tenant)) == 1


@pytest.mark.parametrize("invalid", ["disabled", "epoch", "tenant", "revision", "hash"])
async def test_manual_skill_admission_rejects_revocation_and_corrupt_identity(admission, invalid):
    runtime, tenant = admission
    value = snapshot(tenant)
    await seed(runtime, value)
    if invalid == "disabled":
        await change_head(runtime, value, enabled=False)
    elif invalid == "epoch":
        await change_head(runtime, value, enabled=True, revocation_epoch=1)
    elif invalid == "tenant":
        await change_head(runtime, value, tenant_id="foreign")
    else:
        key = runtime._tenant_key(tenant, f"skills:revision:{value['skill_id']}:1")
        revision = deepcopy(value)
        revision["revision" if invalid == "revision" else "content_sha256"] = (
            2 if invalid == "revision" else "f" * 64
        )
        await runtime._client.set(key, json.dumps(revision))
    submitted = goal(tenant, value)
    with pytest.raises(PermissionError):
        await GoalExecutionStore(runtime).submit_manual(submitted)
    assert await runtime.get_goal(submitted["goal_id"], tenant) is None
    assert await queued(runtime, tenant) == []


async def test_manual_disable_between_guard_and_exec_aborts_entire_admission(
    admission, monkeypatch
):
    runtime, tenant = admission
    value = snapshot(tenant)
    await seed(runtime, value)
    original, changed = skill_store.guard_goal_skills, False

    async def revoke_after_watch(runtime, candidate, **kwargs):
        nonlocal changed
        await original(runtime, candidate, **kwargs)
        if kwargs.get("watch") and not changed:
            changed = True
            await change_head(runtime, value, enabled=False, revocation_epoch=1)

    monkeypatch.setattr(skill_store, "guard_goal_skills", revoke_after_watch)
    submitted = goal(tenant, value)
    with pytest.raises(PermissionError):
        await GoalExecutionStore(runtime).submit_manual(submitted)
    assert changed
    assert await queued(runtime, tenant) == []
    assert await runtime.get_goal(submitted["goal_id"], tenant) is None
    assert await runtime._client.xlen(runtime._tenant_key(tenant, "audit")) == 0


async def test_legacy_manual_goal_requires_no_skill_store_or_redis_client():
    class Adapter:
        async def goal_execution_mutate(self, tenant, goal_id, change):
            return change(None).result

    submitted = {"goal_id": "legacy", "tenant_id": "legacy", "status": "queued"}
    assert await GoalExecutionStore(Adapter()).submit_manual(submitted) == submitted


@pytest.mark.parametrize("invalid", ["disabled", "epoch", "tenant", "hash"])
async def test_automation_lua_rejects_revoked_skill_and_consumes_occurrence(admission, invalid):
    runtime, tenant = admission
    store, value = AutomationStore(runtime), snapshot(tenant)
    await store.create(job(tenant))
    await seed(runtime, value)
    if invalid in {"disabled", "epoch", "tenant"}:
        changes = {
            "disabled": {"enabled": False},
            "epoch": {"revocation_epoch": 1},
            "tenant": {"tenant_id": "foreign"},
        }
        await change_head(runtime, value, **changes[invalid])
    else:
        key = runtime._tenant_key(tenant, f"skills:revision:{value['skill_id']}:1")
        await runtime._client.set(key, json.dumps(value | {"content_sha256": "f" * 64}))
    submitted = goal(tenant, value)
    event = {"trigger_type": "schedule", "schedule_due_at": NOW, "schedule_next_at": NOW + 60}
    result = await store.submit(tenant, "skill-job", "occurrence", submitted, event, NOW)
    assert result["status"] == "skills_revoked"
    assert await runtime.get_goal(submitted["goal_id"], tenant) is None
    assert await queued(runtime, tenant) == []
    assert (await store.get(tenant, "skill-job"))["next_run_at"] == NOW + 60
    repeated = await store.submit(tenant, "skill-job", "occurrence", submitted, event, NOW)
    assert repeated["status"] == "duplicate"
    assert "PRIVATE PACK TEXT" not in json.dumps(
        await runtime._client.xrange(runtime._tenant_key(tenant, "audit"))
    )


async def test_automation_accepts_valid_pinned_revision_without_lua_array_corruption(admission):
    runtime, tenant = admission
    store, value = AutomationStore(runtime), snapshot(tenant)
    await store.create(job(tenant))
    await seed(runtime, value)
    await change_head(runtime, value, revision=2, content_sha256="f" * 64)
    submitted = goal(tenant, value)
    assert (await store.submit(tenant, "skill-job", "one", submitted, {}, NOW))[
        "status"
    ] == "queued"
    saved = await runtime.get_goal(submitted["goal_id"], tenant)
    assert saved["agent_config"]["skill_snapshots"] == [value]
    assert saved["agent_config"]["skill_snapshots"][0]["required_tools"] == []


async def test_revoked_pending_is_discarded_even_while_active_goal_is_busy(admission):
    runtime, tenant = admission
    store, value = AutomationStore(runtime), snapshot(tenant)
    await store.create(job(tenant))
    await seed(runtime, value)
    active = goal(tenant)
    await store.submit(tenant, "skill-job", "first", active, {}, NOW)
    pending = goal(tenant, value)
    result = await store.submit(tenant, "skill-job", "second", pending, {}, NOW)
    assert result["status"] == "coalesced"
    await change_head(runtime, value, enabled=False, revocation_epoch=1)
    assert (await store.flush_pending(tenant, "skill-job", NOW))["status"] == "skills_revoked"
    assert await runtime._client.get(store._key(tenant, "skill-job", ":pending")) is None
    assert (await store.flush_pending(tenant, "skill-job", NOW))["status"] == "no_pending"
    assert len(await queued(runtime, tenant)) == 1


async def test_quota_deferred_skill_is_not_revived_by_disable_then_enable(admission):
    runtime, tenant = admission
    store, value = AutomationStore(runtime), snapshot(tenant)
    await store.create(job(tenant, cap=1))
    await seed(runtime, value)
    active = goal(tenant)
    await store.submit(tenant, "skill-job", "first", active, {}, NOW)
    active["status"] = "completed"
    await runtime.save_goal(active["goal_id"], active, tenant)
    deferred = await store.submit(tenant, "skill-job", "second", goal(tenant, value), {}, NOW)
    assert deferred["status"] == "rate_limited"
    await change_head(runtime, value, enabled=True, revocation_epoch=1)
    assert (await store.flush_pending(tenant, "skill-job", NOW + 86_400))[
        "status"
    ] == "skills_revoked"
    assert await runtime._client.get(store._key(tenant, "skill-job", ":pending")) is None
    assert len(await queued(runtime, tenant)) == 1


async def test_rejected_new_occurrence_preserves_older_pending_with_valid_skills(admission):
    runtime, tenant = admission
    store = AutomationStore(runtime)
    valid, revoked = snapshot(tenant, "valid-pack"), snapshot(tenant, "revoked-pack")
    await store.create(job(tenant))
    await seed(runtime, valid)
    await seed(runtime, revoked, enabled=False)
    active = goal(tenant)
    await store.submit(tenant, "skill-job", "first", active, {}, NOW)
    pending = goal(tenant, valid)
    await store.submit(tenant, "skill-job", "second", pending, {}, NOW)
    rejected = await store.submit(tenant, "skill-job", "third", goal(tenant, revoked), {}, NOW)
    assert rejected["status"] == "skills_revoked"
    active["status"] = "completed"
    await runtime.save_goal(active["goal_id"], active, tenant)
    flushed = await store.flush_pending(tenant, "skill-job", NOW)
    assert flushed["status"] == "queued"
    assert flushed["goal_id"] == pending["goal_id"]
    assert flushed["coalesced_count"] == 1


async def test_delegation_preserves_prepared_skill_revisions_after_new_publication(admission):
    runtime, tenant = admission
    value = snapshot(tenant)
    await seed(runtime, value)
    await change_head(runtime, value, revision=2, content_sha256="f" * 64)
    parent = goal(tenant, value, agent_id="chief", role="chief")
    parent.update(
        status="running",
        execution_owner="owner",
        execution_lease_until=time.time() + 60,
        team_token_budget=2_000,
        chief_budget=1_000,
    )
    parent["agent_config"]["delegate_agent_ids"] = ["worker"]
    await runtime.save_goal(parent["goal_id"], parent, tenant)
    store = DelegationStore(runtime)
    assert await store.create_children(
        parent, "owner", [goal(tenant, value)], {"total_tokens": 100}
    )
    child_id = store.child_id(tenant, parent["goal_id"], 0)
    child = await runtime.get_goal(child_id, tenant)
    saved_parent = await runtime.get_goal(parent["goal_id"], tenant)
    assert child["agent_config"]["skill_snapshots"] == [value]
    assert saved_parent["agent_config"]["skill_snapshots"] == [value]
    assert len(await queued(runtime, tenant)) == 1


async def test_manual_execution_and_delegation_audit_refs_without_private_content(admission):
    runtime, tenant = admission
    value = snapshot(tenant)
    await seed(runtime, value)
    parent = goal(tenant, value, agent_id="chief", role="chief")
    parent.update(team_token_budget=2_000, chief_budget=1_000)
    parent["agent_config"]["delegate_agent_ids"] = ["worker"]
    execution = GoalExecutionStore(runtime)
    await execution.submit_manual(parent)
    running = await execution.claim(parent["goal_id"], tenant, "owner", 60_000)
    assert await DelegationStore(runtime).create_children(
        running, "owner", [goal(tenant, value)], {"total_tokens": 100}
    )
    audit = [
        json.loads(fields["payload"])
        for _, fields in await runtime._client.xrange(runtime._tenant_key(tenant, "audit"))
    ]
    assert {entry["action"] for entry in audit} == {"goal.create", "goal.execute", "goal.delegate"}
    assert all(entry["skills"] == skill_store.skill_refs([value]) for entry in audit)
    encoded = json.dumps(audit)
    assert all(
        value[field] not in encoded for field in ("instructions", "description", "display_name")
    )


async def test_automation_admission_audit_refs_for_queued_coalesced_and_rate_limited(admission):
    runtime, tenant = admission
    store, value = AutomationStore(runtime), snapshot(tenant)
    await store.create(job(tenant, cap=1))
    await seed(runtime, value)
    active = goal(tenant, value)
    await store.submit(tenant, "skill-job", "first", active, {}, NOW)
    await store.submit(tenant, "skill-job", "second", goal(tenant, value), {}, NOW)
    active["status"] = "completed"
    await runtime.save_goal(active["goal_id"], active, tenant)
    assert (await store.flush_pending(tenant, "skill-job", NOW))["status"] == "rate_limited"
    audit = [
        json.loads(fields["payload"])
        for _, fields in await runtime._client.xrange(runtime._tenant_key(tenant, "audit"))
    ]
    assert {entry["action"] for entry in audit} == {
        "automation.queued",
        "automation.coalesced",
        "automation.rate_limited",
    }
    assert all(entry["skills"] == skill_store.skill_refs([value]) for entry in audit)
    encoded = json.dumps(audit)
    assert all(
        value[field] not in encoded for field in ("instructions", "description", "display_name")
    )


@pytest.mark.parametrize("target", ["parent", "child"])
async def test_delegation_checks_both_parent_and_child_skills_before_queue(admission, target):
    runtime, tenant = admission
    parent_pack, child_pack = snapshot(tenant, "chief-pack"), snapshot(tenant, "worker-pack")
    await seed(runtime, parent_pack)
    await seed(runtime, child_pack)
    parent = goal(tenant, parent_pack, agent_id="chief", role="chief")
    parent.update(
        status="running",
        execution_owner="owner",
        execution_lease_until=time.time() + 60,
        team_token_budget=2_000,
        chief_budget=1_000,
    )
    parent["agent_config"]["delegate_agent_ids"] = ["worker"]
    await runtime.save_goal(parent["goal_id"], parent, tenant)
    await change_head(runtime, parent_pack if target == "parent" else child_pack, enabled=False)
    with pytest.raises(PermissionError):
        await DelegationStore(runtime).create_children(
            parent, "owner", [goal(tenant, child_pack)], {"total_tokens": 100}
        )
    assert await queued(runtime, tenant) == []
    assert (await runtime.get_goal(parent["goal_id"], tenant))["status"] == "running"


async def test_delegation_child_disable_after_watch_aborts_parent_and_children(
    admission, monkeypatch
):
    runtime, tenant = admission
    value = snapshot(tenant)
    await seed(runtime, value)
    parent = goal(tenant, agent_id="chief", role="chief")
    parent.update(
        status="running",
        execution_owner="owner",
        execution_lease_until=time.time() + 60,
        team_token_budget=2_000,
        chief_budget=1_000,
    )
    parent["agent_config"]["delegate_agent_ids"] = ["worker"]
    await runtime.save_goal(parent["goal_id"], parent, tenant)
    original, changed = skill_store.guard_goal_skills, False

    async def revoke_child_after_watch(runtime, candidate, **kwargs):
        nonlocal changed
        await original(runtime, candidate, **kwargs)
        if candidate["agent_id"] == "worker" and not changed:
            changed = True
            await change_head(runtime, value, enabled=False, revocation_epoch=1)

    monkeypatch.setattr(skill_store, "guard_goal_skills", revoke_child_after_watch)
    with pytest.raises(PermissionError):
        await DelegationStore(runtime).create_children(
            parent, "owner", [goal(tenant, value)], {"total_tokens": 100}
        )
    assert changed
    assert await queued(runtime, tenant) == []
    assert (await runtime.get_goal(parent["goal_id"], tenant))["status"] == "running"
    assert await runtime._client.xlen(runtime._tenant_key(tenant, "audit")) == 0


@pytest.mark.parametrize("escape", ["content", "tools", "role", "bindings"])
def test_child_scope_rejects_invalid_skill_snapshot_before_persistence(escape):
    value = snapshot("tenant")
    child = goal("tenant", value)
    pinned = child["agent_config"]["skill_snapshots"][0]
    if escape == "content":
        pinned["instructions"] = "Changed after planning"
    elif escape == "tools":
        pinned["required_tools"] = ["browser_click"]
        pinned["content_sha256"] = skill_store.snapshot_content_hash(pinned)
    elif escape == "role":
        pinned["agent_roles"] = ["chief"]
        pinned["content_sha256"] = skill_store.snapshot_content_hash(pinned)
    else:
        child["agent_config"]["skills"] = []
    with pytest.raises(ValueError):
        _scope_child(child, {"worker"})
