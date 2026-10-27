"""Planning reads trusted configuration without inference, dispatch, or quota writes."""

import hashlib
import json
import os
from copy import deepcopy
from uuid import uuid4

import pytest
from redis.exceptions import WatchError

from app.automations import AutomationPolicy
from app.core.planning_snapshot import (
    PlanningSnapshotInvalid,
    PlanningSnapshotNotFound,
    build_planning_snapshot,
    guard_planning_snapshot,
    planning_snapshot_fingerprint,
    validate_planning_plan,
)
from app.core.redis_runtime import RedisRuntime
from app.core.skill_store import SkillContent, snapshot_content_hash


class Reader:
    def __init__(self, data):
        self.data, self.watched, self.read = data, [], []

    async def watch(self, *keys):
        self.watched.extend(keys)

    async def get(self, key):
        self.read.append(key)
        return self.data.get(key)

    async def mget(self, keys):
        return [await self.get(key) for key in keys]


class MemoryRuntime:
    def __init__(self):
        self.data = {}
        self._client = Reader(self.data)

    @staticmethod
    def _tenant_key(tenant, suffix):
        return f"tenant:{tenant}:{suffix}"

    def put(self, key, value):
        self.data[key] = json.dumps(value)

    def put_agent(self, value):
        self.put(self._tenant_key(value["tenant_id"], f"agents:record:{value['agent_id']}"), value)


class Router:
    def __init__(self):
        self.routes = {}

    def get(self, model):
        pytest.fail("Planning must not instantiate an inference client")

    def planning_route(self, model):
        return {
            "alias": model,
            "resolved_model": "resolved-" + model,
            "route_fingerprint": self.routes.get(
                model, "sha256:" + hashlib.sha256(model.encode()).hexdigest()
            ),
            "base_url": "https://PRIVATE_BACKEND:8443/v1",
            "api_key": "PRIVATE_CREDENTIAL",
        }


def agent(tenant="acme", agent_id="worker", **changes):
    return {
        "tenant_id": tenant,
        "agent_id": agent_id,
        "name": "Trusted " + agent_id,
        "description": "A configured Agent",
        "persona": "PRIVATE PERSONA: cite evidence",
        "role": "worker",
        "enabled": True,
        "version": 1,
        "model": agent_id + "-model",
        "delegate_agent_ids": [],
        "skills": [],
        "policy": AutomationPolicy(allowed_tools=["scrape_dynamic_page"]).model_dump(),
        "created_by": "PRIVATE_ACTOR",
        **changes,
    }


def chief(tenant="acme"):
    return agent(
        tenant,
        "chief",
        role="chief",
        delegate_agent_ids=["worker-a", "worker-b"],
        policy=AutomationPolicy(allowed_tools=[]).model_dump(),
    )


def pack(tenant="acme", skill_id="review-pack", **changes):
    body = SkillContent(
        name=skill_id,
        display_name="Review",
        description="Evidence review procedure",
        instructions="PRIVATE SKILL: be precise",
    ).model_dump()
    body.update(changes)
    return body | {
        "tenant_id": tenant,
        "skill_id": skill_id,
        "revision": 1,
        "revocation_epoch": 0,
        "content_sha256": snapshot_content_hash(body),
    }


def put_pack(runtime, value, **head_changes):
    prefix = f"tenant:{value['tenant_id']}:skills:"
    runtime.put(prefix + "record:" + value["skill_id"], value | {"enabled": True} | head_changes)
    runtime.put(prefix + f"revision:{value['skill_id']}:{value['revision']}", value)


async def test_planning_skill_snapshot_is_separate_pinned_and_revocation_sensitive():
    runtime, router = MemoryRuntime(), Router()
    body = SkillContent(
        name="plan-forecast",
        schema_version=2,
        execution_modes=["plan"],
        display_name="計畫預測",
        description="僅規劃",
        instructions="不啟動執行。",
        resources=[{"path": "references/limits.md", "content": "保留未知。"}],
    ).model_dump()
    skill = pack(skill_id="forecast-pack", **body)
    put_pack(runtime, skill)
    runtime.put_agent(agent(planning_skills=[{"skill_id": "forecast-pack", "revision": 1}]))
    result = await build_planning_snapshot(
        runtime, router, "acme", "worker", client=runtime._client
    )
    root = result["root_agent"]
    assert root["skills"] == [] and root["skill_snapshots"] == []
    assert root["planning_skill_snapshots"][0]["resources"] == body["resources"]
    assert root["planning_skill_snapshots"][0]["execution_modes"] == ["plan"]
    await guard_planning_snapshot(runtime, router, "acme", result, client=runtime._client)
    put_pack(runtime, skill, enabled=False)
    with pytest.raises(PlanningSnapshotInvalid):
        await guard_planning_snapshot(runtime, router, "acme", result, client=runtime._client)


def plan(*, steps=None, assignments=None, **changes):
    return {
        "objective": "Review evidence",
        "context": "Background",
        "steps": steps or [],
        "assignments": assignments or [],
        **changes,
    }


def assignment(worker="worker-a", *, steps=True):
    return {
        "agent_id": worker,
        "objective": "Assigned review",
        "steps": [{"query": "Read evidence", "target_url": None}] if steps else [],
    }


@pytest.fixture
def configured():
    runtime, router = MemoryRuntime(), Router()
    root = chief()
    root["skills"] = [{"skill_id": "chief-pack", "revision": 1}]
    first = agent(agent_id="worker-a")
    first["skills"] = [{"skill_id": "worker-pack", "revision": 1}]
    for value in (root, first, agent(agent_id="worker-b", persona="Independent writer")):
        runtime.put_agent(value)
    put_pack(runtime, pack(skill_id="chief-pack"))
    put_pack(runtime, pack(skill_id="worker-pack", required_tools=["scrape_dynamic_page"]))
    return runtime, router


async def build(runtime, router, agent_id="chief", value=None):
    return await build_planning_snapshot(
        runtime, router, "acme", agent_id, value, client=runtime._client
    )


async def test_chief_snapshot_watches_all_workers_and_skills_without_execution(configured):
    runtime, router = configured
    before = deepcopy(runtime.data)
    value = await build(runtime, router, value=plan(assignments=[assignment()]))
    assert set(value) == {
        "schema_version",
        "tenant_id",
        "agent_id",
        "root_agent",
        "workers",
        "snapshot_fingerprint",
    }
    assert [worker["agent_id"] for worker in value["workers"]] == ["worker-a", "worker-b"]
    assert value["workers"][0]["persona"] != value["workers"][1]["persona"]
    assert value["root_agent"]["policy"]["allowed_tools"] == []
    assert value["workers"][0]["policy"]["allowed_tools"] == ["scrape_dynamic_page"]
    assert value["workers"][0]["model_route"]["resolved_model"] == "resolved-worker-a-model"
    assert set(runtime._client.watched) == set(runtime.data)
    assert runtime.data == before
    assert all("PRIVATE_BACKEND" not in json.dumps(record) for record in [value])
    assert "PRIVATE_CREDENTIAL" not in json.dumps(value)
    assert "PRIVATE_ACTOR" not in json.dumps(value)
    assert (
        "unknown"
        == value["root_agent"]["model_route"]["version_disclosure"]["actual_model_version"]
    )


async def test_plan_text_context_and_completion_do_not_change_configuration_fingerprint(configured):
    runtime, router = configured
    empty = await build(runtime, router)
    complete = await build(runtime, router, value=plan(assignments=[assignment()]))
    changed = await build(
        runtime,
        router,
        value=plan(
            objective="Different task", context="Different context", assignments=[assignment()]
        ),
    )
    assert empty == complete == changed
    assert not validate_planning_plan(empty, None)
    assert not validate_planning_plan(empty, plan())
    assert validate_planning_plan(empty, plan(assignments=[assignment()]))
    assert not validate_planning_plan(empty, plan(assignments=[assignment(steps=False)]))


async def test_worker_plan_is_linear_and_drafts_remain_incomplete():
    runtime, router = MemoryRuntime(), Router()
    runtime.put_agent(agent())
    value = await build(runtime, router, "worker")
    assert value["workers"] == []
    assert not validate_planning_plan(value, plan())
    assert validate_planning_plan(value, plan(steps=[{"query": "Read", "target_url": None}]))
    with pytest.raises(ValueError, match="Plan does not match"):
        validate_planning_plan(value, plan(assignments=[assignment()]))


@pytest.mark.parametrize(
    "value",
    [
        plan(steps=[{"query": "Chief cannot execute steps"}]),
        plan(assignments=[assignment("foreign")]),
        plan(assignments=[assignment(), assignment()]),
        plan(assignments=[{"agent_id": "worker-a", "steps": "PRIVATE INVALID VALUE"}]),
    ],
)
async def test_chief_plan_rejects_unapproved_duplicate_and_mixed_role_work(configured, value):
    runtime, router = configured
    saved = await build(runtime, router)
    with pytest.raises(ValueError) as error:
        validate_planning_plan(saved, value)
    assert "PRIVATE" not in str(error.value)


@pytest.mark.parametrize(
    "changes",
    [
        {"enabled": False},
        {"role": "invalid"},
        {"version": True},
        {"agent_id": "different"},
        {"delegate_agent_ids": ["child"]},
        {"skills": [{"skill_id": "missing", "revision": 1}]},
        {"policy": {"allowed_tools": ["arbitrary_shell"]}},
    ],
)
async def test_invalid_stored_worker_configuration_is_denied_with_safe_errors(changes):
    runtime, router = MemoryRuntime(), Router()
    runtime.put("tenant:acme:agents:record:worker", agent(**changes))
    with pytest.raises(PlanningSnapshotInvalid) as error:
        await build(runtime, router, "worker")
    assert str(error.value) == "Planning configuration is unavailable or invalid"
    assert "PRIVATE" not in str(error.value)


@pytest.mark.parametrize("foreign", [False, True])
async def test_missing_and_foreign_root_have_identical_not_found(foreign):
    runtime, router = MemoryRuntime(), Router()
    if foreign:
        runtime.put("tenant:acme:agents:record:worker", agent(tenant="other"))
    with pytest.raises(PlanningSnapshotNotFound, match="^Agent not found$"):
        await build(runtime, router, "worker")


@pytest.mark.parametrize(
    "changes",
    [
        {"enabled": False},
        {"tenant_id": "other"},
        {"role": "chief"},
        {"delegate_agent_ids": ["chief"]},
        {"agent_id": "different"},
    ],
)
async def test_every_allowlisted_worker_is_validated_even_if_not_assigned(configured, changes):
    runtime, router = configured
    key = "tenant:acme:agents:record:worker-b"
    runtime.put(key, json.loads(runtime.data[key]) | changes)
    with pytest.raises(PlanningSnapshotInvalid):
        await build(runtime, router, value=plan(assignments=[assignment("worker-a")]))


@pytest.mark.parametrize("change", ["agent", "route", "skill_epoch", "skill_hash", "snapshot"])
async def test_guard_rejects_changed_or_revoked_sources_without_refreshing_history(
    configured, change
):
    runtime, router = configured
    saved = await build(runtime, router)
    historic = deepcopy(saved)
    if change == "agent":
        runtime.put_agent(agent(agent_id="worker-b", version=2, persona="Changed writer"))
    elif change == "route":
        router.routes["worker-b-model"] = "sha256:" + "f" * 64
    elif change == "skill_epoch":
        key = "tenant:acme:skills:record:worker-pack"
        runtime.put(key, json.loads(runtime.data[key]) | {"enabled": True, "revocation_epoch": 1})
    elif change == "skill_hash":
        key = "tenant:acme:skills:revision:worker-pack:1"
        runtime.put(key, json.loads(runtime.data[key]) | {"content_sha256": "f" * 64})
    else:
        saved["root_agent"]["persona"] = "Tampered snapshot"
    with pytest.raises(PlanningSnapshotInvalid):
        await guard_planning_snapshot(runtime, router, "acme", saved, client=runtime._client)
    if change != "snapshot":
        assert saved == historic


async def test_guard_accepts_unchanged_snapshot_and_unbound_new_skill_revision(configured):
    runtime, router = configured
    saved = await build(runtime, router)
    key = "tenant:acme:skills:record:worker-pack"
    runtime.put(key, json.loads(runtime.data[key]) | {"revision": 2, "content_sha256": "f" * 64})
    await guard_planning_snapshot(runtime, router, "acme", saved, client=runtime._client)
    assert saved["snapshot_fingerprint"] == planning_snapshot_fingerprint(saved)


async def test_model_resolution_failure_is_generic_and_never_falls_back(configured):
    runtime, router = configured

    def invalid(model):
        raise ValueError("PRIVATE_ROUTE_CREDENTIAL")

    router.planning_route = invalid
    with pytest.raises(PlanningSnapshotInvalid) as error:
        await build(runtime, router)
    assert "PRIVATE" not in str(error.value)
    assert error.value.__cause__ is None


@pytest.fixture
async def real_snapshot():
    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("WATCH integration requires TEST_REDIS_URL")
    runtime = RedisRuntime(url)
    await runtime.ping()
    tenant = f"planning-snapshot-{uuid4().hex}"
    memory = MemoryRuntime()
    memory.put_agent(chief(tenant))
    first = agent(tenant, "worker-a")
    first["skills"] = [{"skill_id": "review-pack", "revision": 1}]
    memory.put_agent(first)
    memory.put_agent(agent(tenant, "worker-b"))
    put_pack(memory, pack(tenant))
    for key, value in memory.data.items():
        await runtime._client.set(key, value)
    try:
        yield runtime, Router(), tenant
    finally:
        keys = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}:*")]
        if keys:
            await runtime._client.delete(*keys)
        await runtime.close()


@pytest.mark.parametrize(
    "suffix",
    [
        "agents:record:chief",
        "agents:record:worker-b",
        "skills:record:review-pack",
        "skills:revision:review-pack:1",
    ],
)
async def test_caller_commit_is_fenced_against_every_configuration_source(real_snapshot, suffix):
    runtime, router, tenant = real_snapshot
    saved_key = runtime._tenant_key(tenant, "planning:test-commit")
    async with runtime._client.pipeline(transaction=True) as pipe:
        value = await build_planning_snapshot(runtime, router, tenant, "chief", client=pipe)
        key = runtime._tenant_key(tenant, suffix)
        source = json.loads(await runtime._client.get(key))
        source["version"] = source.get("version", 0) + 1
        await runtime._client.set(key, json.dumps(source))
        pipe.multi()
        pipe.set(saved_key, json.dumps(value))
        with pytest.raises(WatchError):
            await pipe.execute()
    assert await runtime._client.get(saved_key) is None
