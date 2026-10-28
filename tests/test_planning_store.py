"""Planning persistence invariants, including real Redis WATCH competition."""

import asyncio
import json
import os
from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import pytest
from redis.exceptions import WatchError

from app.core import planning_store
from app.core.planning_store import (
    MAX_PLAN_BYTES,
    MAX_SNAPSHOT_BYTES,
    PlanningConflict,
    PlanningIntegrityError,
    PlanningNotFound,
    PlanningStore,
    canonical_hash,
)
from app.core.redis_runtime import RedisRuntime


async def test_planning_event_reader_keeps_only_safe_references_and_validates_cursor():
    class Client:
        async def xread(self, streams, count, block):
            assert streams == {"tenant:acme:planning:events:session-one": "4-0"}
            assert count == 100 and block == 1
            return [
                (
                    next(iter(streams)),
                    [
                        (
                            "5-0",
                            {
                                "payload": json.dumps(
                                    {
                                        "type": "plan.revised",
                                        "session_id": "session-one",
                                        "plan_revision": 2,
                                        "plan_sha256": "a" * 64,
                                        "subject": "private-user",
                                        "objective": "PRIVATE-DRAFT",
                                    }
                                )
                            },
                        )
                    ],
                )
            ]

    runtime = SimpleNamespace(
        _client=Client(), _tenant_key=lambda tenant, suffix: f"tenant:{tenant}:{suffix}"
    )
    rows = await PlanningStore(runtime).events(
        "acme", "session-one", "4-0", block_ms=1
    )
    assert rows == [
        (
            "5-0",
            {
                "type": "plan.revised",
                "session_id": "session-one",
                "plan_revision": 2,
                "plan_sha256": "a" * 64,
            },
        )
    ]
    assert "PRIVATE" not in json.dumps(rows)
    with pytest.raises(ValueError, match="cursor"):
        await PlanningStore(runtime).events("acme", "session-one", "latest")


def configuration(tenant, *, version=1):
    value = {
        "schema_version": 1,
        "tenant_id": tenant,
        "agent_id": "worker-one",
        "root_agent": {
            "agent_id": "worker-one",
            "version": version,
            "role": "worker",
            "persona": "PRIVATE-PERSONA",
            "skills": [{"instructions": "PRIVATE-SKILL"}],
            "model": "test-model",
        },
        "workers": [],
    }
    value["snapshot_fingerprint"] = canonical_hash(value)
    return value


def plan(**changes):
    return {"objective": "PRIVATE-OBJECTIVE", "steps": [{"query": "PRIVATE-QUERY"}]} | changes


def validate_plan(snapshot, content):
    if snapshot["root_agent"]["role"] != "worker":
        raise ValueError("PRIVATE-INVALID-PLAN")
    return bool(content.get("steps"))


@pytest.fixture
async def storage():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Real Redis integration requires TEST_REDIS_URL")
    runtime = RedisRuntime(url)
    try:
        await runtime.ping()
    except Exception as exc:
        await runtime.close()
        pytest.fail(f"Configured test Redis is unavailable: {exc}")
    tenant = f"planning-store-{uuid4().hex}"
    source_key = runtime._tenant_key(tenant, "test:snapshot-source")
    await runtime._client.set(source_key, json.dumps(configuration(tenant)))

    async def builder(pipe):
        await pipe.watch(source_key)
        return json.loads(await pipe.get(source_key))

    env = SimpleNamespace(
        runtime=runtime,
        store=PlanningStore(runtime),
        tenant=tenant,
        source_key=source_key,
        builder=builder,
    )
    try:
        yield env
    finally:
        keys = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}:*")]
        if keys:
            await runtime._client.delete(*keys)
        await runtime.close()


async def create(env):
    return await env.store.create(env.tenant, "worker-one", "operator", env.builder)


async def test_plan_revision_freezes_selection_and_detects_artifact_tampering(storage):
    env = storage
    source = configuration(env.tenant)
    source["root_agent"].update(
        agent_id="worker-one",
        skills=[],
        skill_selection_rules=[
            {
                "skill_id": "review",
                "revision": 1,
                "field": "objective",
                "values": ["evidence"],
                "priority": 1,
            }
        ],
        skill_selection_candidates=[
            {"skill_id": "review", "revision": 1, "content_sha256": "b" * 64, "revocation_epoch": 0}
        ],
    )
    source.pop("snapshot_fingerprint")
    source["snapshot_fingerprint"] = canonical_hash(source)
    await env.runtime._client.set(env.source_key, json.dumps(source))
    saved = await save(env, await create(env), content=plan(objective="review evidence"))
    revision = await env.store.get_revision(env.tenant, saved["session_id"], 1)
    selected = revision["skill_selection"]["agents"][0]
    assert selected["bindings"][0]["skill_id"] == "review"
    assert selected["selection"]["selected"][0]["source"] == "rule"
    revision["skill_selection"]["agents"][0]["bindings"] = []
    await env.runtime._client.set(
        env.store.revision_key(env.tenant, saved["session_id"], 1), json.dumps(revision)
    )
    with pytest.raises(PlanningIntegrityError, match="selection"):
        await env.store.get_revision(env.tenant, saved["session_id"], 1)


async def test_new_session_registers_owner_in_creation_transaction(storage):
    from app.core.resource_access import AccessStore
    from app.core.security import Principal

    session = await create(storage)
    access = AccessStore(storage.runtime)
    assert await access.can(
        Principal("operator", storage.tenant, "operator", "api_key"),
        "planning_session",
        session["session_id"],
        "operate",
    )
    assert not await access.can(
        Principal("other", storage.tenant, "operator", "api_key"),
        "planning_session",
        session["session_id"],
    )


async def save(env, session, *, content=None, complete=True):
    return await env.store.save_plan(
        env.tenant,
        session["session_id"],
        content if content is not None else plan(),
        session["version"],
        "operator",
        env.builder,
        complete=complete,
    )


async def mode(env, session, target, **kwargs):
    return await env.store.set_mode(
        env.tenant,
        session["session_id"],
        target,
        session["version"],
        "operator",
        env.builder,
        **kwargs,
    )


def test_canonical_hash_is_order_independent_and_rejects_nonfinite_data():
    assert canonical_hash({"schema_version": 1, "中文": [1, 2]}) == canonical_hash(
        {"中文": [1, 2], "schema_version": 1}
    )
    assert canonical_hash({"schema_version": 1}) != canonical_hash({"schema_version": 2})
    for invalid in (float("nan"), float("inf"), object(), "\ud800"):
        with pytest.raises(PlanningIntegrityError):
            canonical_hash({"PRIVATE": invalid})


async def test_create_revisions_and_mode_changes_do_not_enqueue_goals(storage):
    env = storage
    queue_before = await env.runtime._client.xlen("agent:goal:queue")
    session = await create(env)
    assert session["mode"] == "plan" and session["version"] == 1
    assert session["plan_revision"] == 0 and session["accepted_revision"] is None
    session = await save(env, session)
    first = await env.store.get_revision(env.tenant, session["session_id"], 1)
    expected = canonical_hash(
        {"schema_version": 1, "plan": plan(), "complete": True, "snapshot": session["snapshot"]}
    )
    assert first["content_sha256"] == session["plan_sha256"] == expected
    session = await mode(env, session, "execute", plan_revision=1, plan_sha256=expected)
    assert session["accepted_revision"] == 1 and session["accepted_hash"] == expected
    assert session["plan_sha256"] == expected and session["version"] == 3
    unchanged = await mode(env, session, "execute", plan_revision=1)
    assert unchanged == session
    session = await mode(env, session, "plan")
    assert session["accepted_revision"] is None and session["accepted_hash"] is None
    session = await save(env, session, content=plan(objective="Changed"))
    assert session["plan_revision"] == 2
    assert await env.store.get_revision(env.tenant, session["session_id"], 1) == first
    assert await env.store.get(env.tenant, session["session_id"]) == session
    assert await env.runtime._client.xlen("agent:goal:queue") == queue_before
    assert not [
        key async for key in env.runtime._client.scan_iter(f"tenant:{env.tenant}:agent:goal:*")
    ]


async def test_revision_pagination_and_tenant_isolation(storage):
    env = storage
    session = await create(env)
    for index in range(3):
        session = await save(env, session, content=plan(objective=f"Task {index}"))
    page, cursor = await env.store.revisions(env.tenant, session["session_id"], limit=2)
    assert [value["revision"] for value in page] == [1, 2] and cursor == 2
    page, cursor = await env.store.revisions(env.tenant, session["session_id"], after=2)
    assert [value["revision"] for value in page] == [3] and cursor is None
    assert await env.store.revisions(env.tenant, session["session_id"], after=5) == ([], None)
    assert await env.store.get("foreign", session["session_id"]) is None
    assert await env.store.get_revision("foreign", session["session_id"], 1) is None
    with pytest.raises(PlanningNotFound):
        await env.store.revisions("foreign", session["session_id"])


async def test_cas_competition_has_one_revision_event_and_audit(storage):
    env = storage
    session = await create(env)
    results = await asyncio.gather(
        *[save(env, session, content=plan(objective=f"Candidate {index}")) for index in range(20)],
        return_exceptions=True,
    )
    assert sum(isinstance(value, dict) for value in results) == 1
    assert sum(isinstance(value, PlanningConflict) for value in results) == 19, [
        str(value) for value in results if isinstance(value, Exception)
    ]
    revisions, cursor = await env.store.revisions(env.tenant, session["session_id"])
    assert len(revisions) == 1 and cursor is None
    for key in (
        env.store.events_key(env.tenant, session["session_id"]),
        env.runtime._tenant_key(env.tenant, "audit"),
    ):
        rows = await env.runtime._client.xrange(key)
        assert len(rows) == 2
        encoded = json.dumps(rows)
        assert "PRIVATE-" not in encoded and "Candidate" not in encoded
        assert session["session_id"] in encoded


async def test_changed_configuration_requires_explicit_refresh(storage):
    env = storage
    session = await save(env, await create(env))
    original = await env.store.get_revision(env.tenant, session["session_id"], 1)
    await env.runtime._client.set(env.source_key, json.dumps(configuration(env.tenant, version=2)))
    with pytest.raises(PlanningConflict, match="stale"):
        await save(env, session)
    with pytest.raises(PlanningConflict, match="stale"):
        await mode(env, session, "execute", plan_revision=1)
    refreshed = await env.store.refresh(
        env.tenant,
        session["session_id"],
        session["version"],
        "operator",
        env.builder,
        plan_validator=validate_plan,
    )
    assert refreshed["plan_revision"] == 2 and refreshed["version"] == 3
    assert refreshed["snapshot"]["root_agent"]["version"] == 2
    second = await env.store.get_revision(env.tenant, session["session_id"], 2)
    assert second["plan"] == original["plan"] and second["complete"]
    assert second["content_sha256"] != original["content_sha256"]
    assert await env.store.get_revision(env.tenant, session["session_id"], 1) == original


async def test_refresh_without_plan_does_not_create_an_empty_revision(storage):
    env = storage
    session = await create(env)
    await env.runtime._client.set(env.source_key, json.dumps(configuration(env.tenant, version=2)))
    result = await env.store.refresh(
        env.tenant,
        session["session_id"],
        session["version"],
        "operator",
        env.builder,
        plan_validator=validate_plan,
    )
    assert result["version"] == 2 and result["plan_revision"] == 0
    assert await env.store.revisions(env.tenant, session["session_id"]) == ([], None)


async def test_refresh_recomputes_completeness_and_rejects_incompatible_plan(storage):
    env = storage
    session = await save(env, await create(env), content=plan(steps=[]), complete=False)
    calls = []

    def validator(snapshot, content):
        calls.append((snapshot, content))
        return False

    session = await env.store.refresh(
        env.tenant,
        session["session_id"],
        session["version"],
        "operator",
        env.builder,
        plan_validator=validator,
    )
    assert (
        len(calls) == 1
        and not (await env.store.get_revision(env.tenant, session["session_id"], 2))["complete"]
    )
    changed = configuration(env.tenant, version=2)
    changed["root_agent"]["role"] = "chief"
    await env.runtime._client.set(env.source_key, json.dumps(changed))
    with pytest.raises(PlanningConflict, match="incompatible") as error:
        await env.store.refresh(
            env.tenant,
            session["session_id"],
            session["version"],
            "operator",
            env.builder,
            plan_validator=validate_plan,
        )
    assert "PRIVATE" not in str(error.value)
    assert await env.store.get(env.tenant, session["session_id"]) == session


async def test_watch_rechecks_configuration_changed_during_save(storage):
    env = storage
    session = await create(env)
    captured, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def racing_builder(pipe):
        nonlocal calls
        snapshot = await env.builder(pipe)
        calls += 1
        if calls == 1:
            captured.set()
            await release.wait()
        return snapshot

    task = asyncio.create_task(
        env.store.save_plan(
            env.tenant,
            session["session_id"],
            plan(),
            1,
            "operator",
            racing_builder,
            complete=True,
        )
    )
    await asyncio.wait_for(captured.wait(), timeout=5)
    await env.runtime._client.set(env.source_key, json.dumps(configuration(env.tenant, version=2)))
    release.set()
    with pytest.raises(PlanningConflict, match="stale"):
        await asyncio.wait_for(task, timeout=5)
    assert calls == 2
    assert await env.store.get(env.tenant, session["session_id"]) == session
    assert await env.store.revisions(env.tenant, session["session_id"]) == ([], None)


async def test_mode_transition_cannot_accept_a_plan_changed_during_validation(storage):
    env = storage
    session = await save(env, await create(env))
    captured, release = asyncio.Event(), asyncio.Event()

    async def paused_builder(pipe):
        snapshot = await env.builder(pipe)
        captured.set()
        await release.wait()
        return snapshot

    task = asyncio.create_task(
        env.store.set_mode(
            env.tenant,
            session["session_id"],
            "execute",
            session["version"],
            "operator",
            paused_builder,
            plan_revision=1,
        )
    )
    await asyncio.wait_for(captured.wait(), timeout=5)
    revised = await save(env, session, content=plan(objective="Replacement plan"))
    release.set()
    with pytest.raises(PlanningConflict, match="version conflict"):
        await asyncio.wait_for(task, timeout=5)
    assert await env.store.get(env.tenant, session["session_id"]) == revised
    assert revised["mode"] == "plan" and revised["accepted_revision"] is None


async def test_noop_mode_request_checks_watch_before_returning_a_session(storage):
    env = storage
    session = await mode(env, await save(env, await create(env)), "execute", plan_revision=1)
    captured, release = asyncio.Event(), asyncio.Event()

    async def paused_builder(pipe):
        snapshot = await env.builder(pipe)
        captured.set()
        await release.wait()
        return snapshot

    task = asyncio.create_task(
        env.store.set_mode(
            env.tenant,
            session["session_id"],
            "execute",
            session["version"],
            "operator",
            paused_builder,
            plan_revision=1,
        )
    )
    await asyncio.wait_for(captured.wait(), timeout=5)
    revised = await mode(env, session, "plan")
    release.set()
    with pytest.raises(PlanningConflict, match="version conflict"):
        await asyncio.wait_for(task, timeout=5)
    assert await env.store.get(env.tenant, session["session_id"]) == revised
    assert (
        await env.runtime._client.xlen(env.store.events_key(env.tenant, session["session_id"])) == 4
    )


@pytest.mark.parametrize("accepted_revision", [True, 0, "1"])
async def test_read_rejects_non_positive_integer_accepted_revision(storage, accepted_revision):
    env = storage
    session = await mode(env, await save(env, await create(env)), "execute", plan_revision=1)
    session["accepted_revision"] = accepted_revision
    await env.runtime._client.set(
        env.store.session_key(env.tenant, session["session_id"]), json.dumps(session)
    )
    with pytest.raises(PlanningIntegrityError, match="acceptance"):
        await env.store.get(env.tenant, session["session_id"])


async def test_execute_requires_current_complete_revision_and_plan_mode_writes(storage):
    env = storage
    session = await create(env)
    with pytest.raises(PlanningConflict):
        await mode(env, session, "execute", plan_revision=1)
    session = await save(env, session, content=plan(steps=[]), complete=False)
    with pytest.raises(PlanningConflict):
        await mode(env, session, "execute", plan_revision=1)
    session = await save(env, session)
    for number in (None, 1, True):
        with pytest.raises(PlanningConflict):
            await mode(env, session, "execute", plan_revision=number)
    with pytest.raises(PlanningConflict, match="hash"):
        await mode(env, session, "execute", plan_revision=2, plan_sha256="0" * 64)
    session = await mode(env, session, "execute", plan_revision=2)
    with pytest.raises(PlanningConflict, match="plan mode"):
        await save(env, session)
    with pytest.raises(PlanningConflict, match="plan mode"):
        await env.store.refresh(
            env.tenant,
            session["session_id"],
            session["version"],
            "operator",
            env.builder,
            plan_validator=validate_plan,
        )
    # Stale configuration must not trap an execute session outside plan/refresh.
    await env.runtime._client.set(env.source_key, json.dumps(configuration(env.tenant, version=2)))
    assert (await mode(env, session, "plan"))["mode"] == "plan"


@pytest.mark.parametrize(
    "status", [None, "running", "waiting_approval", "paused", "waiting_children"]
)
async def test_return_to_plan_rejects_missing_or_nonterminal_active_goal(storage, status):
    env = storage
    session = await mode(env, await save(env, await create(env)), "execute", plan_revision=1)
    session["active_goal_id"] = "future-goal"
    await env.runtime._client.set(
        env.store.session_key(env.tenant, session["session_id"]), json.dumps(session)
    )
    if status is not None:
        await env.runtime._client.set(
            env.runtime._tenant_key(env.tenant, "agent:goal:future-goal"),
            json.dumps({"tenant_id": env.tenant, "goal_id": "future-goal", "status": status}),
        )
    with pytest.raises(PlanningConflict, match="active Goal"):
        await mode(env, session, "plan")


@pytest.mark.parametrize("status", ["completed", "cancelled", "failed"])
async def test_return_to_plan_allows_terminal_active_goal(storage, status):
    env = storage
    session = await mode(env, await save(env, await create(env)), "execute", plan_revision=1)
    session["active_goal_id"] = "future-goal"
    await env.runtime._client.set(
        env.store.session_key(env.tenant, session["session_id"]), json.dumps(session)
    )
    await env.runtime._client.set(
        env.runtime._tenant_key(env.tenant, "agent:goal:future-goal"),
        json.dumps({"tenant_id": env.tenant, "goal_id": "future-goal", "status": status}),
    )
    assert (await mode(env, session, "plan"))["mode"] == "plan"


@pytest.mark.parametrize(
    "target", ["session-snapshot", "revision-plan", "revision-snapshot", "schema"]
)
async def test_read_integrity_rejects_tampered_records_without_leaking_content(storage, target):
    env = storage
    session = await save(env, await create(env))
    key = (
        env.store.session_key(env.tenant, session["session_id"])
        if target == "session-snapshot"
        else env.store.revision_key(env.tenant, session["session_id"], 1)
    )
    value = json.loads(await env.runtime._client.get(key))
    if target in {"session-snapshot", "revision-snapshot"}:
        value["snapshot"]["root_agent"]["persona"] = "PRIVATE-TAMPER"
    elif target == "revision-plan":
        value["plan"]["objective"] = "PRIVATE-TAMPER"
    else:
        value["schema_version"] = 2
    await env.runtime._client.set(key, json.dumps(value))
    with pytest.raises(PlanningIntegrityError) as error:
        await env.store.get(env.tenant, session["session_id"])
    assert "PRIVATE" not in str(error.value)


async def test_limits_prevent_growth_without_partial_writes(storage, monkeypatch):
    env = storage
    monkeypatch.setattr(planning_store, "MAX_SESSIONS_PER_TENANT", 1)
    session = await create(env)
    with pytest.raises(PlanningConflict, match="session limit"):
        await create(env)
    with pytest.raises(ValueError, match="byte limit"):
        await save(env, session, content=plan(objective="中" * (MAX_PLAN_BYTES // 3)))
    session = await save(env, session)
    monkeypatch.setattr(planning_store, "MAX_REVISIONS", 1)
    with pytest.raises(PlanningConflict, match="revision limit"):
        await save(env, session)
    assert await env.store.get(env.tenant, session["session_id"]) == session
    assert (
        await env.runtime._client.xlen(env.store.events_key(env.tenant, session["session_id"])) == 2
    )
    oversized = configuration(env.tenant)
    oversized["root_agent"]["persona"] = "x" * MAX_SNAPSHOT_BYTES

    async def oversized_builder(pipe):
        return oversized

    monkeypatch.setattr(planning_store, "MAX_SESSIONS_PER_TENANT", 2)
    with pytest.raises(ValueError, match="byte limit"):
        await env.store.create(env.tenant, "worker-one", "operator", oversized_builder)
    assert await env.runtime._client.scard(env.store._index_key(env.tenant)) == 1


async def test_tenant_session_limit_is_atomic_under_competition(storage, monkeypatch):
    env = storage
    monkeypatch.setattr(planning_store, "MAX_SESSIONS_PER_TENANT", 2)
    results = await asyncio.gather(*[create(env) for _ in range(20)], return_exceptions=True)
    assert sum(isinstance(value, dict) for value in results) == 2
    assert sum(isinstance(value, PlanningConflict) for value in results) == 18
    assert await env.runtime._client.scard(env.store._index_key(env.tenant)) == 2
    assert await env.runtime._client.xlen(env.runtime._tenant_key(env.tenant, "audit")) == 2


async def test_immutable_revision_collision_is_not_overwritten(storage):
    env = storage
    session = await create(env)
    key = env.store.revision_key(env.tenant, session["session_id"], 1)
    await env.runtime._client.set(key, "preserve-existing-record")
    with pytest.raises(PlanningIntegrityError, match="already exists"):
        await save(env, session)
    assert await env.runtime._client.get(key) == "preserve-existing-record"


async def test_strict_version_and_invalid_mode_are_rejected(storage):
    env = storage
    session = await create(env)
    for invalid in (True, "1", 0):
        with pytest.raises(ValueError, match="Expected version"):
            await env.store.save_plan(
                env.tenant,
                session["session_id"],
                plan(),
                invalid,
                "operator",
                env.builder,
                complete=True,
            )
    with pytest.raises(ValueError, match="Unknown planning mode"):
        await mode(env, session, "prompt-says-plan")


async def test_watch_retry_limit_prevents_starvation_without_partial_state(storage):
    env = storage
    attempts = 0

    async def conflicting_builder(pipe):
        nonlocal attempts
        attempts += 1
        raise WatchError("forced contention")

    store = PlanningStore(env.runtime, max_watch_retries=3)
    with pytest.raises(PlanningConflict, match="too often"):
        await store.create(env.tenant, "worker-one", "operator", conflicting_builder)
    assert attempts == 3
    assert await env.runtime._client.scard(store._index_key(env.tenant)) == 0
    assert not [
        key async for key in env.runtime._client.scan_iter(f"tenant:{env.tenant}:planning:*")
    ]


async def test_foreign_snapshot_identity_does_not_create_a_session(storage):
    env = storage

    async def invalid_builder(pipe):
        return deepcopy(configuration("foreign"))

    with pytest.raises(PlanningIntegrityError, match="identity"):
        await env.store.create(env.tenant, "worker-one", "operator", invalid_builder)
    assert await env.runtime._client.scard(env.store._index_key(env.tenant)) == 0
