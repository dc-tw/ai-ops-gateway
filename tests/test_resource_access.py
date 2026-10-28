"""Opt-in ACL authority on real Redis; synthetic data in a disposable instance."""

import asyncio
import json
import os
import subprocess
import sys
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from app.access import router as access_router
from app.agents import router as agents_router
from app.core.config import Settings
from app.core.goal_execution import GoalExecutionStore
from app.core.postgres_schema import SCHEMA_VERSION
from app.core.redis_runtime import RedisRuntime
from app.core.resource_access import (
    AccessStore,
    AccessUpdate,
    acl_key,
    actor_index,
    claim_conversation,
    filter_history,
    guard_owner,
    history_permissions,
    queue_owner,
    stream_access,
)
from app.core.security import Principal


def test_runtime_skill_import_does_not_load_acl_management_stack():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, app.core.skill_store; "
            "assert 'app.core.resource_access' not in sys.modules",
        ],
        check=True,
        timeout=15,
    )


@pytest.fixture
async def env():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("ACL integration requires TEST_REDIS_URL")
    runtime = RedisRuntime(url)
    await runtime.ping()
    tenant = "acl-" + uuid4().hex
    app = FastAPI()
    app.include_router(access_router)
    app.include_router(agents_router)
    keys = {
        name: {"subject": name, "tenant_id": tenant, "role": role}
        for name, role in [
            ("alice", "operator"),
            ("bob", "operator"),
            ("viewer", "viewer"),
            ("admin", "admin"),
        ]
    }
    keys["foreign"] = {"subject": "alice", "tenant_id": tenant + "-foreign", "role": "operator"}
    app.state.settings = Settings(resource_acl_enabled=True, api_keys_json=json.dumps(keys))
    app.state.redis = runtime

    def principal(name):
        return Principal(name, tenant, keys[name]["role"], "api_key")

    value = SimpleNamespace(
        runtime=runtime,
        client=runtime._client,
        tenant=tenant,
        store=AccessStore(runtime),
        app=app,
        principal=principal,
    )
    try:
        yield value
    finally:
        owned = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}:*")]
        if owned:
            await runtime._client.delete(*owned)
        await runtime.close()


async def owner(env, kind="goal", resource="one", subject="alice", actor_type="human"):
    async with env.client.pipeline(transaction=True) as pipe:
        await guard_owner(pipe, env.tenant, kind, resource, subject, actor_type)
        pipe.multi()
        queue_owner(pipe, env.tenant, kind, resource, subject, actor_type)
        await pipe.execute()


def request(env, key="alice"):
    return Request(
        {
            "type": "http",
            "app": env.app,
            "method": "GET",
            "path": "/",
            "headers": [(b"x-api-key", key.encode())],
            "query_string": b"",
        }
    )


@pytest.mark.parametrize(
    "kind",
    ["agent", "skill", "planning_session", "goal", "run", "automation", "conversation", "finding"],
)
async def test_missing_legacy_acl_is_admin_only_and_tenants_never_share(env, kind):
    assert not await env.store.can(env.principal("alice"), kind, "one")
    assert await env.store.can(env.principal("admin"), kind, "one")
    await owner(env, kind)
    assert await env.store.can(env.principal("alice"), kind, "one", "operate")
    assert not await env.store.can(env.principal("bob"), kind, "one")
    foreign = Principal("alice", env.tenant + "-foreign", "operator", "api_key")
    assert not await env.store.can(foreign, kind, "one")
    # Typed actors prevent a service Agent from impersonating a human subject.
    service = Principal("alice", env.tenant, "operator", "agent", actor_type="agent")
    assert not await env.store.can(service, kind, "one")


async def test_share_read_operate_revoke_and_revision_aliases(env):
    await owner(env, "skill")
    body = AccessUpdate(expected_version=1, grants=[{"subject": "bob", "permission": "read"}])
    await env.store.update(env.principal("alice"), "skill", "one", body)
    assert await env.store.can(env.principal("bob"), "skill_revision", "one:2")
    assert not await env.store.can(env.principal("bob"), "skill", "one", "operate")
    assert await env.store.allowed_resources(env.principal("bob")) == ["skill:one"]
    await env.store.update(
        env.principal("alice"), "skill", "one", AccessUpdate(expected_version=2, grants=[])
    )
    assert not await env.store.can(env.principal("bob"), "skill_revision", "one:2")
    assert await env.store.allowed_resources(env.principal("bob")) == []
    assert await env.store.can(env.principal("alice"), "skill", "one")


async def test_grant_cas_has_one_winner_and_failed_index_write_changes_nothing(env):
    await owner(env)
    body = AccessUpdate(expected_version=1, grants=[{"subject": "bob", "permission": "operate"}])
    result = await asyncio.gather(
        *(env.store.update(env.principal("alice"), "goal", "one", body) for _ in range(2)),
        return_exceptions=True,
    )
    assert sum(isinstance(value, dict) for value in result) == 1
    assert (
        sum(isinstance(value, HTTPException) and value.status_code == 409 for value in result) == 1
    )
    before = await env.store.get(env.tenant, "goal", "one")
    audit = await env.client.xlen(f"tenant:{env.tenant}:audit")
    bad = actor_index(env.tenant, {"actor_type": "human", "subject": "viewer"})
    await env.client.set(bad, "wrong-type")
    with pytest.raises(HTTPException) as exc:
        await env.store.update(
            env.principal("alice"),
            "goal",
            "one",
            AccessUpdate(expected_version=2, grants=[{"subject": "viewer", "permission": "read"}]),
        )
    assert exc.value.status_code == 503
    assert await env.store.get(env.tenant, "goal", "one") == before
    assert await env.client.xlen(f"tenant:{env.tenant}:audit") == audit
    assert await env.store.can(env.principal("bob"), "goal", "one", "operate")


async def test_legacy_preview_is_read_only_and_adoption_is_explicit_live_admin_only(env):
    await env.client.set(f"tenant:{env.tenant}:agent:goal:old", '{"created_by":"UNTRUSTED"}')
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=env.app), base_url="http://test"
    ) as c:
        url = "/v1/access/goal/old"
        preview = await c.get("/v1/access/legacy-preview/goal/old", headers={"x-api-key": "admin"})
        assert preview.json()["can_adopt"] and not preview.json()["writes"]
        assert await env.store.get(env.tenant, "goal", "old") is None
        body = {"expected_version": 0, "legacy_owner": {"subject": "alice"}}
        assert (await c.put(url, json=body, headers={"x-api-key": "alice"})).status_code == 404
        assert (
            await c.put(url, json={"expected_version": 0}, headers={"x-api-key": "admin"})
        ).status_code == 409
        assert (await c.put(url, json=body, headers={"x-api-key": "admin"})).status_code == 200
        assert (
            await c.put("/v1/access/goal/missing", json=body, headers={"x-api-key": "admin"})
        ).status_code == 404
        assert (await c.get(url, headers={"x-api-key": "bob"})).status_code == 404
        assert (
            await c.put(url, json=body | {"expected_version": 1}, headers={"x-api-key": "alice"})
        ).status_code == 422
        for body in [
            {"expected_version": "PRIVATE"},
            {"expected_version": 1, "grants": [{"subject": "PRIVATE\ud800", "permission": "read"}]},
        ]:
            response = await c.put(
                url,
                content=json.dumps(body),
                headers={"x-api-key": "alice", "content-type": "application/json"},
            )
            assert response.status_code == 422 and "PRIVATE" not in response.text


async def test_route_acl_and_list_filter_preserve_rbac(env):
    record = {"agent_id": "one", "tenant_id": env.tenant, "created_by": "alice"}
    await env.client.set(f"tenant:{env.tenant}:agents:record:one", json.dumps(record))
    await env.client.sadd(f"tenant:{env.tenant}:agents", "one")
    await owner(env, "agent")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=env.app), base_url="http://test"
    ) as c:
        assert (await c.get("/v1/agents/one", headers={"x-api-key": "alice"})).status_code == 200
        assert (await c.get("/v1/agents/one", headers={"x-api-key": "bob"})).status_code == 404
        assert (await c.get("/v1/agents", headers={"x-api-key": "bob"})).json() == {"agents": []}
        await env.store.update(
            env.principal("alice"),
            "agent",
            "one",
            AccessUpdate(
                expected_version=1,
                grants=[
                    {"subject": "bob", "permission": "read"},
                    {"subject": "viewer", "permission": "operate"},
                ],
            ),
        )
        assert (await c.get("/v1/agents/one", headers={"x-api-key": "bob"})).status_code == 200
        # A grant cannot elevate the principal's global role.
        assert (
            await c.post(
                "/v1/agents/one/goals", json={"objective": "test"}, headers={"x-api-key": "viewer"}
            )
        ).status_code == 403
        assert (
            await c.post(
                "/v1/agents/one/goals", json={"objective": "test"}, headers={"x-api-key": "bob"}
            )
        ).status_code == 404


async def test_live_sse_check_revocation_and_removed_identity(env):
    await owner(env)
    await env.store.update(
        env.principal("alice"),
        "goal",
        "one",
        AccessUpdate(expected_version=1, grants=[{"subject": "bob", "permission": "read"}]),
    )
    req, principal = request(env, "bob"), env.principal("bob")
    assert await stream_access(req, principal, "goal", "one")
    await env.store.update(env.principal("alice"), "goal", "one", AccessUpdate(expected_version=2))
    assert not await stream_access(req, principal, "goal", "one")
    assert not await stream_access(request(env, "foreign"), principal, "goal", "one")
    env.app.state.settings.api_keys_json = "{}"
    assert not await stream_access(request(env), env.principal("alice"), "goal", "one")


@pytest.mark.parametrize("kind", ["goal", "run", "automation"])
async def test_sse_generators_do_not_yield_buffered_events_after_revocation(env, monkeypatch, kind):
    from app.api.routes import replay_agent_events
    from app.automations import automation_events
    from app.core.automation_store import AutomationStore
    from app.goals import stream_goal_events

    await owner(env, kind)
    await env.store.update(
        env.principal("alice"),
        kind,
        "one",
        AccessUpdate(expected_version=1, grants=[{"subject": "bob", "permission": "read"}]),
    )
    suffix = {"goal": "agent:goal", "run": "agent:run", "automation": "automation"}[kind]
    await env.client.set(f"tenant:{env.tenant}:{suffix}:one", json.dumps({"status": "completed"}))

    async def batch(*args, **kwargs):
        return [("1-0", {"type": "first"}), ("1-1", {"type": "PRIVATE-SECOND"})]

    monkeypatch.setattr(env.runtime, "read_goal_events", batch)
    monkeypatch.setattr(env.runtime, "read_agent_events", batch)
    monkeypatch.setattr(AutomationStore, "events", batch)
    req = request(env, "bob")

    async def connected():
        return False

    monkeypatch.setattr(req, "is_disconnected", connected)
    fn = {"goal": stream_goal_events, "run": replay_agent_events, "automation": automation_events}[
        kind
    ]
    response = await fn("one", req)
    assert "first" in await anext(response.body_iterator)
    await env.store.update(env.principal("alice"), kind, "one", AccessUpdate(expected_version=2))
    with pytest.raises(StopAsyncIteration):
        await anext(response.body_iterator)


async def test_conversation_first_writer_wins_and_legacy_is_not_claimed(env):
    result = await asyncio.gather(
        *(
            claim_conversation(request(env, user), env.principal(user), "shared")
            for user in ("alice", "bob")
        ),
        return_exceptions=True,
    )
    assert result.count(None) == 1
    assert (
        sum(isinstance(value, HTTPException) and value.status_code == 404 for value in result) == 1
    )
    await env.client.rpush(f"tenant:{env.tenant}:conversation:old:messages", "PRIVATE")
    with pytest.raises(HTTPException):
        await claim_conversation(request(env), env.principal("alice"), "old")
    await claim_conversation(request(env, "admin"), env.principal("admin"), "old")
    assert await env.store.get(env.tenant, "conversation", "old") is None


async def test_admission_registers_goal_run_and_conversation_owners(env):
    goal = {
        "goal_id": "one",
        "tenant_id": env.tenant,
        "status": "queued",
        "created_by": "alice",
        "objective": "test",
        "steps": [],
        "policy": {"token_budget": 100},
    }
    await GoalExecutionStore(env.runtime).submit_manual(goal)
    for kind in ("goal", "conversation"):
        assert await env.store.can(env.principal("alice"), kind, "one", "operate")
    await env.runtime.save_run_state(
        "run-one", {"created_by": "alice", "status": "running"}, env.tenant
    )
    assert await env.store.can(env.principal("alice"), "run", "run-one")
    await env.client.rpush(f"tenant:{env.tenant}:conversation:one:messages", "test")
    assert await env.runtime.delete_conversation("one", env.tenant)
    assert not await env.store.can(env.principal("alice"), "conversation", "one")
    with pytest.raises(HTTPException) as exc:
        await env.store.update(
            env.principal("alice"), "conversation", "one", AccessUpdate(expected_version=2)
        )
    assert exc.value.status_code == 409


async def test_skill_creation_still_registers_owner_with_lazy_acl_import(env):
    from app.core.skill_store import SkillStore

    skill = await SkillStore(env.runtime).create(
        env.tenant,
        {
            "name": "ownership-proof",
            "display_name": "所有權驗證",
            "description": "合成測試",
            "instructions": "只整理已提供的證據。",
            "required_tools": [],
        },
        "alice",
    )
    assert await env.store.can(env.principal("alice"), "skill", skill["skill_id"], "operate")
    assert not await env.store.can(env.principal("bob"), "skill", skill["skill_id"])


async def test_history_fresh_filter_and_corrupt_authority_fail_closed(env):
    await owner(env, "skill")
    rows = [
        {"kind": "skill_revision", "resource_id": "one:1"},
        {"kind": "skill_revision", "resource_id": "hidden:1"},
        {"kind": "audit", "resource_id": "audit"},
    ]
    assert await filter_history(request(env), env.principal("alice"), rows) == rows[:1]
    assert await history_permissions(request(env), env.principal("alice")) == {
        "authorized_resources": ["skill:one"]
    }
    await env.client.set(acl_key(env.tenant, "skill", "one"), "PRIVATE-CORRUPTION")
    with pytest.raises(HTTPException) as exc:
        await filter_history(request(env), env.principal("alice"), rows)
    assert exc.value.status_code == 503 and "PRIVATE" not in exc.value.detail


@pytest.mark.parametrize(
    "path,method",
    [
        ("/v1/history/records", "list_records"),
        ("/v1/history/search?q=proof", "search_documents"),
        ("/v1/history/events", "events"),
        ("/v1/history/records/goal/one", "get_record"),
    ],
)
async def test_history_rechecks_acl_after_archive_query(env, path, method):
    from app.history import router

    env.app.include_router(router)
    env.app.state.settings.postgres_history_mode = "postgres"
    await owner(env)
    await env.store.update(
        env.principal("alice"),
        "goal",
        "one",
        AccessUpdate(expected_version=1, grants=[{"subject": "bob", "permission": "read"}]),
    )

    async def verified():
        return {"status": "verified", "schema_version": SCHEMA_VERSION}

    async def query(*args, **kwargs):
        if method != "get_record":
            assert kwargs["authorized_resources"] == ["goal:one"]
        await env.store.update(
            env.principal("alice"), "goal", "one", AccessUpdate(expected_version=2)
        )
        row = {
            "kind": "goal",
            "resource_id": "one",
            "data": {"objective": "PRIVATE"},
            "cursor": "1",
        }
        return row if method == "get_record" else [row]

    env.app.state.postgres = SimpleNamespace(latest_migration=verified, **{method: query})
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=env.app), base_url="http://test"
    ) as c:
        response = await c.get(path, headers={"x-api-key": "bob"})
    assert "PRIVATE" not in response.text
    assert response.status_code == (404 if method == "get_record" else 200)


@pytest.mark.parametrize("user", ["alice", "admin"])
async def test_history_authority_outage_is_503_not_pg_fallback(env, monkeypatch, user):
    from app.history import router

    env.app.include_router(router)
    env.app.state.settings.postgres_history_mode = "postgres"

    async def verified():
        return {"status": "verified", "schema_version": SCHEMA_VERSION}

    async def broken(*args, **kwargs):
        raise ConnectionError("PRIVATE-REDIS-LOCATION")

    env.app.state.postgres = SimpleNamespace(latest_migration=verified)
    monkeypatch.setattr(env.client, "ping", broken)
    monkeypatch.setattr(env.client, "get", broken)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=env.app), base_url="http://test"
    ) as c:
        for path in ("/v1/history/records", "/v1/history/records/goal/one"):
            response = await c.get(path, headers={"x-api-key": user})
            assert response.status_code == 503 and "PRIVATE" not in response.text


async def test_acl_disabled_preserves_legacy_api_behavior(env):
    env.app.state.settings.resource_acl_enabled = False
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=env.app), base_url="http://test"
    ) as c:
        assert (
            await c.get("/v1/access/goal/one", headers={"x-api-key": "admin"})
        ).status_code == 409
    assert await filter_history(
        request(env), env.principal("bob"), [{"kind": "goal", "resource_id": "private"}]
    )
