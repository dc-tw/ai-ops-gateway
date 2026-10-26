"""True Redis/PostgreSQL projection, fault recovery and migration boundaries.

Every test owns a UUID schema, tenant and source ID. The backfill scanner is
scoped to that tenant so unrelated tests' retained Redis data is never copied.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from types import SimpleNamespace
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from app import postgres_worker as worker
from app.core.config import Settings
from app.core.durable_outbox import maintenance_key, outbox_key
from app.core.postgres_store import PostgresStore
from app.core.redis_runtime import RedisRuntime
from app.main import create_app


@pytest.fixture
async def projection(monkeypatch):
    redis_url, postgres_url = os.getenv("TEST_REDIS_URL"), os.getenv("TEST_POSTGRES_URL")
    if not redis_url or not postgres_url:
        pytest.skip("Projection checks require TEST_REDIS_URL and TEST_POSTGRES_URL")
    case = uuid4().hex
    source, tenant, schema = "projection-" + case, "projection-" + case, "test_gateway_" + case
    raw = Redis.from_url(redis_url, decode_responses=True)
    await raw.ping()
    admin = await psycopg.AsyncConnection.connect(postgres_url, autocommit=True)
    await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    dsn = make_conninfo(postgres_url, options=f"-c search_path={schema}")
    store = PostgresStore(dsn, source)
    runtime = RedisRuntime(redis_url)
    writer = RedisRuntime(redis_url, durable_source_id=source)
    try:
        await store.open(migrate=True)
        await runtime.ping()
        scan = runtime._client.scan_iter

        def scoped_scan(*args, **kwargs):
            if kwargs.get("match") == "tenant:*":
                kwargs["match"] = f"tenant:{tenant}:*"
            return scan(*args, **kwargs)

        monkeypatch.setattr(runtime._client, "scan_iter", scoped_scan)
        yield SimpleNamespace(
            source=source,
            tenant=tenant,
            runtime=runtime,
            writer=writer,
            raw=raw,
            store=store,
            stream=outbox_key(source),
            dsn=dsn,
            redis_url=redis_url,
            key=lambda suffix: f"tenant:{tenant}:{suffix}",
        )
    finally:
        await runtime.close()
        await writer.close()
        await store.close()
        own_keys = [key async for key in raw.scan_iter(match=f"tenant:{tenant}:*")]
        own_keys += [key async for key in raw.scan_iter(match=f"gateway:durable:{source}:*")]
        if own_keys:
            await raw.delete(*own_keys)
        await raw.aclose()
        await admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        await admin.close()


async def set_goal(env, resource="goal-one", **changes):
    value = {
        "tenant_id": env.tenant,
        "goal_id": resource,
        "status": "completed",
        "name": "Projection milestone",
    } | changes
    await env.writer._client.set(env.key(f"agent:goal:{resource}"), json.dumps(value))
    return value


async def pending(env):
    return (await env.raw.xpending(env.stream, worker.GROUP))["pending"]


async def test_two_real_consumers_compete_without_duplicate_projections(projection):
    env = projection
    for number in range(20):
        await set_goal(env, f"goal-{number}")

    async def consumer(name):
        counts = {"applied": 0, "duplicates": 0, "failed": 0}
        for _ in range(15):
            result = await worker.project_batch(
                env.runtime, env.store, consumer=name, lease_ms=5_000, count=1
            )
            for label in counts:
                counts[label] += result[label]
            await asyncio.sleep(0)
        return counts

    results = await asyncio.gather(consumer("replica-one"), consumer("replica-two"))
    assert sum(result["applied"] for result in results) == 20
    assert all(result["failed"] == 0 for result in results)
    assert await pending(env) == 0
    status = await env.store.status(env.tenant)
    assert status["records"] == status["applied_operations"] == 20
    rows = await env.store.list_records(env.tenant)
    assert len({row["resource_id"] for row in rows}) == 20


async def test_commit_before_ack_failure_is_reclaimed_and_deduplicated(projection, monkeypatch):
    env = projection
    await set_goal(env)
    acknowledge = env.runtime._client.xack

    async def interrupted_ack(*args, **kwargs):
        raise ConnectionError("Simulated process interruption after Postgres commit")

    monkeypatch.setattr(env.runtime._client, "xack", interrupted_ack)
    with pytest.raises(ConnectionError, match="interruption"):
        await worker.project_batch(env.runtime, env.store, consumer="crashed-owner")
    assert await pending(env) == 1
    assert (await env.store.status(env.tenant))["applied_operations"] == 1
    monkeypatch.setattr(env.runtime._client, "xack", acknowledge)
    result = await worker.project_batch(
        env.runtime, env.store, consumer="recovery-owner", lease_ms=0
    )
    assert result == {"applied": 0, "duplicates": 1, "failed": 0}
    assert await pending(env) == 0
    assert (await env.store.status(env.tenant))["records"] == 1
    assert (await env.store.status(env.tenant))["applied_operations"] == 1


async def test_transient_postgres_failure_retains_pending_and_recovers(
    projection, monkeypatch, caplog
):
    env = projection
    private = "PRIVATE_PG_TRANSIENT_CANARY"
    await set_goal(env, name=private)
    apply = env.store.apply

    async def unavailable(*args, **kwargs):
        raise psycopg.OperationalError(private)

    monkeypatch.setattr(env.store, "apply", unavailable)
    assert await worker.project_batch(env.runtime, env.store) == {
        "applied": 0,
        "duplicates": 0,
        "failed": 1,
    }
    assert await pending(env) == 1
    assert (await env.store.status(env.tenant))["applied_operations"] == 0
    assert private not in caplog.text
    monkeypatch.setattr(env.store, "apply", apply)
    assert (await worker.project_batch(env.runtime, env.store, lease_ms=0))["applied"] == 1
    assert await pending(env) == 0
    assert (await env.store.get_record(env.tenant, "goal", "goal-one"))["data"]["name"] == private


@pytest.mark.parametrize("command", ["backfill", "reconcile"])
async def test_poison_is_pending_metadata_only_allows_progress_but_refuses_cutover(
    projection, monkeypatch, caplog, command
):
    env = projection
    private = "PRIVATE_POISON_PAYLOAD_CANARY"
    poison = await env.raw.xadd(
        env.stream,
        {"payload": json.dumps({"schema_version": 999, "secret": private})},
    )
    await set_goal(env)
    result = await worker.project_batch(env.runtime, env.store)
    assert result == {"applied": 1, "duplicates": 0, "failed": 1}
    assert await pending(env) == 1
    assert await env.store.get_record(env.tenant, "goal", "goal-one") is not None
    error_rows = await env.raw.xrange(env.stream + ":errors")
    assert len(error_rows) == 1
    assert error_rows[0][1] == {"event_id": poison, "error": "invalid_envelope"}
    assert private not in json.dumps(error_rows) + caplog.text
    await worker.project_batch(env.runtime, env.store, lease_ms=0)
    assert len(await env.raw.xrange(env.stream + ":errors")) == 1
    drain = worker.drain

    async def immediate_drain(runtime, store):
        return await drain(runtime, store, deadline_seconds=0)

    monkeypatch.setattr(worker, "drain", immediate_drain)
    with pytest.raises(RuntimeError, match="unresolved pending"):
        await getattr(worker, command)(env.runtime, env.store)
    assert await env.raw.get(maintenance_key(env.source)) is None
    assert (await env.store.latest_migration())["status"] in {"backfilling", "verifying"}
    assert await pending(env) == 1


async def legacy_sources(env):
    goal = {"tenant_id": env.tenant, "goal_id": "goal-one", "status": "completed"}
    await env.raw.set(env.key("agent:goal:goal-one"), json.dumps(goal))
    event = {"type": "goal_completed", "goal_id": "goal-one"}
    event_id = await env.raw.xadd(
        env.key("agent:goal:events:goal-one"), {"payload": json.dumps(event)}
    )
    messages = [
        {"request_id": "request-one", "role": "user", "content": "Synthetic question"},
        {"request_id": "request-one", "role": "assistant", "content": "Synthetic answer"},
    ]
    await env.raw.rpush(
        env.key("conversation:thread-one:messages"), *(json.dumps(row) for row in messages)
    )
    for suffix in (
        "credentials:api-key",
        "quota:tokens:today",
        "checkpoint:private",
        "conversation:thread-one:summary",
        "conversation:thread-one:memory",
    ):
        await env.raw.set(env.key(suffix), "PRIVATE_NON_ARCHIVAL_CANARY")
    return event_id


async def test_backfill_reconcile_repetition_deduplicates_and_excludes_execution_secrets(
    projection,
):
    env = projection
    original_id = await legacy_sources(env)
    for _ in range(2):
        filled = await worker.backfill(env.runtime, env.store)
        assert filled["status"] == "backfilled" and filled["snapshots"] == 4
        verified = await worker.reconcile(env.runtime, env.store)
        assert verified["status"] == "verified"
        assert verified["checked"] == 4 and verified["mismatches"] == 0
        assert await env.raw.get(maintenance_key(env.source)) is None
    status = await env.store.status(env.tenant)
    assert status["records"] == 2 and status["events"] == 3
    assert status["applied_operations"] == 8
    assert [row["event_id"] for row in await env.store.events(env.tenant, "goal")] == [original_id]
    conversation = await env.store.events(env.tenant, "conversation", "thread-one")
    assert [row["data"]["role"] for row in conversation] == ["user", "assistant"]
    assert all(row["event_id"].startswith("request-one:") for row in conversation)
    rows = await env.store.list_records(env.tenant)
    assert "PRIVATE_NON_ARCHIVAL_CANARY" not in json.dumps(rows + conversation)
    assert await env.store.search(env.tenant, "PRIVATE_NON_ARCHIVAL_CANARY") == []
    assert await pending(env) == 0
    assert (await env.store.latest_migration())["status"] == "verified"


async def test_delete_and_purge_fences_survive_repeated_backfill_and_reconcile(projection):
    env = projection
    await legacy_sources(env)
    await worker.backfill(env.runtime, env.store)
    await env.writer.delete_conversation("thread-one", env.tenant)
    await worker.drain(env.runtime, env.store, deadline_seconds=2)
    cutoff = (await env.store.status(env.tenant))["latest_cursor"]
    purged = await env.store.purge(env.tenant, "goal", cutoff)
    assert purged["records"] == 1 and purged["events"] == 1
    # Simulate an older message cache restored after explicit deletion. Neither
    # this legacy data nor the still-retained Goal may be resurrected by backfill.
    await env.raw.rpush(
        env.key("conversation:thread-one:messages"),
        json.dumps({"request_id": "request-one", "role": "user", "content": "Old cached data"}),
    )
    for _ in range(2):
        await worker.backfill(env.runtime, env.store)
        report = await worker.reconcile(env.runtime, env.store)
        assert report["status"] == "verified" and report["excluded"] == 3
        assert await env.store.get_record(env.tenant, "goal", "goal-one") is None
        assert await env.store.get_record(env.tenant, "conversation", "thread-one") is None
        assert await env.store.events(env.tenant) == []


async def test_maintenance_blocks_domain_writes_and_releases_its_own_lease(projection):
    env = projection
    async with worker.maintenance(env.runtime, env.source) as check:
        assert await env.raw.get(maintenance_key(env.source))
        await check()
        with pytest.raises(RuntimeError, match="Another durable maintenance"):
            async with worker.maintenance(env.runtime, env.source):
                raise AssertionError("Second owner must not acquire maintenance")
        with pytest.raises(ResponseError, match="DURABLE_MAINTENANCE"):
            await set_goal(env)
        assert await env.raw.get(env.key("agent:goal:goal-one")) is None
        assert await env.raw.xlen(env.stream) == 0
    assert await env.raw.get(maintenance_key(env.source)) is None
    await set_goal(env)
    assert await env.raw.xlen(env.stream) == 1


async def test_maintenance_exception_cleanup_does_not_delete_another_owner(projection):
    env = projection
    with pytest.raises(ValueError, match="Injected maintenance failure"):
        async with worker.maintenance(env.runtime, env.source):
            raise ValueError("Injected maintenance failure")
    assert await env.raw.get(maintenance_key(env.source)) is None
    with pytest.raises(RuntimeError, match="lease lost"):
        async with worker.maintenance(env.runtime, env.source) as check:
            await env.raw.set(maintenance_key(env.source), "replacement-owner", px=60_000)
            await check()
    assert await env.raw.get(maintenance_key(env.source)) == "replacement-owner"


async def test_cli_backfill_failure_closes_connections_and_releases_maintenance(
    projection, monkeypatch
):
    env = projection
    settings = SimpleNamespace(
        postgres_dsn=env.dsn, postgres_source_id=env.source, redis_url=env.redis_url
    )
    closed = {"redis": 0, "postgres": 0}
    close_runtime, close_store = env.runtime.close, env.store.close

    async def close_redis():
        closed["redis"] += 1
        await close_runtime()

    async def close_postgres():
        closed["postgres"] += 1
        await close_store()

    async def invalid_snapshot(runtime):
        assert await env.raw.get(maintenance_key(env.source))
        raise ValueError("Injected invalid source snapshot")
        yield  # async generator contract; no private payload is manufactured.

    monkeypatch.setattr(worker, "get_settings", lambda: settings)
    monkeypatch.setattr(worker, "PostgresStore", lambda *args: env.store)
    monkeypatch.setattr(worker, "RedisRuntime", lambda *args: env.runtime)
    monkeypatch.setattr(worker, "snapshots", invalid_snapshot)
    monkeypatch.setattr(env.runtime, "close", close_redis)
    monkeypatch.setattr(env.store, "close", close_postgres)
    monkeypatch.setattr(sys, "argv", ["postgres_worker", "backfill"])
    with pytest.raises(ValueError, match="invalid source"):
        await worker.main()
    assert closed == {"redis": 1, "postgres": 1}
    assert env.store.pool is None
    assert await env.raw.get(maintenance_key(env.source)) is None


async def test_compaction_keeps_pending_and_unread_positions(projection):
    env = projection
    for number in range(4):
        await set_goal(env, f"goal-{number}")
    ids = [event_id for event_id, _ in await env.raw.xrange(env.stream)]
    await worker.ensure_group(env.raw, env.stream)
    await env.raw.xreadgroup(worker.GROUP, "crashed-projector", {env.stream: ">"}, count=3)
    await env.raw.xack(env.stream, worker.GROUP, ids[0], ids[2])
    result = await worker.compact(env.runtime, env.source)
    assert result["removed"] == 1
    assert result["retained_from"] == ids[1]
    assert [event_id for event_id, _ in await env.raw.xrange(env.stream)] == ids[1:]
    assert await pending(env) == 1
    assert (await env.raw.xpending_range(env.stream, worker.GROUP, "-", "+", 10))[0][
        "message_id"
    ] == ids[1]
    recovered = await worker.project_batch(env.runtime, env.store, lease_ms=0)
    assert recovered["applied"] == 2 and recovered["failed"] == 0
    assert await pending(env) == 0
    await worker.compact(env.runtime, env.source)
    assert [event_id for event_id, _ in await env.raw.xrange(env.stream)] == [ids[-1]]


async def test_redis_unavailable_keeps_true_postgres_history_but_never_allows_execution(projection):
    env = projection
    await set_goal(env)
    await worker.drain(env.runtime, env.store, deadline_seconds=2)
    await env.store.save_migration({"status": "verified", "report_id": uuid4().hex})
    keys = {
        "viewer": {"subject": "reader", "tenant_id": env.tenant, "role": "viewer"},
        "admin": {"subject": "owner", "tenant_id": env.tenant, "role": "admin"},
    }
    settings = Settings(
        app_env="production",
        api_keys_json=json.dumps(keys),
        postgres_dsn=env.dsn,
        postgres_source_id=env.source,
        postgres_history_mode="postgres",
        langgraph_checkpointing=False,
    )
    app = create_app(settings=settings)
    app.state.settings, app.state.postgres, app.state.redis = settings, env.store, None
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"x-api-key": "admin"},
    ) as client:
        response = await client.get("/v1/history/records/goal/goal-one")
        assert response.status_code == 200
        assert response.json()["record"]["data"]["status"] == "completed"
        assert response.json()["execution_authority"] is False
        for method, path, body in (
            ("GET", "/v1/agents", None),
            ("POST", "/v1/goals", {"objective": "Must not execute from archive"}),
            ("POST", "/v1/agent/stream", {"query": "Must not execute from archive"}),
        ):
            response = await client.request(method, path, json=body)
            assert response.status_code == 503
            assert response.json() == {"detail": "Execution requires Redis"}


async def test_exact_one_mib_escaped_source_survives_real_outbox_projection(projection):
    env = projection
    value = json.dumps({"text": "\\" * 524_282})
    assert len(value.encode()) == 1_048_576
    await env.writer._client.set(env.key("agent:run:escaped-run"), value)
    result = await worker.project_batch(env.runtime, env.store)
    assert result == {"applied": 1, "duplicates": 0, "failed": 0}
    assert (await env.store.get_record(env.tenant, "run", "escaped-run"))["data"] == json.loads(
        value
    )
    assert await pending(env) == 0


async def test_multi_message_envelope_is_not_confused_with_individual_payload_limit(projection):
    env = projection
    values = [
        json.dumps(
            {"request_id": f"request-{number}", "role": "assistant", "content": "\\" * 300_000}
        )
        for number in range(2)
    ]
    await env.writer._client.rpush(env.key("conversation:escaped:messages"), *values)
    assert (await worker.project_batch(env.runtime, env.store))["failed"] == 0
    rows = await env.store.events(env.tenant, "conversation", "escaped")
    assert len(rows) == 2 and [row["data"] for row in rows] == [json.loads(raw) for raw in values]


async def test_reclaim_continues_beyond_poison_prefix_to_later_crashed_owner_work(projection):
    env = projection
    for _ in range(21):
        await env.raw.xadd(env.stream, {"payload": '{"schema_version":999}'})
    await set_goal(env)
    await worker.ensure_group(env.raw, env.stream)
    await env.raw.xreadgroup(worker.GROUP, "crashed-owner", {env.stream: ">"}, count=100)
    assert await pending(env) == 22
    for _ in range(8):
        await worker.project_batch(env.runtime, env.store, consumer="recovery", lease_ms=0, count=5)
    assert await env.store.get_record(env.tenant, "goal", "goal-one") is not None
    assert await pending(env) == 21  # Invalid work is visible, not silently ACKed.


async def test_real_finding_review_and_cache_eviction_keep_only_safe_postgres_history(projection):
    env = projection
    await env.writer.save_threat_findings(
        env.tenant,
        {
            "event_type": "tool_response",
            "location": "tool_response:browser_close",
            "findings": [
                {
                    "finding_id": "finding-one",
                    "rule_id": "ATR-TEST",
                    "title": "Synthetic finding",
                    "severity": "high",
                    "enforcement_action": "blocked",
                    "matched_text": "RAW_ATTACK_CANARY",
                }
            ],
        },
        context_id="run-one",
        model="test-model",
        ttl_seconds=60,
        max_records=100,
        emit_internal_events=False,
    )
    await worker.drain(env.runtime, env.store, deadline_seconds=2)
    row = await env.store.get_record(env.tenant, "finding", "finding-one")
    assert row["data"]["review_status"] == "unreviewed"
    assert "RAW_ATTACK_CANARY" not in json.dumps(row)
    await env.writer.review_threat_finding(
        env.tenant,
        "finding-one",
        review_status="false_positive",
        reason="Synthetic review",
        reviewer="human-reviewer",
        ttl_seconds=60,
    )
    await worker.drain(env.runtime, env.store, deadline_seconds=2)
    await env.writer._client.delete(env.key("security:threat-finding:finding-one"))
    await worker.drain(env.runtime, env.store, deadline_seconds=2)
    row = await env.store.get_record(env.tenant, "finding", "finding-one")
    assert row["data"]["review_status"] == "false_positive"
    assert len(await env.store.search(env.tenant, "ATR-TEST")) == 1


async def test_running_redis_connection_failure_returns_safe_503_in_pg_mode(
    projection, monkeypatch
):
    env = projection
    settings = Settings(api_key="test-key", postgres_dsn=env.dsn)
    app = create_app(settings=settings)
    app.state.settings, app.state.redis = settings, env.writer

    async def disconnected(*args, **kwargs):
        raise RedisConnectionError("PRIVATE_REDIS_CONNECTION_CANARY")

    monkeypatch.setattr(env.writer._client, "smembers", disconnected)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"x-api-key": "test-key"},
    ) as client:
        response = await client.get("/v1/agents")
    assert response.status_code == 503
    assert response.json() == {"detail": "Execution requires Redis"}
