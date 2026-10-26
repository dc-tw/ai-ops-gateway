"""Real PostgreSQL isolation, history ordering, migration and retention checks."""

import asyncio
import json
import logging
import os
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

from app.core.postgres_schema import SCHEMA_CHECKSUM, SCHEMA_VERSION
from app.core.postgres_store import (
    PostgresConflict,
    PostgresEnvelopeError,
    PostgresInvalidCursor,
    PostgresSchemaError,
    PostgresStore,
    SafePoolLogFilter,
    message_identity,
)


@pytest.fixture
async def database():
    url = os.getenv("TEST_POSTGRES_URL")
    if not url:
        pytest.skip("Real history storage checks require TEST_POSTGRES_URL")
    schema = "test_gateway_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(url, autocommit=True)
    await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    dsn = make_conninfo(url, options=f"-c search_path={schema}")
    connection = await psycopg.AsyncConnection.connect(dsn, autocommit=True, row_factory=dict_row)
    try:
        yield SimpleNamespace(dsn=dsn, connection=connection)
    finally:
        await connection.close()
        await admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        await admin.close()


@pytest.fixture
async def store(database):
    value = PostgresStore(database.dsn, source_id="test-" + uuid4().hex)
    await value.open(migrate=True)
    try:
        yield value
    finally:
        await value.close()


def record(kind="goal", resource="goal-one", tenant="acme", **data):
    suffixes = {
        "goal": f"agent:goal:{resource}",
        "agent": f"agents:record:{resource}",
        "skill": f"skills:record:{resource}",
        "tool_receipt": f"goal:goal-one:tool-receipt:{resource}",
    }
    return {
        "schema_version": 1,
        "command": "SET",
        "key": f"tenant:{tenant}:" + suffixes[kind],
        "value": json.dumps({"tenant_id": tenant, "status": "completed", **data}),
    }


def event(tenant="acme", event_id="100-0", **data):
    return {
        "schema_version": 1,
        "command": "XADD",
        "key": f"tenant:{tenant}:agent:goal:events:goal-one",
        "event_id": event_id,
        "value": {"payload": json.dumps({"type": "goal_completed", **data})},
    }


def messages(request="request-one", tenant="acme", **changes):
    return {
        "schema_version": 1,
        "command": "RPUSH",
        "key": f"tenant:{tenant}:conversation:thread-one:messages",
        "value": [
            json.dumps({"request_id": request, "role": "user", "content": "Question"}),
            json.dumps({"request_id": request, "role": "assistant", "content": "Answer"}),
        ],
        **changes,
    }


async def test_migration_is_explicit_checksummed_and_concurrent(database):
    unopened = PostgresStore(database.dsn)
    with pytest.raises(PostgresSchemaError, match="requires migration"):
        await unopened.open()
    assert unopened.pool is None
    stores = [PostgresStore(database.dsn, source_id=f"migration-{index}") for index in range(4)]
    try:
        await asyncio.gather(*[value.open(migrate=True) for value in stores])
        rows = await (
            await database.connection.execute(
                "SELECT version,checksum FROM gateway_schema_migrations"
            )
        ).fetchall()
        assert rows == [{"version": SCHEMA_VERSION, "checksum": SCHEMA_CHECKSUM}]
        await stores[0].open()  # An open pool is safe to reuse.
    finally:
        await asyncio.gather(*[value.close() for value in stores])


@pytest.mark.parametrize("change", ["unknown-version", "checksum", "missing-table"])
async def test_migration_refuses_unknown_or_corrupt_schema(database, change):
    value = PostgresStore(database.dsn)
    await value.open(migrate=True)
    await value.close()
    if change == "unknown-version":
        await database.connection.execute(
            "INSERT INTO gateway_schema_migrations(version,checksum) VALUES (99,'unknown')"
        )
    elif change == "checksum":
        await database.connection.execute("UPDATE gateway_schema_migrations SET checksum='changed'")
    else:
        await database.connection.execute("DROP TABLE gateway_records")
    for migrate in (False, True):
        with pytest.raises(PostgresSchemaError):
            await value.open(migrate=migrate)
        assert value.pool is None


async def test_apply_exact_retries_are_deduplicated_under_competition(store):
    body = record(name="Release milestone")
    results = await asyncio.gather(*[store.apply("10-1", body) for _ in range(12)])
    assert results.count(True) == 1 and results.count(False) == 11
    fetched = await store.get_record("acme", "goal", "goal-one")
    assert fetched["data"]["name"] == "Release milestone"
    assert fetched["source_cursor"] == "10-1" and fetched["archived_at"]
    assert (await store.status("acme"))["applied_operations"] == 1
    with pytest.raises(PostgresConflict):
        await store.apply("10-1", record(name="PRIVATE-COLLIDING-CONTENT"))
    with pytest.raises(PostgresConflict):
        await store.apply("10-1", record(tenant="foreign"))
    assert (await store.status("acme"))["applied_operations"] == 1


async def test_source_order_is_numeric_and_stale_records_cannot_replace_newer_state(store):
    await store.apply("10-0", record(status="completed"))
    await store.apply("9-100", record(status="running"))
    await store.apply("10-1", record(status="cancelled"))
    assert (await store.get_record("acme", "goal", "goal-one"))["data"]["status"] == "cancelled"
    assert (await store.status("acme"))["latest_cursor"] == "10-1"


async def test_sources_and_tenants_are_isolated(store, database):
    other = PostgresStore(database.dsn, source_id="other-source")
    await other.open()
    try:
        await store.apply("1-0", record(tenant="acme", name="Alpha"))
        await store.apply("2-0", record(tenant="foreign", name="Beta"))
        await other.apply("1-0", record(tenant="acme", name="Other source"))
        assert (await store.get_record("acme", "goal", "goal-one"))["data"]["name"] == "Alpha"
        assert (await other.get_record("acme", "goal", "goal-one"))["data"][
            "name"
        ] == "Other source"
        assert (await store.status("acme"))["records"] == 1
        assert (await store.status("acme"))["latest_cursor"] == "1-0"
        assert (await store.status("foreign"))["latest_cursor"] == "2-0"
        assert (await store.status())["records"] == 2
        assert await store.get_record("missing", "goal", "goal-one") is None
        assert await store.search("acme", "Beta") == []
        assert await store.search("foreign", "Alpha") == []
    finally:
        await other.close()


async def test_events_deduplicate_original_stream_ids_and_reject_payload_conflicts(store):
    value = event(status="completed")
    await store.apply("10-0", value)
    await store.apply("11-0", value | {"backfill": True})
    rows = await store.events("acme", "goal", "goal-one")
    assert len(rows) == 1 and rows[0]["event_id"] == "100-0"
    assert await store.event_matches(
        "acme", "goal", "goal-one", "100-0", {"type": "goal_completed", "status": "completed"}
    )
    assert not await store.event_matches(
        "foreign", "goal", "goal-one", "100-0", {"type": "goal_completed", "status": "completed"}
    )
    before = (await store.status())["applied_operations"]
    with pytest.raises(PostgresConflict):
        await store.apply("12-0", event(status="different"))
    assert (await store.status())["applied_operations"] == before
    await store.apply("13-0", event(event_id="101-0"))
    page = await store.events("acme", "goal", "goal-one", limit=1)
    rest = await store.events("acme", after=page[0]["cursor"])
    assert len(rest) == 1 and rest[0]["event_id"] == "101-0"


async def test_conversation_backfill_uses_request_role_identity(store):
    await store.apply("1-0", messages())
    await store.apply("2-0", messages(backfill=True))
    rows = await store.events("acme", "conversation", "thread-one")
    assert [row["event_id"] for row in rows] == [
        message_identity(json.loads(raw)) for raw in messages()["value"]
    ]
    assert (await store.get_record("acme", "conversation", "thread-one"))["data"] == {
        "conversation_id": "thread-one"
    }
    assert await store.event_matches(
        "acme",
        "conversation",
        "thread-one",
        message_identity(json.loads(messages()["value"][0])),
        json.loads(messages()["value"][0]),
    )


async def test_conversation_delete_fences_old_messages_and_allows_new_generation(
    store,
):
    await store.apply("10-0", messages())
    deletion = {"schema_version": 1, "command": "DEL", "key": messages()["key"]}
    await store.apply("30-0", deletion)
    assert await store.events("acme", "conversation", "thread-one") == []
    assert await store.get_record("acme", "conversation", "thread-one") is None
    assert await store.is_suppressed("acme", "conversation", "thread-one")
    await store.apply("20-0", messages(request="late-request"))
    await store.apply("50-0", messages(request="backfill-request", backfill=True))
    await store.apply(
        "51-0", messages()
    )  # Re-enqueued old request cannot recreate an empty record.
    assert await store.events("acme", "conversation", "thread-one") == []
    assert await store.get_record("acme", "conversation", "thread-one") is None
    await store.apply("60-0", messages(request="new-request"))
    await store.apply("40-0", deletion)  # A delayed older delete must not erase the new generation.
    rows = await store.events("acme", "conversation", "thread-one")
    assert len(rows) == 2 and all(row["event_id"].startswith("new-request:") for row in rows)
    assert await store.get_record("acme", "conversation", "thread-one") is not None
    assert not await store.event_matches(
        "acme", "conversation", "thread-one", "request-one:user", json.loads(messages()["value"][0])
    )


async def test_purge_retains_deduplication_and_backfill_fences_while_allowing_new_live_work(store):
    old = record(name="Purged history")
    await store.apply("10-0", old)
    await store.apply("20-0", record(resource="goal-new", name="Newer history"))
    await store.apply("11-0", event())
    result = await store.purge("acme", "goal", "15-0")
    assert result["records"] == result["events"] == 1
    assert await store.get_record("acme", "goal", "goal-one") is None
    assert await store.get_record("acme", "goal", "goal-new") is not None
    assert await store.events("acme", "goal") == []
    assert await store.is_suppressed("acme", "goal", "goal-one")
    assert await store.apply("10-0", old) is False
    await store.apply("14-0", record(name="Late prior record"))
    await store.apply("30-0", record(name="Backfilled prior record") | {"backfill": True})
    assert await store.get_record("acme", "goal", "goal-one") is None
    await store.apply("40-0", record(name="New live generation"))
    await store.apply("41-0", event())  # Retained identity stops resurrecting a purged event.
    assert (await store.get_record("acme", "goal", "goal-one"))["data"][
        "name"
    ] == "New live generation"
    assert await store.events("acme", "goal") == []


async def test_fts_and_literal_chinese_search_only_index_safe_metadata(store):
    await store.apply(
        "1-0",
        record(
            kind="agent",
            resource="agent-one",
            name="資安調查 Security Analyst",
            persona="PRIVATE_PERSONA_CANARY",
            instructions="PRIVATE_INSTRUCTION_CANARY",
            query="PRIVATE_QUERY_CANARY",
            answer="PRIVATE_ANSWER_CANARY",
        ),
    )
    assert len(await store.search("acme", "調查")) == 1
    assert len(await store.search("acme", "Security", kind="agent")) == 1
    for query in (
        "PRIVATE_PERSONA_CANARY",
        "PRIVATE_INSTRUCTION_CANARY",
        "PRIVATE_QUERY_CANARY",
        "PRIVATE_ANSWER_CANARY",
    ):
        assert await store.search("acme", query) == []
    assert await store.search("acme", "%") == []  # Substring search treats wildcards literally.
    assert await store.search("acme", "' OR 1=1 --") == []
    assert await store.search("foreign", "Security") == []
    row = await store.get_record("acme", "agent", "agent-one")
    assert row["data"]["persona"] == "PRIVATE_PERSONA_CANARY"


async def test_receipt_storage_strips_raw_arguments_and_unscanned_content(store):
    await store.apply(
        "1-0",
        record(
            kind="tool_receipt",
            resource="action-one",
            tool="browser_type",
            action_id="action-one",
            arguments={"password": "PRIVATE_PASSWORD"},
            raw_output="PRIVATE_RAW_TOOL",
        ),
    )
    value = await store.get_record("acme", "tool_receipt", "goal-one:action-one")
    assert value["data"] == {
        "status": "completed",
        "tool": "browser_type",
        "action_id": "action-one",
    }
    assert "PRIVATE" not in json.dumps(value)
    assert await store.search("acme", "PRIVATE_PASSWORD") == []


async def test_record_pagination_is_stable_across_kinds(store):
    for number, (kind, resource) in enumerate((("agent", "a"), ("goal", "b"), ("goal", "c")), 1):
        await store.apply(f"{number}-0", record(kind=kind, resource=resource))
    first = await store.list_records("acme", limit=2)
    last = await store.list_records("acme", after=first[-1]["cursor"])
    assert [(row["kind"], row["resource_id"]) for row in first + last] == [
        ("agent", "a"),
        ("goal", "b"),
        ("goal", "c"),
    ]


@pytest.mark.parametrize(
    "key",
    [
        "agent:result:request",
        "tenant:acme:quota:tokens:today",
        "tenant:acme:conversation:x:summary",
        "checkpoint:private",
        "tenant:foreign:unknown:key",
    ],
)
async def test_unknown_or_execution_keys_are_rejected_without_archiving_sensitive_payload(
    store, key
):
    with pytest.raises(PostgresEnvelopeError):
        await store.apply(
            "1-0",
            {"schema_version": 1, "command": "SET", "key": key, "value": '{"secret":"PRIVATE"}'},
        )
    assert (await store.status())["applied_operations"] == 0


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": 2},
        {"schema_version": True},
        {"command": "EVAL"},
        {"extra": "PRIVATE"},
        {"backfill": 1},
        {"value": '{"tenant_id":"foreign"}'},
        {"value": '{"snapshot":{"tenant_id":"foreign"}}'},
        {"value": '{"name":"\\ud800"}'},
        {"value": '{"value":NaN}'},
        {"value": '{"value":"\\u0000"}'},
        {"value": '{"key\\u0000":"PRIVATE"}'},
    ],
)
async def test_invalid_envelopes_fail_closed_without_persisting_or_leaking(store, change):
    with pytest.raises((PostgresEnvelopeError, PostgresInvalidCursor)) as error:
        await store.apply("1-0", record() | change)
    assert "PRIVATE" not in str(error.value)
    assert (await store.status())["applied_operations"] == 0


@pytest.mark.parametrize(
    "cursor", ["not-a-cursor", "01-0", "-1-0", "1--1", "18446744073709551616-0"]
)
async def test_invalid_source_cursors_are_rejected(store, cursor):
    with pytest.raises(PostgresInvalidCursor):
        await store.apply(cursor, record())


async def test_bad_read_pagination_and_foreign_nested_data_are_rejected(store):
    with pytest.raises(PostgresInvalidCursor):
        await store.list_records("acme", after="not-base64")
    with pytest.raises(PostgresInvalidCursor):
        await store.events("acme", after=True)
    with pytest.raises(ValueError):
        await store.list_records("acme", limit=0)
    with pytest.raises(ValueError):
        await store.get_record("other:tenant", "goal", "one")


async def test_migration_reports_remain_source_scoped_and_status_does_not_dump_reports(
    store, database
):
    report = {
        "source_id": store.source_id,
        "status": "verified",
        "details": {"raw": "PRIVATE-REPORT"},
    }
    assert await store.latest_migration() is None
    await store.save_migration(report)
    assert await store.latest_migration() == report
    assert "PRIVATE-REPORT" not in json.dumps(await store.status("acme"))
    other = PostgresStore(database.dsn, source_id="other-report-source")
    await other.open()
    try:
        assert await other.latest_migration() is None
    finally:
        await other.close()


async def test_distinct_actual_turns_of_one_request_do_not_conflict(store):
    first = messages()
    second = messages(
        value=[
            json.dumps(
                {
                    "request_id": "request-one",
                    "role": "assistant",
                    "content": "Answer",
                    "created_at": "2026-10-27T00:00:00Z",
                }
            )
        ]
    )
    await store.apply("1-0", first)
    await store.apply("2-0", second)
    await store.apply("3-0", first | {"backfill": True})
    rows = await store.events("acme", "conversation", "thread-one")
    assert len(rows) == 3 and len({row["event_id"] for row in rows}) == 3


async def test_delete_cannot_target_other_resource_families(store):
    body = record()
    await store.apply("1-0", body)
    with pytest.raises(PostgresEnvelopeError):
        await store.apply("2-0", body | {"command": "DEL", "value": None})
    assert await store.get_record("acme", "goal", "goal-one") is not None


def test_pool_logs_redact_all_connection_context_and_filter_is_installed_once():
    logger = logging.getLogger("psycopg.pool")
    PostgresStore("postgresql://PRIVATE@invalid/database")
    count = sum(isinstance(item, SafePoolLogFilter) for item in logger.filters)
    PostgresStore("postgresql://OTHER_PRIVATE@invalid/database")
    assert sum(isinstance(item, SafePoolLogFilter) for item in logger.filters) == count == 1
    record = logging.LogRecord(
        "psycopg.pool",
        logging.ERROR,
        "test",
        1,
        "PRIVATE DSN %s",
        ("PRIVATE_PASSWORD",),
        (ValueError, ValueError("PRIVATE_EXCEPTION"), None),
    )
    record.exc_text = "PRIVATE_TRACEBACK"
    record.stack_info = "PRIVATE_STACK"
    assert logger.filter(record)
    assert "PRIVATE" not in record.getMessage()
    assert record.exc_info is None and record.exc_text is None and record.stack_info is None


@pytest.mark.parametrize("tenant", ["acme", "empty-tenant"])
async def test_future_purge_cutoff_is_rejected_without_suppressing_new_work(store, tenant):
    await store.apply("10-0", record())
    with pytest.raises(PostgresInvalidCursor, match="observed tenant history"):
        await store.purge(tenant, "goal", "18446744073709551615-0")
    assert not await store.is_suppressed(tenant, "goal", "future-goal")
    await store.apply("11-0", record(tenant=tenant, resource="future-goal"))
    assert await store.get_record(tenant, "goal", "future-goal") is not None
