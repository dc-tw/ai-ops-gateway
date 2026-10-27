"""Typed, idempotent durable history. Redis remains execution authority."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import re
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from app.core.durable_types import (
    EVENT_KINDS,
    RECORD_KINDS,
    classify_key,
    conversation_id_from_key,
    sanitize_record,
    search_text,
    tenant_from_key,
)
from app.core.postgres_schema import SCHEMA_CHECKSUM, SCHEMA_SQL, SCHEMA_VERSION, TABLE_NAMES
from app.core.security import TENANT_PATTERN

_MAX_JSON_BYTES = 2 * 1_024 * 1_024
_MAX_ENVELOPE_BYTES = 8 * 1_024 * 1_024
_CURSOR = re.compile(r"^(0|[1-9][0-9]*)-(0|[1-9][0-9]*)$")


class PostgresSchemaError(RuntimeError):
    pass


class PostgresEnvelopeError(ValueError):
    pass


class PostgresConflict(ValueError):
    pass


class PostgresInvalidCursor(ValueError):
    pass


class SafePoolLogFilter(logging.Filter):
    """libpq diagnostics may include connection options; never log them here."""

    def filter(self, record):
        record.msg = "Postgres pool status changed; inspect service connectivity"
        record.args = ()
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        return True


def _position(value: Any) -> tuple[int, int]:
    if not isinstance(value, str) or len(value) > 41 or not _CURSOR.fullmatch(value):
        raise PostgresInvalidCursor("Invalid archive source cursor")
    parts = tuple(int(part) for part in value.split("-"))
    if any(part > 2**64 - 1 for part in parts):
        raise PostgresInvalidCursor("Invalid archive source cursor")
    return parts


def _source_cursor(row: dict[str, Any]) -> str:
    return f"{int(row['source_ms'])}-{int(row['source_seq'])}"


def _canonical(value: Any, *, max_bytes: int = _MAX_JSON_BYTES) -> str:
    try:

        def validate(item):
            if isinstance(item, str):
                if "\0" in item:
                    raise ValueError
            elif isinstance(item, dict):
                for key, child in item.items():
                    validate(key)
                    validate(child)
            elif isinstance(item, list):
                for child in item:
                    validate(child)

        validate(value)

        def check(item):
            if isinstance(item, str) and "\x00" in item:
                raise ValueError
            if isinstance(item, dict):
                for key, child in item.items():
                    check(key)
                    check(child)
            elif isinstance(item, (list, tuple)):
                for child in item:
                    check(child)

        check(value)
        result = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        encoded = result.encode("utf-8")
        if len(encoded) > max_bytes:
            raise ValueError
        return result
    except (ValueError, TypeError, RecursionError, UnicodeError):
        raise PostgresEnvelopeError("Invalid archive JSON") from None


def _payload(value: Any, tenant: str) -> dict[str, Any]:
    try:
        decoded = json.loads(value) if isinstance(value, str) else value
        if not isinstance(decoded, dict):
            raise ValueError
        _canonical(decoded)

        def check(item):
            if isinstance(item, dict):
                if "tenant_id" in item and item["tenant_id"] != tenant:
                    raise ValueError
                for child in item.values():
                    check(child)
            elif isinstance(item, list):
                for child in item:
                    check(child)

        check(decoded)
        return decoded
    except (ValueError, TypeError, RecursionError):
        raise PostgresEnvelopeError("Invalid archive resource payload") from None


def message_identity(message: dict[str, Any]) -> str:
    """Deduplicate the same retained message, not distinct turns of one request."""
    digest = hashlib.sha256(_canonical(message).encode("utf-8")).hexdigest()
    return f"{message['request_id']}:{message['role']}:{digest}"


def _page_cursor(kind: str, resource_id: str) -> str:
    return base64.urlsafe_b64encode(json.dumps([kind, resource_id]).encode()).decode()


def _page_after(value: str) -> tuple[str, str]:
    if value == "":
        return "", ""
    try:
        if not isinstance(value, str) or len(value) > 512:
            raise ValueError
        decoded = json.loads(base64.b64decode(value, altchars=b"-_", validate=True))
        if (
            not isinstance(decoded, list)
            or len(decoded) != 2
            or not all(isinstance(part, str) for part in decoded)
            or decoded[0] not in RECORD_KINDS
            or not decoded[1]
            or len(decoded[1]) > 256
            or "\x00" in decoded[1]
        ):
            raise ValueError
        decoded[1].encode("utf-8")
        return tuple(decoded)
    except (ValueError, TypeError, UnicodeError):
        raise PostgresInvalidCursor("Invalid archive page cursor") from None


class PostgresStore:
    def __init__(self, dsn: str, source_id: str = "gateway") -> None:
        if not isinstance(source_id, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", source_id):
            raise ValueError("Invalid archive source identity")
        self._dsn = dsn
        self.source_id = source_id
        self.pool: AsyncConnectionPool | None = None
        pool_logger = logging.getLogger("psycopg.pool")
        if not any(isinstance(item, SafePoolLogFilter) for item in pool_logger.filters):
            pool_logger.addFilter(SafePoolLogFilter())
        self._lifecycle_lock = asyncio.Lock()

    async def open(self, migrate: bool = False) -> None:
        async with self._lifecycle_lock:
            await self._open(migrate)

    async def _open(self, migrate: bool) -> None:
        if self.pool is not None:
            return

        async def configure(connection):
            await connection.execute("SET statement_timeout TO '10s'")
            await connection.execute("SET lock_timeout TO '5s'")

        pool = AsyncConnectionPool(
            self._dsn,
            open=False,
            min_size=1,
            max_size=4,
            timeout=10,
            kwargs={"autocommit": True, "row_factory": dict_row, "connect_timeout": 5},
            configure=configure,
        )
        try:
            await pool.open(wait=True, timeout=10)
            async with pool.connection() as connection, connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtext('gateway.schema.migration'), 0)"
                )
                row = await (
                    await connection.execute(
                        "SELECT to_regclass('gateway_schema_migrations') AS name"
                    )
                ).fetchone()
                if row["name"] is None:
                    if not migrate:
                        raise PostgresSchemaError("Archive schema requires migration")
                    await connection.execute(
                        "CREATE TABLE gateway_schema_migrations ("
                        "version integer PRIMARY KEY, checksum text NOT NULL, "
                        "applied_at timestamptz NOT NULL DEFAULT clock_timestamp())"
                    )
                versions = await (
                    await connection.execute(
                        "SELECT version, checksum FROM gateway_schema_migrations ORDER BY version"
                    )
                ).fetchall()
                if any(
                    row["version"] != SCHEMA_VERSION or row["checksum"] != SCHEMA_CHECKSUM
                    for row in versions
                ):
                    raise PostgresSchemaError("Archive schema version or checksum is unsupported")
                if not versions:
                    if not migrate:
                        raise PostgresSchemaError("Archive schema requires migration")
                    await connection.execute(SCHEMA_SQL)
                    await connection.execute(
                        "INSERT INTO gateway_schema_migrations(version, checksum) VALUES (%s, %s)",
                        (SCHEMA_VERSION, SCHEMA_CHECKSUM),
                    )
                for table in TABLE_NAMES:
                    row = await (
                        await connection.execute("SELECT to_regclass(%s) AS name", (table,))
                    ).fetchone()
                    if row["name"] is None:
                        raise PostgresSchemaError("Archive schema is incomplete")
            self.pool = pool
        except BaseException:
            await pool.close()
            raise

    async def close(self) -> None:
        async with self._lifecycle_lock:
            pool, self.pool = self.pool, None
            if pool is not None:
                await pool.close()

    def _pool(self) -> AsyncConnectionPool:
        if self.pool is None:
            raise RuntimeError("Archive store is not open")
        return self.pool

    @staticmethod
    def _tenant(tenant: str) -> None:
        if not isinstance(tenant, str) or not TENANT_PATTERN.fullmatch(tenant):
            raise ValueError("Invalid archive tenant")

    @staticmethod
    def _limit(limit: int) -> None:
        if type(limit) is not int or not 1 <= limit <= 1_000:
            raise ValueError("Invalid archive page limit")

    async def _lock(self, connection, tenant):
        await connection.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))", (self.source_id, tenant)
        )

    async def _blocked(self, connection, tenant, kind, resource, position, backfill):
        for table, extra, params in (
            ("gateway_purge_fences", "", ()),
            ("gateway_tombstones", " AND resource_id=%s", (resource,)),
        ):
            row = await (
                await connection.execute(
                    f"SELECT source_ms, source_seq FROM {table} "
                    "WHERE source_id=%s AND tenant_id=%s AND kind=%s" + extra,
                    (self.source_id, tenant, kind, *params),
                )
            ).fetchone()
            if row and (backfill or position <= (int(row["source_ms"]), int(row["source_seq"]))):
                return True
        return False

    async def _record(self, connection, tenant, kind, resource, data, position):
        data = sanitize_record(kind, data)
        await connection.execute(
            "INSERT INTO gateway_records AS saved "
            "(source_id,tenant_id,kind,resource_id,data,search_text,source_ms,source_seq) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(source_id,tenant_id,kind,resource_id) DO UPDATE SET "
            "data=EXCLUDED.data,search_text=EXCLUDED.search_text,source_ms=EXCLUDED.source_ms,"
            "source_seq=EXCLUDED.source_seq,archived_at=clock_timestamp() "
            "WHERE (saved.source_ms,saved.source_seq)<(EXCLUDED.source_ms,EXCLUDED.source_seq)",
            (
                self.source_id,
                tenant,
                kind,
                resource,
                Jsonb(data),
                search_text(kind, data),
                *position,
            ),
        )

    async def _event(self, connection, tenant, kind, resource, event_id, data, position):
        digest = hashlib.sha256(_canonical(data).encode("utf-8")).hexdigest()
        scope = (self.source_id, tenant, kind, resource, event_id)
        inserted = await (
            await connection.execute(
                "INSERT INTO gateway_event_identities "
                "(source_id,tenant_id,kind,resource_id,event_id,payload_hash) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING event_id",
                (*scope, digest),
            )
        ).fetchone()
        if inserted is None:
            prior = await (
                await connection.execute(
                    "SELECT payload_hash FROM gateway_event_identities WHERE source_id=%s "
                    "AND tenant_id=%s AND kind=%s AND resource_id=%s AND event_id=%s",
                    scope,
                )
            ).fetchone()
            if prior["payload_hash"] != digest:
                raise PostgresConflict("Archive event identity conflicts with prior content")
            return False
        await connection.execute(
            "INSERT INTO gateway_events "
            "(source_id,tenant_id,kind,resource_id,event_id,data,source_ms,source_seq) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (*scope, Jsonb(data), *position),
        )
        return True

    async def _delete(self, connection, tenant, kind, resource, position, reason="deleted"):
        await connection.execute(
            "INSERT INTO gateway_tombstones AS saved "
            "(source_id,tenant_id,kind,resource_id,source_ms,source_seq,reason) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(source_id,tenant_id,kind,resource_id) DO UPDATE SET "
            "source_ms=EXCLUDED.source_ms,source_seq=EXCLUDED.source_seq,reason=EXCLUDED.reason,"
            "archived_at=clock_timestamp() "
            "WHERE (saved.source_ms,saved.source_seq)<(EXCLUDED.source_ms,EXCLUDED.source_seq)",
            (self.source_id, tenant, kind, resource, *position, reason),
        )
        for table in ("gateway_records", "gateway_events"):
            await connection.execute(
                f"DELETE FROM {table} WHERE source_id=%s AND tenant_id=%s AND kind=%s "
                "AND resource_id=%s AND (source_ms,source_seq)<=(%s,%s)",
                (self.source_id, tenant, kind, resource, *position),
            )

    async def apply(self, outbox_id: str, envelope: dict[str, Any]) -> bool:
        position = _position(outbox_id)
        if (
            not isinstance(envelope, dict)
            or set(envelope) - {"schema_version", "command", "key", "value", "event_id", "backfill"}
            or type(envelope.get("schema_version")) is not int
            or envelope["schema_version"] != 1
            or envelope.get("command") not in {"SET", "XADD", "RPUSH", "DEL"}
            or type(envelope.get("backfill", False)) is not bool
        ):
            raise PostgresEnvelopeError("Unsupported archive envelope")
        command, key = envelope["command"], envelope.get("key")
        tenant, classified = tenant_from_key(key), classify_key(key)
        if command == "DEL" and classified is None and conversation_id_from_key(key):
            classified = ("conversation", conversation_id_from_key(key), "messages")
        if tenant is None or classified is None:
            raise PostgresEnvelopeError("Archive key is not supported")
        kind, resource, mode = classified
        if command == "DEL" and mode != "messages":
            raise PostgresEnvelopeError("Archive deletion requires a conversation resource")
        data, messages = None, []
        if command == "SET" and mode == "record" and isinstance(envelope.get("value"), str):
            data = _payload(envelope["value"], tenant)
        elif command == "XADD" and mode == "events":
            fields = envelope.get("value")
            if (
                not isinstance(fields, dict)
                or set(fields) != {"payload"}
                or not isinstance(fields["payload"], str)
            ):
                raise PostgresEnvelopeError("Invalid archive stream payload")
            data = _payload(fields["payload"], tenant)
        elif command == "RPUSH" and mode == "messages":
            values = envelope.get("value")
            if not isinstance(values, list) or not values or len(values) > 1_000:
                raise PostgresEnvelopeError("Invalid archive messages")
            for raw in values:
                message = _payload(raw, tenant)
                if (
                    not isinstance(raw, str)
                    or message.get("role") not in {"user", "assistant"}
                    or not isinstance(message.get("request_id"), str)
                    or not 1 <= len(message["request_id"]) <= 128
                ):
                    raise PostgresEnvelopeError("Invalid archive message identity")
                messages.append((message_identity(message), message))
        elif command != "DEL":
            raise PostgresEnvelopeError("Archive command does not match its resource")
        event_id = envelope.get("event_id") or outbox_id
        if command == "XADD":
            _position(event_id)
        digest = hashlib.sha256(
            _canonical(envelope, max_bytes=_MAX_ENVELOPE_BYTES).encode("utf-8")
        ).hexdigest()
        async with self._pool().connection() as connection, connection.transaction():
            await self._lock(connection, tenant)
            inserted = await (
                await connection.execute(
                    "INSERT INTO gateway_applied_operations "
                    "(source_id,tenant_id,source_ms,source_seq,envelope_hash) "
                    "VALUES (%s,%s,%s,%s,%s) "
                    "ON CONFLICT DO NOTHING RETURNING envelope_hash",
                    (self.source_id, tenant, *position, digest),
                )
            ).fetchone()
            if inserted is None:
                prior = await (
                    await connection.execute(
                        "SELECT envelope_hash,tenant_id FROM gateway_applied_operations "
                        "WHERE source_id=%s AND source_ms=%s AND source_seq=%s",
                        (self.source_id, *position),
                    )
                ).fetchone()
                if prior["envelope_hash"] != digest or prior["tenant_id"] != tenant:
                    raise PostgresConflict(
                        "Archive operation identity conflicts with prior content"
                    )
                return False
            if await self._blocked(
                connection, tenant, kind, resource, position, envelope.get("backfill", False)
            ):
                return True
            if command == "DEL":
                await self._delete(connection, tenant, kind, resource, position)
            elif command == "SET":
                await self._record(connection, tenant, kind, resource, data, position)
            elif command == "XADD":
                await self._event(connection, tenant, kind, resource, event_id, data, position)
            else:
                changed = False
                for identity, message in messages:
                    changed = (
                        await self._event(
                            connection, tenant, kind, resource, identity, message, position
                        )
                        or changed
                    )
                if changed:
                    await self._record(
                        connection, tenant, kind, resource, {"conversation_id": resource}, position
                    )
        return True

    async def is_suppressed(
        self, tenant: str, kind: str, resource_id: str, backfill: bool = True
    ) -> bool:
        self._tenant(tenant)
        if kind not in RECORD_KINDS | EVENT_KINDS or type(backfill) is not bool:
            raise ValueError("Invalid archive suppression query")
        async with self._pool().connection() as connection:
            return await self._blocked(connection, tenant, kind, resource_id, (0, 0), backfill)

    async def event_matches(
        self, tenant: str, kind: str, resource_id: str, event_id: str, data: dict
    ) -> bool:
        self._tenant(tenant)
        if kind not in EVENT_KINDS:
            raise ValueError("Unsupported archive event kind")
        data = _payload(data, tenant)
        async with self._pool().connection() as connection:
            row = await (
                await connection.execute(
                    "SELECT data FROM gateway_events WHERE source_id=%s AND tenant_id=%s "
                    "AND kind=%s AND resource_id=%s AND event_id=%s",
                    (self.source_id, tenant, kind, resource_id, event_id),
                )
            ).fetchone()
        return row is not None and row["data"] == data

    @staticmethod
    def _record_view(row):
        return {
            "cursor": _page_cursor(row["kind"], row["resource_id"]),
            "kind": row["kind"],
            "resource_id": row["resource_id"],
            "data": row["data"],
            "source_cursor": _source_cursor(row),
            "archived_at": row["archived_at"].isoformat(),
        }

    async def get_record(self, tenant: str, kind: str, resource_id: str) -> dict | None:
        self._tenant(tenant)
        _canonical(resource_id)
        if kind not in RECORD_KINDS:
            raise ValueError("Unsupported archive record kind")
        async with self._pool().connection() as connection:
            row = await (
                await connection.execute(
                    "SELECT * FROM gateway_records WHERE source_id=%s AND tenant_id=%s "
                    "AND kind=%s AND resource_id=%s",
                    (self.source_id, tenant, kind, resource_id),
                )
            ).fetchone()
        return self._record_view(row) if row else None

    async def list_records(
        self,
        tenant: str,
        kind: str = "",
        after: str = "",
        limit: int = 50,
        authorized_resources=None,
    ):
        return await self._records(
            tenant, kind, after, limit, authorized_resources=authorized_resources
        )

    async def search(
        self,
        tenant: str,
        query: str,
        kind: str = "",
        after: str = "",
        limit: int = 50,
        authorized_resources=None,
    ):
        if not isinstance(query, str) or not query.strip() or len(query) > 512:
            raise ValueError("Invalid archive search query")
        _canonical(query)
        return await self._records(tenant, kind, after, limit, query, authorized_resources)

    async def _records(self, tenant, kind, after, limit, query=None, authorized_resources=None):
        self._tenant(tenant)
        self._limit(limit)
        if kind and kind not in RECORD_KINDS:
            raise ValueError("Unsupported archive record kind")
        last_kind, last_id = _page_after(after)
        sql = (
            "SELECT * FROM gateway_records WHERE source_id=%s AND tenant_id=%s "
            "AND (kind,resource_id)>(%s,%s)"
        )
        params = [self.source_id, tenant, last_kind, last_id]
        if authorized_resources is not None:
            sql += """ AND ((CASE kind WHEN 'skill_revision' THEN 'skill'
                WHEN 'planning_revision' THEN 'planning_session' WHEN 'tool_receipt' THEN 'goal'
                ELSE kind END) || ':' || (CASE WHEN kind IN
                ('skill_revision','planning_revision','tool_receipt')
                THEN split_part(resource_id,':',1) ELSE resource_id END)) = ANY(%s::text[])"""
            params.append(authorized_resources)
        if kind:
            sql += " AND kind=%s"
            params.append(kind)
        if query is not None:
            sql += (
                " AND (search_vector @@ plainto_tsquery('simple', %s) "
                "OR strpos(lower(search_text),lower(%s))>0)"
            )
            params.extend([query, query])
        sql += " ORDER BY kind,resource_id LIMIT %s"
        params.append(limit)
        async with self._pool().connection() as connection:
            rows = await (await connection.execute(sql, params)).fetchall()
        return [self._record_view(row) for row in rows]

    async def events(
        self,
        tenant: str,
        kind: str = "",
        resource_id: str = "",
        after: int = 0,
        limit: int = 100,
        authorized_resources=None,
    ):
        self._tenant(tenant)
        self._limit(limit)
        if type(after) is not int or after < 0:
            raise PostgresInvalidCursor("Invalid archive event cursor")
        if kind and kind not in EVENT_KINDS:
            raise ValueError("Unsupported archive event kind")
        sql = "SELECT * FROM gateway_events WHERE source_id=%s AND tenant_id=%s AND cursor>%s"
        params = [self.source_id, tenant, after]
        if authorized_resources is not None:
            sql += (
                " AND ((CASE kind WHEN 'planning' THEN 'planning_session' ELSE kind END) "
                "|| ':' || resource_id) = ANY(%s::text[])"
            )
            params.append(authorized_resources)
        if kind:
            sql += " AND kind=%s"
            params.append(kind)
        if resource_id:
            sql += " AND resource_id=%s"
            params.append(resource_id)
        sql += " ORDER BY cursor LIMIT %s"
        params.append(limit)
        async with self._pool().connection() as connection:
            rows = await (await connection.execute(sql, params)).fetchall()
        return [
            {
                "cursor": row["cursor"],
                "kind": row["kind"],
                "resource_id": row["resource_id"],
                "event_id": row["event_id"],
                "data": row["data"],
                "source_cursor": _source_cursor(row),
                "archived_at": row["archived_at"].isoformat(),
            }
            for row in rows
        ]

    async def purge(self, tenant: str, kind: str, before_cursor: str) -> dict:
        self._tenant(tenant)
        if kind not in RECORD_KINDS | EVENT_KINDS:
            raise ValueError("Unsupported archive purge kind")
        position = _position(before_cursor)
        async with self._pool().connection() as connection, connection.transaction():
            await self._lock(connection, tenant)
            latest = await (
                await connection.execute(
                    "SELECT source_ms,source_seq FROM gateway_applied_operations "
                    "WHERE source_id=%s AND tenant_id=%s "
                    "ORDER BY source_ms DESC,source_seq DESC LIMIT 1",
                    (self.source_id, tenant),
                )
            ).fetchone()
            if latest is None or position > (int(latest["source_ms"]), int(latest["source_seq"])):
                raise PostgresInvalidCursor("Purge cutoff exceeds observed tenant history")
            event_count = await (
                await connection.execute(
                    "SELECT count(*) AS count FROM gateway_events WHERE source_id=%s "
                    "AND tenant_id=%s AND kind=%s AND (source_ms,source_seq)<=(%s,%s)",
                    (self.source_id, tenant, kind, *position),
                )
            ).fetchone()
            rows = await (
                await connection.execute(
                    "SELECT resource_id FROM gateway_records WHERE source_id=%s AND tenant_id=%s "
                    "AND kind=%s AND (source_ms,source_seq)<=(%s,%s)",
                    (self.source_id, tenant, kind, *position),
                )
            ).fetchall()
            for row in rows:
                await self._delete(connection, tenant, kind, row["resource_id"], position, "purged")
            await connection.execute(
                "DELETE FROM gateway_events WHERE source_id=%s AND tenant_id=%s AND kind=%s "
                "AND (source_ms,source_seq)<=(%s,%s)",
                (self.source_id, tenant, kind, *position),
            )
            await connection.execute(
                "INSERT INTO gateway_purge_fences AS saved "
                "(source_id,tenant_id,kind,source_ms,source_seq) VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT(source_id,tenant_id,kind) DO UPDATE SET "
                "source_ms=EXCLUDED.source_ms,source_seq=EXCLUDED.source_seq "
                "WHERE (saved.source_ms,saved.source_seq)<(EXCLUDED.source_ms,EXCLUDED.source_seq)",
                (self.source_id, tenant, kind, *position),
            )
        return {
            "kind": kind,
            "before_cursor": before_cursor,
            "records": len(rows),
            "events": event_count["count"],
        }

    async def status(self, tenant: str | None = None) -> dict:
        if tenant is not None:
            self._tenant(tenant)
        scope = "source_id=%s" + (" AND tenant_id=%s" if tenant is not None else "")
        params = (self.source_id, tenant) if tenant is not None else (self.source_id,)
        counts = {}
        async with self._pool().connection() as connection:
            for label, table in (
                ("records", "gateway_records"),
                ("events", "gateway_events"),
                ("applied_operations", "gateway_applied_operations"),
                ("tombstones", "gateway_tombstones"),
            ):
                row = await (
                    await connection.execute(
                        f"SELECT count(*) AS count FROM {table} WHERE {scope}", params
                    )
                ).fetchone()
                counts[label] = row["count"]
            latest = await (
                await connection.execute(
                    f"SELECT source_ms,source_seq FROM gateway_applied_operations WHERE {scope} "
                    "ORDER BY source_ms DESC,source_seq DESC LIMIT 1",
                    params,
                )
            ).fetchone()
            migration = await (
                await connection.execute(
                    "SELECT report_id,saved_at,report->>'status' AS status "
                    "FROM gateway_migration_reports WHERE source_id=%s "
                    "ORDER BY report_id DESC LIMIT 1",
                    (self.source_id,),
                )
            ).fetchone()
        return {
            "source_id": self.source_id,
            "schema_version": SCHEMA_VERSION,
            **counts,
            "latest_cursor": _source_cursor(latest) if latest else None,
            "migration": {
                "report_id": migration["report_id"],
                "saved_at": migration["saved_at"].isoformat(),
                "status": migration["status"],
            }
            if migration
            else None,
        }

    async def save_migration(self, report: dict) -> None:
        if (
            not isinstance(report, dict)
            or report.get("source_id", self.source_id) != self.source_id
        ):
            raise ValueError("Invalid migration report")
        _canonical(report)
        async with self._pool().connection() as connection:
            await connection.execute(
                "INSERT INTO gateway_migration_reports(source_id,report) VALUES (%s,%s)",
                (self.source_id, Jsonb(report)),
            )

    async def latest_migration(self) -> dict | None:
        async with self._pool().connection() as connection:
            row = await (
                await connection.execute(
                    "SELECT report FROM gateway_migration_reports WHERE source_id=%s "
                    "ORDER BY report_id DESC LIMIT 1",
                    (self.source_id,),
                )
            ).fetchone()
        return row["report"] if row else None
