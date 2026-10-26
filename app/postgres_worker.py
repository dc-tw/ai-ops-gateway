"""Postgres history projector and guarded migration operations.

Usage: python -m app.postgres_worker migrate|project|backfill|reconcile|status|compact
No command drops a schema, changes execution authority, or repairs poison data
by silently acknowledging it. Credentials and payloads are never logged.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import socket
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import uuid4

from redis.exceptions import ResponseError

from app.core.config import get_settings
from app.core.durable_outbox import maintenance_key, outbox_key
from app.core.durable_types import classify_key, sanitize_record, tenant_from_key
from app.core.postgres_store import PostgresStore, message_identity
from app.core.redis_runtime import RedisRuntime

GROUP = "postgres-history-v1"
logger = logging.getLogger(__name__)


async def ensure_group(client, stream):
    try:
        await client.xgroup_create(stream, GROUP, id="0-0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def project_batch(runtime, store, *, consumer="projector", lease_ms=5_000, count=100):
    client, stream = runtime._client, outbox_key(store.source_id)
    await ensure_group(client, stream)
    cursors = getattr(runtime, "_postgres_claim_cursors", None)
    if cursors is None:
        cursors = runtime._postgres_claim_cursors = {}
    owner = (store.source_id, consumer)
    pending = await client.xautoclaim(
        stream,
        GROUP,
        consumer,
        min_idle_time=lease_ms,
        start_id=cursors.get(owner, "0-0"),
        count=count,
    )
    cursors[owner] = pending[0]
    rows = list(pending[1])
    new = await client.xreadgroup(GROUP, consumer, {stream: ">"}, count=count)
    rows.extend(row for _, items in new for row in items)
    counts = {"applied": 0, "duplicates": 0, "failed": 0}
    for event_id, fields in rows:
        try:
            envelope = json.loads(fields["payload"])
            changed = await store.apply(event_id, envelope)
        except (ValueError, KeyError, TypeError):
            # Keep the original pending (not ACKed) and record metadata only.
            # Other valid records can advance; cutover still fails until repaired.
            marker = f"{stream}:invalid:{event_id}"
            if await client.set(marker, "1", nx=True):
                await client.xadd(
                    f"{stream}:errors",
                    {"event_id": event_id, "error": "invalid_envelope"},
                )
            counts["failed"] += 1
            continue
        except Exception:
            counts["failed"] += 1
            logger.warning("Postgres projection unavailable; pending records retained")
            continue
        await client.xack(stream, GROUP, event_id)
        counts["applied" if changed else "duplicates"] += 1
    return counts


async def drain(runtime, store, *, deadline_seconds=60):
    client, stream = runtime._client, outbox_key(store.source_id)
    await ensure_group(client, stream)
    barrier = (await client.xinfo_stream(stream))["last-generated-id"]
    deadline = time.monotonic() + deadline_seconds
    consumer = "migration-" + uuid4().hex
    while True:
        await project_batch(runtime, store, consumer=consumer, lease_ms=0)
        pending = await client.xpending(stream, GROUP)
        groups = await client.xinfo_groups(stream)
        current = next(group for group in groups if group["name"] == GROUP)
        cursor = current["last-delivered-id"]
        if pending["pending"] == 0 and _position(cursor) >= _position(barrier):
            return barrier
        if time.monotonic() >= deadline:
            raise RuntimeError("Projection has unresolved pending records; cutover refused")
        await asyncio.sleep(0.05)


def _position(cursor):
    return tuple(int(part) for part in cursor.split("-"))


@asynccontextmanager
async def maintenance(runtime, source_id):
    client, key, owner = runtime._client, maintenance_key(source_id), uuid4().hex
    if not await client.set(key, owner, nx=True, px=60_000):
        raise RuntimeError("Another durable maintenance operation is active")
    lost = asyncio.Event()

    async def renew():
        while True:
            await asyncio.sleep(15)
            try:
                renewed = await client.eval(
                    "if redis.call('GET',KEYS[1]) == ARGV[1] then "
                    "return redis.call('PEXPIRE',KEYS[1],60000) end return 0",
                    1,
                    key,
                    owner,
                )
                if not renewed:
                    lost.set()
                    return
            except Exception:
                lost.set()
                return

    async def check():
        if lost.is_set() or await client.get(key) != owner:
            raise RuntimeError("Maintenance lease lost; verification refused")

    task = asyncio.create_task(renew())
    try:
        yield check
        await check()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await client.eval(
            "if redis.call('GET',KEYS[1]) == ARGV[1] then "
            "return redis.call('DEL',KEYS[1]) end return 0",
            1,
            key,
            owner,
        )


async def snapshots(runtime):
    """Only retained, typed data: never checkpoints, raw cache or credentials."""
    client = runtime._client
    async for key in client.scan_iter(match="tenant:*", count=100):
        match = classify_key(key)
        if not match:
            continue
        _, _, mode = match
        kind = await client.type(key)
        if kind == "none":  # Cache expiry during maintenance is not a deletion.
            continue
        expected = {"record": "string", "events": "stream", "messages": "list"}[mode]
        if kind != expected:
            raise ValueError("Archival source type mismatch")
        if mode == "record":
            value = await client.get(key)
            if value is not None:
                yield {"schema_version": 1, "command": "SET", "key": key, "value": value}
        elif mode == "messages":
            values = await client.lrange(key, 0, -1)
            for value in values:
                # One message per envelope keeps retained long conversations
                # within the same bounded per-operation JSON capacity.
                yield {"schema_version": 1, "command": "RPUSH", "key": key, "value": [value]}
        else:
            after = "-"
            while rows := await client.xrange(key, min=after, max="+", count=100):
                for event_id, fields in rows:
                    yield {
                        "schema_version": 1,
                        "command": "XADD",
                        "key": key,
                        "value": fields,
                        "event_id": event_id,
                    }
                after = "(" + rows[-1][0]


async def backfill(runtime, store):
    async with maintenance(runtime, store.source_id) as check:
        await store.save_migration({"report_id": uuid4().hex, "status": "backfilling"})
        await drain(runtime, store)
        report = {
            "report_id": uuid4().hex,
            "status": "backfilled",
            "snapshots": 0,
            "coverage": "retained Redis resources only; prior TTL/trim history is unrecoverable",
        }
        # New real source positions, not fabricated IDs. Competing projectors may
        # process these; order fencing and identity ledgers make replay harmless.
        async for envelope in snapshots(runtime):
            await check()
            envelope["backfill"] = True
            await runtime._client.xadd(
                outbox_key(store.source_id), {"payload": json.dumps(envelope, ensure_ascii=False)}
            )
            report["snapshots"] += 1
        report["source_cursor"] = await drain(runtime, store)
        await check()
        report["finished_at"] = datetime.now(UTC).isoformat()
        await store.save_migration(report)
        return report


async def reconcile(runtime, store):
    async with maintenance(runtime, store.source_id) as check:
        await store.save_migration({"report_id": uuid4().hex, "status": "verifying"})
        barrier = await drain(runtime, store)
        report = {
            "report_id": uuid4().hex,
            "status": "verifying",
            "source_cursor": barrier,
            "checked": 0,
            "excluded": 0,
            "mismatches": 0,
            "coverage": "retained source subset; archived extras and cache expiry are expected",
        }
        async for envelope in snapshots(runtime):
            await check()
            kind, resource, mode = classify_key(envelope["key"])
            tenant = tenant_from_key(envelope["key"])
            if await store.is_suppressed(tenant, kind, resource):
                report["excluded"] += 1
                continue
            if mode == "record":
                expected = sanitize_record(kind, json.loads(envelope["value"]))
                actual = await store.get_record(tenant, kind, resource)
                matched = actual is not None and actual["data"] == expected
            elif mode == "events":
                payload = json.loads(envelope["value"]["payload"])
                matched = await store.event_matches(
                    tenant, kind, resource, envelope["event_id"], payload
                )
            else:
                matched = True
                for raw in envelope["value"]:
                    payload = json.loads(raw)
                    identity = message_identity(payload)
                    matched = matched and await store.event_matches(
                        tenant, kind, resource, identity, payload
                    )
            report["checked"] += 1
            report["mismatches"] += int(not matched)
        await check()
        report["status"] = "verified" if report["mismatches"] == 0 else "diverged"
        report["finished_at"] = datetime.now(UTC).isoformat()
        await store.save_migration(report)
        if report["mismatches"]:
            raise RuntimeError("History reconciliation diverged; read cutover refused")
        return report


async def compact(runtime, source_id):
    """Prune only delivered entries older than the earliest pending position."""
    client, stream = runtime._client, outbox_key(source_id)
    await ensure_group(client, stream)
    removed, boundary = await client.eval(
        """
local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
if #groups ~= 1 then return redis.error_reply('Unexpected archive consumer groups') end
local name, delivered
for i=1,#groups[1],2 do
  if groups[1][i] == 'name' then name = groups[1][i+1] end
  if groups[1][i] == 'last-delivered-id' then delivered = groups[1][i+1] end
end
if name ~= ARGV[1] then return redis.error_reply('Unexpected archive consumer group') end
local pending = redis.call('XPENDING', KEYS[1], ARGV[1])
local boundary = pending[1] > 0 and pending[2] or delivered
return {redis.call('XTRIM', KEYS[1], 'MINID', boundary), boundary}
""",
        1,
        stream,
        GROUP,
    )
    return {"removed": removed, "retained_from": boundary}


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=["migrate", "project", "backfill", "reconcile", "status", "compact"]
    )
    args = parser.parse_args()
    settings = get_settings()
    if not settings.postgres_dsn:
        parser.error("POSTGRES_DSN is required")
    store = PostgresStore(settings.postgres_dsn, settings.postgres_source_id)
    runtime = RedisRuntime(settings.redis_url)
    try:
        await store.open(migrate=args.command == "migrate")
        if args.command == "migrate":
            print(json.dumps({"status": "schema_ready"}))
        elif args.command == "project":
            consumer = socket.gethostname() + "-" + uuid4().hex
            while True:
                await project_batch(
                    runtime,
                    store,
                    consumer=consumer,
                    lease_ms=settings.postgres_outbox_lease_ms,
                )
                await asyncio.sleep(settings.postgres_projector_poll_seconds)
        else:
            result = (
                await backfill(runtime, store)
                if args.command == "backfill"
                else await reconcile(runtime, store)
                if args.command == "reconcile"
                else await compact(runtime, store.source_id)
                if args.command == "compact"
                else await store.status()
            )
            print(json.dumps(result, default=str, ensure_ascii=False))
    finally:
        await runtime.close()
        await store.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception:
        # A CLI traceback can contain libpq DSNs or private source payloads.
        logger.error("Postgres operation failed; inspect service health and migration status")
        raise SystemExit(1) from None
