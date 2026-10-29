"""Tenant-scoped, tamper-evident audit-chain projection and checkpoints."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from redis.exceptions import WatchError

from app.core.security import TENANT_PATTERN

SCHEMA_VERSION = 1
GENESIS_HASH = "0" * 64
MAX_VERIFY_ERRORS = 100
logger = logging.getLogger(__name__)


def canonical_json(value: Any) -> str:
    """Return the fixed canonical representation used by the chain contract."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _cursor_tuple(cursor: str) -> tuple[int, int]:
    try:
        milliseconds, sequence = cursor.split("-", 1)
        return int(milliseconds), int(sequence)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("Invalid Redis stream cursor") from exc


def _occurred_at(cursor: str) -> str:
    milliseconds, _ = _cursor_tuple(cursor)
    return datetime.fromtimestamp(milliseconds / 1_000, UTC).isoformat()


def _digest(previous_hash: str, body: dict[str, Any]) -> str:
    material = previous_hash.encode("ascii") + canonical_json(body).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _decode_record(fields: dict[str, str]) -> dict[str, Any]:
    raw = fields.get("payload")
    if not isinstance(raw, str):
        raise ValueError("Audit-chain entry has no payload")
    record = json.loads(raw)
    if not isinstance(record, dict):
        raise ValueError("Audit-chain entry is not an object")
    return record


def verify_records(
    entries: Iterable[tuple[str, dict[str, str]]],
    head: dict[str, Any] | None,
    tenant_id: str,
) -> dict[str, Any]:
    """Verify an ordered chain snapshot without exposing source audit content."""
    errors: list[dict[str, Any]] = []
    previous_hash = GENESIS_HASH
    expected_sequence = 1
    last_source_cursor: str | None = None
    coverage_start_cursor: str | None = None

    def fail(code: str, sequence: int | None = None) -> None:
        if len(errors) < MAX_VERIFY_ERRORS:
            item: dict[str, Any] = {"code": code}
            if sequence is not None:
                item["sequence"] = sequence
            errors.append(item)

    for stream_cursor, fields in entries:
        sequence = expected_sequence
        try:
            record = _decode_record(fields)
            sequence = int(record.get("sequence"))
        except (ValueError, TypeError, json.JSONDecodeError):
            fail("invalid_entry", expected_sequence)
            expected_sequence += 1
            continue
        if stream_cursor != f"{sequence}-0":
            fail("stream_sequence_mismatch", sequence)
        if sequence != expected_sequence:
            fail("missing_or_duplicate_sequence", sequence)
        if record.get("tenant_id") != tenant_id:
            fail("tenant_mismatch", sequence)
        if record.get("schema_version") != SCHEMA_VERSION:
            fail("schema_version_mismatch", sequence)
        if record.get("prev_hash") != previous_hash:
            fail("previous_hash_mismatch", sequence)
        body = {key: value for key, value in record.items() if key not in {"prev_hash", "hash"}}
        try:
            calculated = _digest(previous_hash, body)
        except (TypeError, ValueError, UnicodeError):
            fail("invalid_canonical_event", sequence)
            calculated = ""
        if not hmac.compare_digest(str(record.get("hash", "")), calculated):
            fail("hash_mismatch", sequence)
        source_cursor = record.get("source_cursor")
        try:
            source_tuple = _cursor_tuple(source_cursor)
            if last_source_cursor and source_tuple <= _cursor_tuple(last_source_cursor):
                fail("source_order_mismatch", sequence)
        except ValueError:
            fail("invalid_source_cursor", sequence)
        if coverage_start_cursor is None and isinstance(source_cursor, str):
            coverage_start_cursor = source_cursor
        if isinstance(source_cursor, str):
            last_source_cursor = source_cursor
        previous_hash = str(record.get("hash", ""))
        expected_sequence = max(expected_sequence + 1, sequence + 1)

    count = expected_sequence - 1
    expected_head = {
        "schema_version": SCHEMA_VERSION,
        "tenant_id": tenant_id,
        "sequence": count,
        "hash": previous_hash,
        "source_cursor": last_source_cursor,
        "coverage_start_cursor": coverage_start_cursor,
    }
    if count == 0 and head is None:
        expected_head = None
    if head != expected_head:
        fail("head_mismatch", count)
    return {
        "valid": not errors,
        "tenant_id": tenant_id,
        "sequence": count,
        "hash": previous_hash,
        "source_cursor": last_source_cursor,
        "coverage_start_cursor": coverage_start_cursor,
        "errors": errors,
    }


def sign_checkpoint(checkpoint: dict[str, Any], secret: str) -> str:
    if len(secret.encode("utf-8")) < 32:
        raise ValueError("Audit checkpoint signing key must contain at least 32 bytes")
    return hmac.new(
        secret.encode("utf-8"), canonical_json(checkpoint).encode("utf-8"), hashlib.sha256
    ).hexdigest()


def verify_checkpoint(
    checkpoint: dict[str, Any],
    secret: str,
    *,
    tenant_id: str | None = None,
    key_id: str | None = None,
) -> bool:
    expected_fields = {
        "schema_version",
        "tenant_id",
        "sequence",
        "hash",
        "source_cursor",
        "coverage_start_cursor",
        "exported_at",
        "algorithm",
        "key_id",
        "signature",
    }
    if set(checkpoint) != expected_fields:
        return False
    if (
        checkpoint.get("schema_version") != SCHEMA_VERSION
        or checkpoint.get("algorithm") != "HMAC-SHA256"
        or type(checkpoint.get("sequence")) is not int
        or checkpoint["sequence"] < 0
        or tenant_id is not None
        and checkpoint.get("tenant_id") != tenant_id
        or key_id is not None
        and checkpoint.get("key_id") != key_id
    ):
        return False
    digest = checkpoint.get("hash")
    if not isinstance(digest, str) or len(digest) != 64:
        return False
    try:
        int(digest, 16)
        exported_at = datetime.fromisoformat(str(checkpoint["exported_at"]))
        if exported_at.utcoffset() is None:
            return False
        if checkpoint["source_cursor"] is not None:
            _cursor_tuple(checkpoint["source_cursor"])
        if checkpoint["coverage_start_cursor"] is not None:
            _cursor_tuple(checkpoint["coverage_start_cursor"])
    except (TypeError, ValueError):
        return False
    empty = checkpoint["sequence"] == 0
    if empty and not (
        checkpoint["source_cursor"] is None
        and checkpoint["coverage_start_cursor"] is None
        and digest == GENESIS_HASH
    ):
        return False
    if not empty and (
        checkpoint["source_cursor"] is None
        or checkpoint["coverage_start_cursor"] is None
        or digest == GENESIS_HASH
    ):
        return False
    signature = checkpoint.get("signature")
    if not isinstance(signature, str):
        return False
    unsigned = {key: value for key, value in checkpoint.items() if key != "signature"}
    try:
        expected = sign_checkpoint(unsigned, secret)
    except (TypeError, ValueError):
        return False
    return hmac.compare_digest(signature, expected)


class AuditChainStore:
    """Project the legacy-compatible audit stream into one serialized hash chain."""

    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self.client = runtime._client

    def source_key(self, tenant_id: str) -> str:
        return self.runtime._tenant_key(tenant_id, "audit")

    def chain_key(self, tenant_id: str) -> str:
        return self.runtime._tenant_key(tenant_id, "audit-chain:v1")

    def head_key(self, tenant_id: str) -> str:
        return self.runtime._tenant_key(tenant_id, "audit-chain:v1:head")

    async def _head(self, tenant_id: str) -> dict[str, Any] | None:
        raw = await self.client.get(self.head_key(tenant_id))
        if raw is None:
            return None
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise RuntimeError("Audit-chain head is corrupt")
        return value

    async def _append_source(
        self, tenant_id: str, source_cursor: str, fields: dict[str, str]
    ) -> None:
        source_payload = fields.get("payload")
        try:
            event = json.loads(source_payload or "")
        except json.JSONDecodeError as exc:
            raise RuntimeError("Source audit entry is corrupt") from exc
        if not isinstance(event, dict):
            raise RuntimeError("Source audit entry is not an object")
        embedded_tenant = event.get("tenant_id")
        if embedded_tenant is not None and embedded_tenant != tenant_id:
            raise RuntimeError("Source audit tenant mismatch")

        head_key, chain_key = self.head_key(tenant_id), self.chain_key(tenant_id)
        for _ in range(32):
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(head_key, chain_key)
                    raw_head = await pipe.get(head_key)
                    head = json.loads(raw_head) if raw_head else None
                    if head is not None and not isinstance(head, dict):
                        raise RuntimeError("Audit-chain head is corrupt")
                    if head and _cursor_tuple(str(head["source_cursor"])) >= _cursor_tuple(
                        source_cursor
                    ):
                        return
                    sequence = int(head["sequence"]) + 1 if head else 1
                    previous_hash = str(head["hash"]) if head else GENESIS_HASH
                    coverage_start = str(head["coverage_start_cursor"]) if head else source_cursor
                    tail = await pipe.xrevrange(chain_key, count=1)
                    if head:
                        if not tail or tail[0][0] != f"{sequence - 1}-0":
                            # A concurrent writer can change the chain between the
                            # separately observed head and tail. Retry under WATCH;
                            # a stable mismatch eventually fails closed below.
                            await pipe.unwatch()
                            continue
                        tail_record = _decode_record(tail[0][1])
                        if tail_record.get("hash") != previous_hash:
                            await pipe.unwatch()
                            continue
                    elif tail:
                        await pipe.unwatch()
                        continue
                    body = {
                        "schema_version": SCHEMA_VERSION,
                        "sequence": sequence,
                        "tenant_id": tenant_id,
                        "source_cursor": source_cursor,
                        "occurred_at": _occurred_at(source_cursor),
                        "event": event,
                    }
                    digest = _digest(previous_hash, body)
                    record = body | {"prev_hash": previous_hash, "hash": digest}
                    new_head = {
                        "schema_version": SCHEMA_VERSION,
                        "tenant_id": tenant_id,
                        "sequence": sequence,
                        "hash": digest,
                        "source_cursor": source_cursor,
                        "coverage_start_cursor": coverage_start,
                    }
                    pipe.multi()
                    pipe.xadd(chain_key, {"payload": canonical_json(record)}, id=f"{sequence}-0")
                    pipe.set(head_key, canonical_json(new_head))
                    await pipe.execute()
                    return
                except WatchError:
                    continue
        raise RuntimeError("Audit-chain append contention exceeded retry limit")

    async def sync(
        self, tenant_id: str, *, through_cursor: str | None = None
    ) -> dict[str, Any] | None:
        while True:
            head = await self._head(tenant_id)
            start = f"({head['source_cursor']}" if head else "-"
            rows = await self.client.xrange(
                self.source_key(tenant_id), min=start, max=through_cursor or "+", count=256
            )
            if not rows:
                return await self._head(tenant_id)
            for cursor, fields in rows:
                await self._append_source(tenant_id, cursor, fields)
            if through_cursor and _cursor_tuple(rows[-1][0]) >= _cursor_tuple(through_cursor):
                return await self._head(tenant_id)

    async def verify(self, tenant_id: str) -> dict[str, Any]:
        await self.sync(tenant_id)
        rows = await self.client.xrange(self.chain_key(tenant_id))
        report = verify_records(rows, await self._head(tenant_id), tenant_id)
        source_rows = dict(await self.client.xrange(self.source_key(tenant_id)))
        source_errors: list[dict[str, Any]] = []
        for _stream_cursor, fields in rows:
            try:
                record = _decode_record(fields)
                sequence = int(record["sequence"])
                source_cursor = str(record["source_cursor"])
                source_fields = source_rows.get(source_cursor)
                if source_fields is None:
                    code = "source_entry_missing"
                else:
                    source_event = json.loads(source_fields.get("payload", ""))
                    if canonical_json(source_event) == canonical_json(record.get("event")):
                        continue
                    code = "source_payload_mismatch"
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                sequence = None
                code = "source_entry_invalid"
            item: dict[str, Any] = {"code": code}
            if sequence is not None:
                item["sequence"] = sequence
            source_errors.append(item)
            if len(source_errors) >= MAX_VERIFY_ERRORS:
                break
        if source_errors:
            report["valid"] = False
            remaining = max(0, MAX_VERIFY_ERRORS - len(report["errors"]))
            report["errors"].extend(source_errors[:remaining])
        return report

    async def export_checkpoint(
        self, tenant_id: str, *, secret: str, key_id: str
    ) -> dict[str, Any]:
        report = await self.verify(tenant_id)
        if not report["valid"]:
            raise RuntimeError("Audit chain verification failed")
        checkpoint = {
            "schema_version": SCHEMA_VERSION,
            "tenant_id": tenant_id,
            "sequence": report["sequence"],
            "hash": report["hash"],
            "source_cursor": report["source_cursor"],
            "coverage_start_cursor": report["coverage_start_cursor"],
            "exported_at": datetime.now(UTC).isoformat(),
            "algorithm": "HMAC-SHA256",
            "key_id": key_id,
        }
        return checkpoint | {"signature": sign_checkpoint(checkpoint, secret)}


async def project_audit_chains_once(runtime: Any) -> tuple[int, int]:
    """Discover and synchronize all valid tenant audit streams once."""
    completed = failures = 0
    async for key in runtime._client.scan_iter(match="tenant:*:audit", _type="stream"):
        if not isinstance(key, str) or not key.startswith("tenant:") or not key.endswith(":audit"):
            continue
        tenant_id = key[len("tenant:") : -len(":audit")]
        if not TENANT_PATTERN.fullmatch(tenant_id):
            continue
        try:
            await AuditChainStore(runtime).sync(tenant_id)
            completed += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            failures += 1
            logger.exception("Audit-chain projection failed", extra={"tenant_id": tenant_id})
    return completed, failures


async def run_audit_chain_projector(runtime: Any, poll_seconds: float) -> None:
    """Continuously project compatibility audit producers; verify/export also catch up."""
    while True:
        try:
            await project_audit_chains_once(runtime)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Redis outages are recoverable; one failed scan must not permanently
            # disable the projector after execution authority returns.
            logger.exception("Audit-chain discovery scan failed")
        await asyncio.sleep(poll_seconds)
