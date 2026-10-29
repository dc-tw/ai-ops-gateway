"""Pure and real-Redis verification for tenant audit hash chains."""

from __future__ import annotations

import asyncio
import json
import os
from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI

import app.core.audit_chain as audit_chain_module
from app.audit_chain import router
from app.core.audit_chain import (
    GENESIS_HASH,
    AuditChainStore,
    canonical_json,
    project_audit_chains_once,
    run_audit_chain_projector,
    sign_checkpoint,
    verify_checkpoint,
    verify_records,
)
from app.core.config import Settings
from app.core.redis_runtime import RedisRuntime


def _record(sequence: int, previous: str, *, tenant: str = "acme"):
    import hashlib

    body = {
        "schema_version": 1,
        "sequence": sequence,
        "tenant_id": tenant,
        "source_cursor": f"100{sequence}-0",
        "occurred_at": f"1970-01-01T00:00:01.00{sequence}+00:00",
        "event": {"action": f"event-{sequence}"},
    }
    digest = hashlib.sha256(
        previous.encode("ascii") + canonical_json(body).encode("utf-8")
    ).hexdigest()
    return body | {"prev_hash": previous, "hash": digest}


def _chain(count: int = 3):
    entries, previous = [], GENESIS_HASH
    for sequence in range(1, count + 1):
        record = _record(sequence, previous)
        entries.append((f"{sequence}-0", {"payload": canonical_json(record)}))
        previous = record["hash"]
    head = {
        "schema_version": 1,
        "tenant_id": "acme",
        "sequence": count,
        "hash": previous,
        "source_cursor": f"100{count}-0" if count else None,
        "coverage_start_cursor": "1001-0" if count else None,
    }
    return entries, head


def test_pure_verifier_detects_mutation_missing_entry_and_foreign_tenant():
    entries, head = _chain()
    assert verify_records(entries, head, "acme")["valid"]

    mutated = deepcopy(entries)
    value = json.loads(mutated[1][1]["payload"])
    value["event"]["action"] = "rewritten"
    mutated[1][1]["payload"] = canonical_json(value)
    codes = {error["code"] for error in verify_records(mutated, head, "acme")["errors"]}
    assert "hash_mismatch" in codes

    missing_codes = {
        error["code"] for error in verify_records([entries[0], entries[2]], head, "acme")["errors"]
    }
    assert "missing_or_duplicate_sequence" in missing_codes
    assert not verify_records(entries, head, "other")["valid"]


def test_empty_chain_and_signed_checkpoint_contract():
    assert verify_records([], None, "acme")["valid"]
    unsigned = {
        "schema_version": 1,
        "tenant_id": "acme",
        "sequence": 0,
        "hash": GENESIS_HASH,
        "source_cursor": None,
        "coverage_start_cursor": None,
        "exported_at": "2026-10-29T00:00:00+00:00",
        "algorithm": "HMAC-SHA256",
        "key_id": "primary",
    }
    secret = "a" * 32
    checkpoint = unsigned | {"signature": sign_checkpoint(unsigned, secret)}
    assert verify_checkpoint(checkpoint, secret)
    assert not verify_checkpoint(checkpoint | {"hash": "f" * 64}, secret)
    assert not verify_checkpoint(checkpoint | {"extra": "field"}, secret)
    assert not verify_checkpoint(checkpoint, secret, tenant_id="other")
    assert not verify_checkpoint(checkpoint, "b" * 32)
    with pytest.raises(ValueError, match="32 bytes"):
        sign_checkpoint(unsigned, "short")


@pytest.fixture
async def audit_env():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Audit-chain integration requires TEST_REDIS_URL")
    runtime = RedisRuntime(url, stream_maxlen=1)
    await runtime.ping()
    tenant = "audit-" + uuid4().hex
    try:
        yield SimpleNamespace(runtime=runtime, tenant=tenant, url=url)
    finally:
        keys = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}*")]
        if keys:
            await runtime._client.delete(*keys)
        await runtime.close()


async def test_concurrent_projection_is_ordered_untrimmed_and_tenant_scoped(audit_env):
    runtime, tenant = audit_env.runtime, audit_env.tenant
    source = runtime._tenant_key(tenant, "audit")
    for index in range(50):
        await runtime._client.xadd(source, {"payload": json.dumps({"action": str(index)})})
    stores = [AuditChainStore(runtime) for _ in range(8)]
    await asyncio.gather(*(store.sync(tenant) for store in stores))
    report = await stores[0].verify(tenant)
    assert report["valid"] and report["sequence"] == 50

    # append_audit ignores the ordinary stream MAXLEN and projects before returning.
    await runtime.append_audit(tenant, {"action": "final"})
    assert await runtime._client.xlen(source) == 51
    assert (await stores[0].verify(tenant))["sequence"] == 51
    assert (await AuditChainStore(runtime).verify(tenant + "-other"))["sequence"] == 0


async def test_discovery_projector_catches_up_legacy_transaction_producer(audit_env):
    runtime, tenant = audit_env.runtime, audit_env.tenant
    await runtime._client.xadd(
        runtime._tenant_key(tenant, "audit"),
        {"payload": json.dumps({"action": "legacy.transaction"})},
    )
    assert not await runtime._client.exists(AuditChainStore(runtime).head_key(tenant))
    completed, failures = await project_audit_chains_once(runtime)
    assert completed >= 1 and failures == 0
    assert (await AuditChainStore(runtime).verify(tenant))["sequence"] == 1


async def test_background_projector_recovers_after_transient_scan_failure(monkeypatch):
    recovered = asyncio.Event()
    calls = 0

    async def flaky(_runtime):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("transient redis outage")
        recovered.set()
        return 0, 0

    monkeypatch.setattr(audit_chain_module, "project_audit_chains_once", flaky)
    task = asyncio.create_task(run_audit_chain_projector(object(), 0.01))
    try:
        await asyncio.wait_for(recovered.wait(), 1)
        assert calls >= 2
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_redis_deletion_and_source_mutation_are_detected(audit_env):
    store, tenant = AuditChainStore(audit_env.runtime), audit_env.tenant
    for index in range(3):
        await audit_env.runtime.append_audit(tenant, {"action": str(index)})
    await audit_env.runtime._client.xdel(store.chain_key(tenant), "2-0")
    deleted = await store.verify(tenant)
    assert not deleted["valid"]
    assert any(error["code"] == "missing_or_duplicate_sequence" for error in deleted["errors"])

    # Restore a separate tenant and prove source deletion is not hidden by an intact chain.
    source_tenant = tenant + "-source"
    await audit_env.runtime.append_audit(source_tenant, {"action": "must-remain"})
    source_store = AuditChainStore(audit_env.runtime)
    source_head = await source_store._head(source_tenant)
    await audit_env.runtime._client.xdel(
        source_store.source_key(source_tenant), source_head["source_cursor"]
    )
    source_deleted = await source_store.verify(source_tenant)
    assert not source_deleted["valid"]
    assert any(error["code"] == "source_entry_missing" for error in source_deleted["errors"])


async def test_human_admin_api_exports_and_verifies_checkpoint(audit_env):
    settings = Settings(
        _env_file=None,
        app_env="production",
        api_keys_json=json.dumps(
            {
                "admin": {"subject": "owner", "tenant_id": audit_env.tenant, "role": "admin"},
                "agent": {
                    "subject": "robot",
                    "tenant_id": audit_env.tenant,
                    "role": "admin",
                    "actor_type": "agent",
                },
            }
        ),
        audit_checkpoint_signing_key="checkpoint-secret-is-at-least-32-bytes",
        audit_checkpoint_key_id="2026-q4",
    )
    app = FastAPI()
    app.include_router(router)
    app.state.redis = audit_env.runtime
    app.state.settings = settings
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/v1/audit-chain/checkpoints", headers={"x-api-key": "admin"})
        assert response.status_code == 200
        checkpoint = response.json()
        assert checkpoint["key_id"] == "2026-q4"
        valid = await client.post(
            "/v1/audit-chain/checkpoints/verify",
            headers={"x-api-key": "admin"},
            json={"checkpoint": checkpoint},
        )
        assert valid.json() == {"valid": True}
        foreign_unsigned = {
            key: value for key, value in checkpoint.items() if key != "signature"
        } | {"tenant_id": audit_env.tenant + "-other"}
        foreign = foreign_unsigned | {
            "signature": sign_checkpoint(foreign_unsigned, settings.audit_checkpoint_signing_key)
        }
        scoped = await client.post(
            "/v1/audit-chain/checkpoints/verify",
            headers={"x-api-key": "admin"},
            json={"checkpoint": foreign},
        )
        assert scoped.json() == {"valid": False}
        forbidden = await client.get("/v1/audit-chain/verify", headers={"x-api-key": "agent"})
        assert forbidden.status_code == 403
