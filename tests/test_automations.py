import asyncio
import hashlib
import hmac
import json
import os
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

from app.automations import (
    AutomationCreate,
    TriggerConfig,
    dispatch,
    emit_internal_event,
    event_matches,
    materialize_goal,
    previous_day_summary,
    router,
)
from app.core.automation_scheduling import coalesce_due, next_run
from app.core.automation_store import AutomationStore
from app.core.config import Settings
from app.core.internal_events import decode_internal_event
from app.core.redis_runtime import RedisRuntime
from app.core.threat_detection import ThreatScan
from app.core.threat_feedback import record_threat_findings
from app.scheduler import tick


class FakeRouter:
    default_model = "local"

    def get(self, name):
        if name != "local":
            raise ValueError("Unknown model")
        return object()


class FakeDetector:
    async def ascan(self, text, *, event_type, location, **kwargs):
        blocked = "OVERRIDE_POLICY" in text
        findings = (
            ({"finding_id": "f1", "rule_id": "ATR-test", "severity": "high"},) if blocked else ()
        )
        return ThreatScan(event_type, location, findings, blocked)


class FakeRuntime:
    def __init__(self):
        self.audit = []
        self.findings = []

    async def append_audit(self, tenant, event):
        self.audit.append((tenant, deepcopy(event)))

    async def list_threat_findings(self, tenant, **kwargs):
        return self.findings


class FakeStore:
    def __init__(self):
        self.jobs = {}
        self.submissions = []

    async def create(self, job):
        self.jobs[(job["tenant_id"], job["automation_id"])] = deepcopy(job)
        return job

    async def get(self, tenant, automation_id):
        return deepcopy(self.jobs.get((tenant, automation_id)))

    async def list(self, tenant):
        return [deepcopy(j) for (t, _), j in self.jobs.items() if t == tenant]

    async def submit(self, tenant, automation_id, occurrence, goal, metadata, now, **kwargs):
        self.submissions.append(deepcopy(goal))
        return {"status": "queued", "goal_id": goal["goal_id"]}


def job():
    payload = AutomationCreate.model_validate(
        {
            "name": "Deployment analysis",
            "goal": {"objective": "分析部署失敗的原因，提出修復建議", "model": "local"},
            "trigger": {"kind": "webhook", "event_types": ["deployment.failed"]},
        }
    ).model_dump(mode="json")
    return payload | {
        "automation_id": "job-1",
        "version": 1,
        "agent_id": "server-agent",
        "tenant_id": "acme",
        "created_by": "operator",
        "next_run_at": None,
    }


def app():
    result = FastAPI()
    result.include_router(router)
    result.state.redis = FakeRuntime()
    result.state.automation_store = FakeStore()
    result.state.model_router = FakeRouter()
    result.state.threat_detector = FakeDetector()
    result.state.settings = Settings(
        api_keys_json=json.dumps(
            {
                "op": {"subject": "operator", "tenant_id": "acme", "role": "operator"},
                "view": {"subject": "viewer", "tenant_id": "acme", "role": "viewer"},
                "other": {"subject": "other", "tenant_id": "other", "role": "admin"},
            }
        ),
        pii_redaction=False,
    )
    return result


def test_cron_timezone_and_interval_phase():
    trigger = TriggerConfig(kind="schedule", cron="0 9 * * *").model_dump()
    now = datetime(2026, 10, 24, 23, tzinfo=UTC)
    assert next_run(trigger, now) == datetime(2026, 10, 25, 1, tzinfo=UTC)
    count, following = coalesce_due(
        trigger, datetime(2026, 10, 22, 1, tzinfo=UTC), datetime(2026, 10, 25, 2, tzinfo=UTC)
    )
    assert count == 4
    assert following == datetime(2026, 10, 26, 1, tzinfo=UTC)
    interval = TriggerConfig(kind="schedule", interval_seconds=3600).model_dump()
    count, following = coalesce_due(interval, now, datetime(2026, 10, 25, 2, 30, tzinfo=UTC))
    assert count == 4
    assert following == datetime(2026, 10, 25, 3, tzinfo=UTC)


def test_cron_dst_skips_nonexistent_time_and_retains_both_repeated_instants():
    trigger = {"kind": "schedule", "cron": "30 2 * * *", "timezone": "America/New_York"}
    # March 8 has no local 02:30; the next scheduled wall time is March 9.
    assert next_run(trigger, datetime(2026, 3, 7, 8, tzinfo=UTC)) == datetime(
        2026, 3, 9, 6, 30, tzinfo=UTC
    )
    trigger["cron"] = "30 1 * * *"
    first = next_run(trigger, datetime(2026, 10, 31, 8, tzinfo=UTC))
    second = next_run(trigger, first)
    assert first == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
    assert second == datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
    count, following = coalesce_due(trigger, first, second)
    assert count == 2
    assert following == datetime(2026, 11, 2, 6, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    "trigger",
    [
        {"kind": "schedule", "cron": "0 0 31 2 *"},
        {"kind": "schedule", "cron": "* * * * * *"},
        {"kind": "schedule", "cron": "0 9 * * *", "interval_seconds": 60},
        {"kind": "schedule", "interval_seconds": 60, "timezone": "Invalid/Zone"},
        {"kind": "internal", "event_types": ["anything"]},
        {"kind": "webhook", "event_types": []},
    ],
)
def test_invalid_trigger_rejected(trigger):
    with pytest.raises(ValidationError):
        TriggerConfig.model_validate(trigger)


def test_event_cannot_override_identity_or_policy_and_internal_filter_is_conjunctive():
    automation = job()
    malicious = {
        "type": "deployment.failed",
        "data": {
            "tenant_id": "attacker",
            "agent_id": "admin",
            "policy": {"allowed_tools": ["shell"]},
        },
    }
    materialized = materialize_goal(automation, malicious)
    assert materialized["tenant_id"] == "acme"
    assert materialized["agent_id"] == "server-agent"
    assert materialized["policy"] == automation["policy"]
    assert "未信任資料" in materialized["steps"][0]["query"]
    assert materialized["automation_version"] == 1
    automation["trigger"] = {
        "kind": "internal",
        "event_types": ["threat.detected"],
        "rule_ids": ["ATR-a"],
        "severities": ["high"],
    }
    event = {
        "type": "threat.detected",
        "data": {
            "findings": [
                {"rule_id": "ATR-a", "severity": "low"},
                {"rule_id": "ATR-b", "severity": "high"},
            ]
        },
    }
    assert not event_matches(automation, event)
    event["data"]["findings"].append({"rule_id": "ATR-a", "severity": "high"})
    assert event_matches(automation, event)
    event["source_automation_id"] = "other-job"
    assert not event_matches(automation, event)


@pytest.mark.asyncio
async def test_daily_summary_uses_previous_local_calendar_day_not_last24hours():
    runtime, automation = FakeRuntime(), job()
    runtime.findings = [
        {"created_at": "2026-10-23T15:59:59+00:00", "rule_id": "outside"},
        {"created_at": "2026-10-23T16:00:00+00:00", "rule_id": "ATR-a", "severity": "high"},
        {"created_at": "2026-10-24T15:59:59+00:00", "rule_id": "ATR-a", "severity": "low"},
        {"created_at": "2026-10-24T16:00:00+00:00", "rule_id": "outside"},
    ]
    result = await previous_day_summary(runtime, automation, datetime(2026, 10, 25, 1, tzinfo=UTC))
    assert result["total_findings"] == 2
    assert result["by_rule_id"] == {"ATR-a": 2}
    assert result["period_start"] == "2026-10-24T00:00:00+08:00"
    assert result["retained_metadata_only"] is True


@pytest.mark.asyncio
async def test_summary_keeps_server_dates_while_redacting_untrusted_context():
    runtime, store, automation = FakeRuntime(), FakeStore(), job()
    automation["task_type"] = "atr_summary"
    await dispatch(
        runtime,
        Settings(pii_redaction=True),
        FakeRouter(),
        FakeDetector(),
        store,
        automation,
        "summary:1",
        {"data": {"contact": "person@example.com"}},
        now=datetime(2026, 10, 25, 1, tzinfo=UTC),
    )
    goal = store.submissions[0]
    assert "person@example.com" not in goal["steps"][0]["query"]
    assert "2026-10-24T00:00:00+08:00" in goal["steps"][0]["query"]
    assert goal["atr_summary"]["total_findings"] == 0


@pytest.mark.asyncio
async def test_materialized_webhook_is_scanned_before_durable_enqueue():
    runtime, store, automation = FakeRuntime(), FakeStore(), job()
    with pytest.raises(HTTPException) as exc:
        await dispatch(
            runtime,
            Settings(pii_redaction=False),
            FakeRouter(),
            FakeDetector(),
            store,
            automation,
            "webhook:bad",
            {"data": {"text": "OVERRIDE_POLICY"}},
        )
    assert exc.value.status_code == 403
    assert store.submissions == []
    assert "OVERRIDE_POLICY" not in json.dumps(runtime.audit)


@pytest.mark.asyncio
async def test_management_roles_tenant_isolation_and_limit_enforcement():
    application = app()
    body = {
        "name": "daily",
        "goal": {"objective": "彙整昨日 ATR findings"},
        "task_type": "atr_summary",
        "trigger": {"kind": "schedule", "cron": "0 9 * * *"},
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application), base_url="http://test"
    ) as client:
        assert (
            await client.post("/v1/automations", json=body, headers={"x-api-key": "view"})
        ).status_code == 403
        created = await client.post("/v1/automations", json=body, headers={"x-api-key": "op"})
        assert created.status_code == 200
        record = created.json()
        assert record["agent_id"].startswith("agent-")
        assert isinstance(record["next_run_at"], float)
        response = await client.get(
            f"/v1/automations/{record['automation_id']}", headers={"x-api-key": "other"}
        )
        assert response.status_code == 404
        body["policy"] = {"max_runs_per_day": 25}
        assert (
            await client.post("/v1/automations", json=body, headers={"x-api-key": "op"})
        ).status_code == 403


@pytest.mark.asyncio
async def test_webhook_signature_age_size_payload_and_matching():
    application, automation = app(), job()
    await application.state.automation_store.create(automation)
    secret = "s" * 32
    application.state.settings.automation_webhook_secrets_json = json.dumps({"job-1": secret})
    raw = json.dumps(
        {
            "event_id": "deploy-1",
            "type": "deployment.failed",
            "data": {"service": "api", "exit_code": 1},
        }
    ).encode()
    now = str(int(datetime.now(UTC).timestamp()))

    def headers(timestamp=now, body=raw):
        signature = hmac.new(
            secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256
        ).hexdigest()
        return {
            "x-automation-timestamp": timestamp,
            "x-automation-signature": f"sha256={signature}",
        }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application), base_url="http://test"
    ) as client:
        url = "/v1/automations/hooks/acme/job-1"
        assert (await client.post(url, content=raw)).status_code == 401
        assert (await client.post(url, content=raw, headers=headers("1"))).status_code == 401
        assert (await client.post(url, content=raw + b" ", headers=headers())).status_code == 401
        malformed_headers = [
            (b"x-automation-timestamp", now.encode()),
            (b"x-automation-signature", b"sha256=\xc3\xa9"),
        ]
        assert (await client.post(url, content=raw, headers=malformed_headers)).status_code == 401
        accepted = await client.post(url, content=raw, headers=headers())
        assert accepted.json()["status"] == "queued"
        assert application.state.automation_store.submissions[0]["tenant_id"] == "acme"
        too_large = b" " * 65_537
        assert (
            await client.post(url, content=too_large, headers=headers(body=too_large))
        ).status_code == 413
        injected = json.dumps(
            {"event_id": "deploy-2", "type": "deployment.failed", "data": {}, "tenant_id": "other"}
        ).encode()
        assert (
            await client.post(url, content=injected, headers=headers(body=injected))
        ).status_code == 422
        wrong_type = json.dumps(
            {"event_id": "deploy-3", "type": "deployment.succeeded", "data": {}}
        ).encode()
        assert (
            await client.post(url, content=wrong_type, headers=headers(body=wrong_type))
        ).status_code == 422
        nested = json.dumps(
            {
                "event_id": "deploy-4",
                "type": "deployment.failed",
                "data": {
                    "tenant_id": "other",
                    "policy": {"allowed_tools": ["shell"]},
                    "agent_id": "root",
                },
            }
        ).encode()
        assert (
            await client.post(url, content=nested, headers=headers(body=nested))
        ).status_code == 200
        saved = application.state.automation_store.submissions[-1]
        assert saved["tenant_id"] == "acme"
        assert saved["agent_id"] == "server-agent"
        assert saved["policy"] == automation["policy"]


@pytest.mark.parametrize(
    "policy",
    [
        {"max_runs_per_day": 25},
        {"token_budget": 20_001},
        {"approval_timeout_seconds": 86_401},
    ],
)
@pytest.mark.asyncio
async def test_operator_cannot_raise_any_automation_cap(policy):
    application = app()
    body = {
        "name": "elevated",
        "goal": {"objective": "Analyze findings"},
        "trigger": {"kind": "schedule", "interval_seconds": 3600},
        "policy": policy,
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application), base_url="http://test"
    ) as client:
        result = await client.post("/v1/automations", json=body, headers={"x-api-key": "op"})
        assert result.status_code == 403
        assert not application.state.automation_store.jobs


@pytest.mark.asyncio
async def test_internal_event_omits_content_and_suppresses_chain():
    events = []

    class Client:
        async def xadd(self, stream, fields):
            events.append((stream, fields))
            return "1-0"

    runtime = SimpleNamespace(_client=Client())
    await emit_internal_event(
        runtime,
        "acme",
        "threat.detected",
        "f1",
        {
            "rule_id": "ATR-a",
            "severity": "high",
            "content": "secret payload",
        },
    )
    assert "secret payload" not in json.dumps(events)
    envelope = json.loads(events[0][1]["payload"])
    assert envelope["schema_version"] == 1
    assert envelope["event_type"] == "threat.detected"
    assert envelope["payload"]["data"]["findings"] == [
        {"rule_id": "ATR-a", "severity": "high"}
    ]
    assert (
        await emit_internal_event(
            runtime, "acme", "goal.failed", "g1", {}, source_automation_id="job-1"
        )
        is None
    )
    assert len(events) == 1


@pytest.fixture
async def redis_automation():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL for real Redis scheduler checks")
    runtime = RedisRuntime(url)
    await runtime.ping()
    store, tenant = AutomationStore(runtime), f"scheduler-test-{uuid4().hex}"
    try:
        yield runtime, store, tenant
    finally:
        for automation in await store.list(tenant):
            await runtime._client.zrem(
                "automation:due", store._member(tenant, automation["automation_id"])
            )
        for stream in ("agent:goal:queue", "automation:internal-events"):
            rows = await runtime._client.xrange(stream)
            ids = [entry_id for entry_id, fields in rows if fields.get("tenant_id") == tenant]
            if ids:
                await runtime._client.xdel(stream, *ids)
        keys = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}:*")]
        if keys:
            await runtime._client.delete(*keys)
        await runtime._client.srem("automation:tenants", tenant)
        await runtime._client.srem("agent:goal:tenants", tenant)
        await runtime.close()


@pytest.mark.asyncio
async def test_parallel_ticks_recover_missed_cron_once_and_flush_overlap(redis_automation):
    runtime, store, tenant = redis_automation
    automation = job() | {
        "tenant_id": tenant,
        "automation_id": f"cron-{uuid4()}",
        "trigger": {"kind": "schedule", "cron": "0 9 * * *", "timezone": "Asia/Taipei"},
        "next_run_at": datetime(2026, 10, 22, 1, tzinfo=UTC).timestamp(),
    }
    await store.create(automation)
    now = datetime(2026, 10, 25, 2, tzinfo=UTC)
    settings = Settings(pii_redaction=False)
    await asyncio.gather(
        *[
            tick(
                runtime,
                settings,
                FakeRouter(),
                FakeDetector(),
                store=store,
                now=now,
                consumer=f"test-{i}",
            )
            for i in range(5)
        ]
    )
    goals = await store.list_goals(tenant, automation["automation_id"])
    assert len(goals) == 1
    assert goals[0]["coalesced_count"] == 4
    saved = await store.get(tenant, automation["automation_id"])
    assert saved["next_run_at"] == datetime(2026, 10, 26, 1, tzinfo=UTC).timestamp()
    later = datetime(2026, 10, 26, 2, tzinfo=UTC)
    await tick(runtime, settings, FakeRouter(), FakeDetector(), store=store, now=later)
    assert len(await store.list_goals(tenant, automation["automation_id"])) == 1
    goal = goals[0] | {"status": "completed"}
    await runtime.save_goal(goal["goal_id"], goal, tenant)
    await tick(runtime, settings, FakeRouter(), FakeDetector(), store=store, now=later)
    assert len(await store.list_goals(tenant, automation["automation_id"])) == 2


@pytest.mark.asyncio
async def test_crash_after_dispatch_before_external_cas_keeps_later_occurrences(
    redis_automation, monkeypatch
):
    runtime, store, tenant = redis_automation
    start = datetime(2026, 10, 25, 2, tzinfo=UTC)
    automation = job() | {
        "tenant_id": tenant,
        "automation_id": f"crash-cursor-{uuid4()}",
        "trigger": {"kind": "schedule", "interval_seconds": 60, "timezone": "Asia/Taipei"},
        "next_run_at": start.timestamp(),
    }
    await store.create(automation)
    original_advance = store.advance_schedule

    async def crash_before_external_cas(*args, **kwargs):
        raise RuntimeError("simulated process crash after durable dispatch")

    monkeypatch.setattr(store, "advance_schedule", crash_before_external_cas)
    with pytest.raises(RuntimeError, match="simulated process crash"):
        await tick(
            runtime,
            Settings(pii_redaction=False),
            FakeRouter(),
            FakeDetector(),
            store=store,
            now=start,
        )
    # The accepted range and cursor commit with its Goal, before any external CAS.
    saved = await store.get(tenant, automation["automation_id"])
    assert saved["next_run_at"] == start.timestamp() + 60
    first = (await store.list_goals(tenant, automation["automation_id"]))[0]
    assert first["coalesced_count"] == 1
    monkeypatch.setattr(store, "advance_schedule", original_advance)
    recovered_at = datetime.fromtimestamp(start.timestamp() + 120, UTC)
    await tick(
        runtime,
        Settings(pii_redaction=False),
        FakeRouter(),
        FakeDetector(),
        store=store,
        now=recovered_at,
    )
    saved = await store.get(tenant, automation["automation_id"])
    assert saved["next_run_at"] == start.timestamp() + 180
    pending = json.loads(
        await runtime._client.get(store._key(tenant, automation["automation_id"], ":pending"))
    )
    assert pending["count"] == 2
    first["status"] = "completed"
    await runtime.save_goal(first["goal_id"], first, tenant)
    await tick(
        runtime,
        Settings(pii_redaction=False),
        FakeRouter(),
        FakeDetector(),
        store=store,
        now=recovered_at,
    )
    goals = await store.list_goals(tenant, automation["automation_id"])
    assert len(goals) == 2
    assert sum(goal["coalesced_count"] for goal in goals) == 3
    assert (await store.get(tenant, automation["automation_id"]))["goal"]["steps"] == []


@pytest.mark.asyncio
async def test_real_internal_delivery_filters_deduplicates_and_does_not_chain(redis_automation):
    runtime, store, tenant = redis_automation
    automation = job() | {
        "tenant_id": tenant,
        "automation_id": f"event-{uuid4()}",
        "trigger": {
            "kind": "internal",
            "timezone": "Asia/Taipei",
            "event_types": ["threat.detected"],
            "rule_ids": ["ATR-1"],
            "severities": ["high"],
        },
    }
    await store.create(automation)
    await emit_internal_event(
        runtime, tenant, "threat.detected", "low", {"rule_id": "ATR-1", "severity": "low"}
    )
    for _ in range(2):
        await emit_internal_event(
            runtime,
            tenant,
            "threat.detected",
            "same-high",
            {"rule_id": "ATR-1", "severity": "high"},
        )
    await emit_internal_event(
        runtime,
        tenant,
        "threat.detected",
        "chain",
        {"rule_id": "ATR-1", "severity": "high"},
        source_automation_id="another",
    )
    await tick(runtime, Settings(pii_redaction=False), FakeRouter(), FakeDetector(), store=store)
    goals = await store.list_goals(tenant, automation["automation_id"])
    assert len(goals) == 1
    assert goals[0]["trigger_metadata"]["event_id"] == "same-high"
    assert goals[0]["coalesced_count"] == 1
    pending = await runtime._client.xpending("automation:internal-events", "automation-schedulers")
    assert pending["pending"] == 0


@pytest.mark.asyncio
async def test_real_manual_overlap_and_disable_reenable_fence_old_version(redis_automation):
    runtime, store, tenant = redis_automation
    automation = job() | {"tenant_id": tenant, "automation_id": f"controls-{uuid4()}"}
    await store.create(automation)
    application = app()
    application.state.redis, application.state.automation_store = runtime, store
    application.state.settings.api_keys_json = json.dumps(
        {"op": {"subject": "operator", "tenant_id": tenant, "role": "operator"}}
    )
    url = f"/v1/automations/{automation['automation_id']}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://test",
        headers={"x-api-key": "op"},
    ) as client:
        first = await client.post(url + "/run")
        second = await client.post(url + "/run")
        assert first.json()["status"] == "queued"
        assert second.json()["status"] == "coalesced"
        assert first.json()["goal_id"] == second.json()["goal_id"]
        paused = await client.post(url + "/enabled", json={"enabled": False})
        resumed = await client.post(url + "/enabled", json={"enabled": True})
        assert paused.json()["version"] == 2
        assert resumed.json()["version"] == 3
    stale_goal = materialize_goal(automation)
    result = await store.submit(
        tenant,
        automation["automation_id"],
        "old-config",
        stale_goal,
        {},
        datetime.now(UTC).timestamp(),
    )
    assert result["status"] == "stale"


@pytest.mark.parametrize("source_automation_id", [None, "scheduled-source"])
@pytest.mark.asyncio
async def test_atr_source_and_internal_event_commit_before_post_save_crash(
    redis_automation, monkeypatch, source_automation_id
):
    runtime, _, tenant = redis_automation
    finding_id = str(uuid4())
    event = {
        "event_type": "tool_response",
        "location": "source-crash-test",
        "content": "SECRET_UNTRUSTED_PAYLOAD",
        "findings": [
            {
                "finding_id": finding_id,
                "rule_id": "ATR-atomic",
                "severity": "high",
                "title": "Synthetic test finding",
                "matched_text": "SECRET_UNTRUSTED_PAYLOAD",
            }
        ],
    }

    async def crash_after_source_transaction(*args, **kwargs):
        raise RuntimeError("simulated crash immediately after source save")

    monkeypatch.setattr(runtime._client, "zcard", crash_after_source_transaction)
    with pytest.raises(RuntimeError, match="immediately after source save"):
        await record_threat_findings(
            runtime,
            Settings(),
            tenant,
            event,
            context_id="atomic-source",
            model="local",
            source_automation_id=source_automation_id,
        )
    assert await runtime.get_threat_finding(tenant, finding_id)
    queued = [
        decode_internal_event(fields["payload"], tenant, cursor)
        for cursor, fields in await runtime._client.xrange("automation:internal-events")
        if fields.get("tenant_id") == tenant
    ]
    assert len(queued) == (0 if source_automation_id else 1)
    assert "SECRET_UNTRUSTED_PAYLOAD" not in json.dumps(queued)
    if queued:
        assert queued[0]["event_id"] == finding_id
        assert queued[0]["data"]["findings"][0]["rule_id"] == "ATR-atomic"
