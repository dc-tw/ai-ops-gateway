"""Archive classification and HTTP boundaries do not require external services."""

import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from app.core.config import Settings
from app.core.durable_types import (
    classify_key,
    conversation_id_from_key,
    event_search_text,
    sanitize_record,
    search_text,
    tenant_from_key,
)
from app.core.postgres_schema import SCHEMA_VERSION
from app.history import router


@pytest.mark.parametrize(
    ("suffix", "kind", "resource", "mode"),
    [
        ("agents:record:agent-1", "agent", "agent-1", "record"),
        ("skills:record:skill-1", "skill", "skill-1", "record"),
        ("skills:revision:skill-1:2", "skill_revision", "skill-1:2", "record"),
        ("planning:session:session-1", "planning_session", "session-1", "record"),
        ("planning:revision:session-1:8", "planning_revision", "session-1:8", "record"),
        ("agent:goal:goal-1", "goal", "goal-1", "record"),
        ("agent:run:run-1", "run", "run-1", "record"),
        ("security:threat-finding:finding-1", "finding", "finding-1", "record"),
        ("automation:job-1", "automation", "job-1", "record"),
        ("goal:goal-1:tool-receipt:action-1", "tool_receipt", "goal-1:action-1", "record"),
        ("conversation:incident:prod:messages", "conversation", "incident:prod", "messages"),
        ("agent:events:run-1", "run", "run-1", "events"),
        ("agent:goal:events:goal-1", "goal", "goal-1", "events"),
        ("planning:events:session-1", "planning", "session-1", "events"),
        ("automation:job-1:events", "automation", "job-1", "events"),
        ("audit", "audit", "audit", "events"),
    ],
)
def test_classification_is_explicit_and_tenant_validated(suffix, kind, resource, mode):
    key = "tenant:acme.org:" + suffix
    assert tenant_from_key(key) == "acme.org"
    assert classify_key(key) == (kind, resource, mode)
    assert classify_key("tenant:bad tenant:" + suffix) is None


@pytest.mark.parametrize(
    "suffix",
    [
        "quota:tokens:2026-10-26",
        "quota:active-runs",
        "skills:name:secret",
        "skills",
        "skills:revision:skill-1:0",
        "agents:daily-runs:agent-1:2026-10-26",
        "planning:sessions",
        "goal:goal-1:browser-session:session-1",
        "automation:job-1:active",
        "automation:job-1:pending",
        "automation:job-1:occurrence:hash",
        "conversation:incident:summary",
        "conversation:incident:memory",
        "checkpoint:raw-private",
        "credentials:secret",
        "agent:run:bad/id",
    ],
)
def test_unknown_or_execution_internal_keys_are_not_archived(suffix):
    assert classify_key("tenant:acme:" + suffix) is None


@pytest.mark.parametrize(
    "key", ["agent:result:private", "agent:goal:queue", "tenant::audit", "tenant:acme", "audit"]
)
def test_invalid_global_or_tenant_keys_fail_closed(key):
    assert tenant_from_key(key) is None
    assert classify_key(key) is None


@pytest.mark.parametrize("component", ["messages", "summary", "memory"])
def test_all_conversation_components_identify_the_same_deletion(component):
    assert conversation_id_from_key(f"tenant:acme:conversation:prod:incident:{component}") == (
        "prod:incident"
    )
    assert conversation_id_from_key("tenant:acme:goal:goal-1:browser-session:private") is None


def test_receipt_and_search_allowlists_exclude_private_arguments_and_prompts():
    data = {
        "status": "completed",
        "tool": "browser_click",
        "action_id": "action-1",
        "arguments": {"password": "PRIVATE"},
        "raw_response": "UNSCANNED",
        "instructions": "SECRET_SKILL",
        "persona": "SECRET_PERSONA",
        "context": "SECRET_CONTEXT",
        "objective": "SECRET_OBJECTIVE",
        "query": "SECRET_QUERY",
        "answer": "SECRET_ANSWER",
    }
    assert sanitize_record("tool_receipt", data) == {
        "status": "completed",
        "tool": "browser_click",
        "action_id": "action-1",
    }
    indexed = search_text("goal", data)
    assert indexed == "completed browser_click action-1"
    assert "SECRET" not in indexed and "PRIVATE" not in indexed and "UNSCANNED" not in indexed
    assert search_text("conversation", [{"content": "PRIVATE_MESSAGE"}]) == ""
    assert search_text("unknown", data) == ""
    assert sanitize_record("tool_receipt", "PRIVATE") == {}


def test_controlled_goal_body_and_provenance_are_searchable_only_after_approval():
    goal = {
        "status": "completed",
        "objective": "分析部署失敗",
        "steps": [
            {
                "status": "completed",
                "query": "找出根因",
                "answer": "根因是設定漂移",
                "tool_result": "PRIVATE_TOOL_RESULT",
            }
        ],
    }
    assert "部署失敗" not in search_text("goal", goal)
    indexed = search_text("goal", goal | {"history_search_approved": True})
    assert all(value in indexed for value in ("部署失敗", "找出根因", "設定漂移"))
    assert "PRIVATE_TOOL_RESULT" not in indexed

    skill = {
        "name": "safe-review",
        "instructions": "PRIVATE_INSTRUCTIONS",
        "provenance": {
            "source": "github",
            "repository": "dc-tw/agent-threat-rules",
            "commit": "a" * 40,
            "package_sha256": "b" * 64,
            "raw_archive": "PRIVATE_ARCHIVE",
        },
    }
    indexed = search_text("skill", skill)
    assert "dc-tw/agent-threat-rules" in indexed
    assert "PRIVATE_INSTRUCTIONS" not in indexed and "PRIVATE_ARCHIVE" not in indexed


def test_event_search_indexes_safe_audit_and_evidence_without_raw_attack_or_tool_content():
    audit = {
        "action": "goal.threat_detection",
        "status": "blocked",
        "subject": "operator",
        "event": {"user_input": "RAW_ATTACK", "tool_response": "PRIVATE_TOOL"},
    }
    indexed = event_search_text("audit", audit)
    assert all(value in indexed for value in ("goal.threat_detection", "blocked", "operator"))
    assert "RAW_ATTACK" not in indexed and "PRIVATE_TOOL" not in indexed

    evidence = {
        "type": "agent_event",
        "history_search_approved": True,
        "event": {
            "type": "completed",
            "answer": "已驗證的結論",
            "evidence_validated": True,
            "tool_result": "PRIVATE_TOOL",
            "evidence_report": {
                "evidence_sources": [
                    {"url": "https://example.com/proof", "content_sha256": "sha256:abc"}
                ]
            },
        },
    }
    indexed = event_search_text("goal", evidence)
    assert all(value in indexed for value in ("已驗證的結論", "example.com/proof", "sha256:abc"))
    assert "PRIVATE_TOOL" not in indexed


class HistoryStore:
    def __init__(self):
        self.calls = []
        self.migration = {"status": "verified", "schema_version": SCHEMA_VERSION}
        self.failure = None

    def call(self, name, *args, **kwargs):
        self.calls.append((name, args, kwargs))
        if self.failure:
            raise self.failure

    async def latest_migration(self):
        self.call("latest_migration")
        return self.migration

    @staticmethod
    def record(tenant="acme"):
        return {
            "tenant_id": tenant,
            "kind": "goal",
            "resource_id": "goal-1",
            "data": {"status": "completed"},
            "source_cursor": "101-2",
            "cursor": "opaque-cursor",
        }

    async def get_record(self, tenant, kind, resource_id):
        self.call("get_record", tenant, kind, resource_id)
        return (
            self.record(tenant)
            if (tenant, kind, resource_id) == ("acme", "goal", "goal-1")
            else None
        )

    async def list_records(self, tenant, **kwargs):
        self.call("list_records", tenant, **kwargs)
        return [self.record(tenant)] if tenant == "acme" else []

    async def events(self, tenant, **kwargs):
        self.call("events", tenant, **kwargs)
        return [self.record(tenant) | {"cursor": 42}]

    async def search(self, tenant, query, **kwargs):
        self.call("search", tenant, query, **kwargs)
        return [self.record(tenant)]

    async def search_documents(self, tenant, query, **kwargs):
        self.call("search_documents", tenant, query, **kwargs)
        row = self.record(tenant) | {"document_type": "record", "summary": "completed"}
        if kwargs.get("include_relations"):
            row["_relation_candidates"] = [
                {"kind": "conversation", "resource_id": "goal-1", "relation": "conversation"}
            ]
        return [row]

    async def status(self, tenant):
        self.call("status", tenant)
        return {"records": 1, "events": 1, "source_cursor": "101-2"}

    async def preview_purge(self, tenant, **kwargs):
        self.call("preview_purge", tenant, **kwargs)
        return {
            "records": 1,
            "events": 0,
            "source_cursor": kwargs["before_cursor"],
            "preview_sha256": "a" * 64,
            "dry_run": True,
            "executed": False,
        }

    async def purge(self, tenant, **kwargs):
        self.call("purge", tenant, **kwargs)
        return {
            "records": 1,
            "events": 0,
            "source_cursor": kwargs["before_cursor"],
            "preview_sha256": kwargs["expected_preview_sha256"],
            "dry_run": False,
            "executed": True,
        }


@pytest.fixture
async def history_environment():
    keys = {
        role: {"subject": role, "tenant_id": "acme", "role": role}
        for role in ("admin", "operator", "viewer")
    }
    keys["foreign"] = {"subject": "foreign", "tenant_id": "other", "role": "admin"}
    settings = Settings(app_env="production", api_keys_json=json.dumps(keys))
    configured = settings.model_dump() | {"postgres_history_mode": "postgres"}
    app = FastAPI()
    app.state.settings = SimpleNamespace(**configured)
    app.state.postgres = HistoryStore()
    app.state.redis = SimpleNamespace(_client=object())
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"x-api-key": "viewer"},
    ) as client:
        yield SimpleNamespace(app=app, client=client, store=app.state.postgres)


async def test_history_read_metadata_pagination_and_tenant_scope(history_environment):
    env = history_environment
    response = await env.client.get("/v1/history/records?kind=goal&limit=1&after=opaque")
    assert response.status_code == 200
    payload = response.json()
    assert payload["history_only"] is True
    assert payload["execution_authority"] is False
    assert payload["consistency"] == "eventual"
    assert payload["source_cursor"] == "101-2"
    assert payload["next_cursor"] == "opaque-cursor"
    assert env.store.calls[-1] == (
        "list_records",
        ("acme",),
        {"kind": "goal", "after": "opaque", "limit": 1},
    )
    record = await env.client.get("/v1/history/records/goal/goal-1")
    assert record.status_code == 200
    assert record.json()["record"]["data"]["status"] == "completed"
    foreign = await env.client.get(
        "/v1/history/records/goal/goal-1", headers={"x-api-key": "foreign"}
    )
    assert foreign.status_code == 404
    assert (await env.client.get("/v1/history/records/goal/missing")).status_code == 404
    assert [call for call in env.store.calls if call[0] == "get_record"][-2][1][0] == "other"


async def test_events_and_literal_search_text_are_scoped_parameters(history_environment):
    env = history_environment
    events = await env.client.get(
        "/v1/history/events",
        params={"kind": "conversation", "resource_id": "prod:incident", "after": 7, "limit": 1},
    )
    assert events.status_code == 200 and events.json()["next_cursor"] == 42
    assert env.store.calls[-1] == (
        "events",
        ("acme",),
        {"kind": "conversation", "resource_id": "prod:incident", "after": 7, "limit": 1},
    )
    query = "' OR tenant_id='other' --"
    searched = await env.client.get("/v1/history/search", params={"q": query, "kind": "goal"})
    assert searched.status_code == 200
    assert env.store.calls[-1] == (
        "search_documents",
        ("acme", query),
        {
            "kind": "goal",
            "scope": "all",
            "after": "",
            "limit": 50,
            "include_relations": False,
        },
    )

    expanded = await env.client.get(
        "/v1/history/search",
        params={
            "q": "completed",
            "include_summary": "true",
            "expand": "relations",
        },
    )
    assert expanded.status_code == 200
    payload = expanded.json()
    assert payload["summary"] == {
        "returned": 1,
        "by_document_type": {"record": 1, "event": 0},
        "by_kind": {"goal": 1},
        "acl_rechecked": True,
    }
    assert payload["results"][0]["relations"] == [
        {"kind": "conversation", "resource_id": "goal-1", "relation": "conversation"}
    ]


@pytest.mark.parametrize("role", ["viewer", "operator"])
async def test_admin_operations_require_admin(history_environment, role):
    env = history_environment
    for method, path, body in (
        ("get", "/v1/history/status", None),
        ("post", "/v1/history/purge", {"kind": "goal", "before_cursor": "100-0"}),
    ):
        kwargs = {"headers": {"x-api-key": role}}
        if body is not None:
            kwargs["json"] = body
        response = await getattr(env.client, method)(path, **kwargs)
        assert response.status_code == 403
    assert not env.store.calls


async def test_status_and_purge_work_in_shadow_but_do_not_enable_reads(history_environment):
    env = history_environment
    env.app.state.settings.postgres_history_mode = "shadow"
    assert (await env.client.get("/v1/history/records")).status_code == 409
    assert not env.store.calls
    status = await env.client.get("/v1/history/status", headers={"x-api-key": "admin"})
    assert status.status_code == 200 and status.json()["status"]["records"] == 1
    purged = await env.client.post(
        "/v1/history/purge",
        json={"kind": "conversation", "before_cursor": "100-0"},
        headers={"x-api-key": "foreign"},
    )
    assert purged.status_code == 200
    assert env.store.calls[-1] == (
        "preview_purge",
        ("other",),
        {"kind": "conversation", "before_cursor": "100-0"},
    )
    assert purged.json()["purge"]["dry_run"] is True

    executed = await env.client.post(
        "/v1/history/purge",
        json={
            "kind": "conversation",
            "before_cursor": "100-0",
            "execute": True,
            "expected_preview_sha256": "a" * 64,
        },
        headers={"x-api-key": "admin"},
    )
    assert executed.status_code == 200 and executed.json()["purge"]["executed"] is True
    assert env.store.calls[-1] == (
        "purge",
        ("acme",),
        {
            "kind": "conversation",
            "before_cursor": "100-0",
            "expected_preview_sha256": "a" * 64,
            "subject": "admin",
        },
    )


async def test_poison_diagnosis_and_repair_are_admin_only_and_never_echo_payload(
    history_environment, monkeypatch
):
    from app import postgres_worker

    env = history_environment
    calls = []

    async def diagnose(runtime, source_id, **kwargs):
        calls.append(("diagnose", runtime, source_id, kwargs))
        return {
            "records": [
                {
                    "event_id": "10-1",
                    "error": "invalid_envelope",
                    "pending": True,
                    "payload_sha256": "b" * 64,
                }
            ],
            "next_after": None,
        }

    async def repair(runtime, store, event_id, expected, replacement, subject, **kwargs):
        calls.append(
            ("repair", runtime, store, event_id, expected, replacement, subject, kwargs)
        )
        return {
            "event_id": event_id,
            "status": "repaired",
            "payload_sha256": expected,
            "replacement_sha256": "c" * 64,
            "acknowledged_after_projection": True,
        }

    monkeypatch.setattr(postgres_worker, "diagnose_poison", diagnose)
    monkeypatch.setattr(postgres_worker, "repair_poison", repair)
    assert (await env.client.get("/v1/history/poison")).status_code == 403
    diagnosed = await env.client.get(
        "/v1/history/poison?after=0-0&limit=10", headers={"x-api-key": "admin"}
    )
    assert diagnosed.status_code == 200
    assert diagnosed.json()["records"][0]["payload_sha256"] == "b" * 64
    replacement = {
        "schema_version": 1,
        "command": "SET",
        "key": "tenant:acme:agent:goal:goal-1",
        "value": "PRIVATE_CORRECTED_ENVELOPE",
    }
    repaired = await env.client.post(
        "/v1/history/poison/10-1/repair",
        headers={"x-api-key": "admin"},
        json={
            "expected_payload_sha256": "b" * 64,
            "replacement_envelope": replacement,
        },
    )
    assert repaired.status_code == 200
    assert "PRIVATE_CORRECTED_ENVELOPE" not in repaired.text
    assert calls[-1][0] == "repair" and calls[-1][-2] == "admin"
    assert calls[-1][-1] == {"tenant_id": "acme"}


@pytest.mark.parametrize(
    "report",
    [
        None,
        {},
        {"status": "prepared", "schema_version": SCHEMA_VERSION},
        {"status": "failed", "schema_version": SCHEMA_VERSION},
        {"status": "verified"},
        {"status": "verified", "schema_version": SCHEMA_VERSION - 1},
    ],
)
async def test_unverified_migration_cannot_enable_history_reads(history_environment, report):
    env = history_environment
    env.store.migration = report
    for path in (
        "/v1/history/records",
        "/v1/history/records/goal/goal-1",
        "/v1/history/events",
        "/v1/history/search?q=goal",
    ):
        assert (await env.client.get(path)).status_code == 409
    assert all(call[0] == "latest_migration" for call in env.store.calls)


@pytest.mark.parametrize(
    "path",
    [
        "/v1/history/records?tenant_id=other",
        "/v1/history/records?sql=DROP",
        "/v1/history/records?after=a&after=b",
        "/v1/history/records?limit=0",
        "/v1/history/records?limit=201",
        "/v1/history/records?kind=credentials",
        "/v1/history/records?after=" + "x" * 513,
        "/v1/history/records/goal/goal-1?tenant_id=other",
        "/v1/history/events?after=-1",
        "/v1/history/events?after=1-0",
        "/v1/history/events?tenant=other",
        "/v1/history/search?q=",
        "/v1/history/search?q=%20%20",
        "/v1/history/search?q=" + "x" * 201,
        "/v1/history/search?q=private&where=tenant_id",
    ],
)
async def test_invalid_queries_fail_without_reflecting_input(history_environment, path):
    env = history_environment
    response = await env.client.get(path)
    assert response.status_code == 422
    assert response.json() == {"detail": "Invalid history request"}
    assert not env.store.calls


@pytest.mark.parametrize(
    "body",
    [
        {"kind": "goal", "before_cursor": "100-0", "tenant_id": "other"},
        {"kind": "goal", "before_cursor": 100},
        {"kind": "goal", "before_cursor": "PRIVATE_DSN"},
        {"kind": "credentials", "before_cursor": "100-0"},
        {"kind": "goal", "before_cursor": "100-0", "execute": True},
        {
            "kind": "goal",
            "before_cursor": "100-0",
            "execute": False,
            "expected_preview_sha256": "a" * 64,
        },
    ],
)
async def test_purge_body_is_strict_and_does_not_echo_private_data(history_environment, body):
    response = await history_environment.client.post(
        "/v1/history/purge", json=body, headers={"x-api-key": "admin"}
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Invalid history request"}
    assert not history_environment.store.calls


async def test_public_storage_errors_never_expose_dsns_or_payloads(history_environment):
    env = history_environment
    env.store.failure = RuntimeError("postgres://private:secret@host/db PRIVATE_PAYLOAD")
    for path in ("/v1/history/records", "/v1/history/status"):
        response = await env.client.get(path, headers={"x-api-key": "admin"})
        assert response.status_code == 503
        assert response.json() == {"detail": "History storage unavailable"}
    env.app.state.postgres = None
    assert (await env.client.get("/v1/history/records")).status_code == 503


async def test_invalid_opaque_cursor_has_generic_validation_error(history_environment):
    env = history_environment
    env.store.failure = ValueError("PRIVATE_CURSOR")
    response = await env.client.get("/v1/history/records?after=broken")
    assert response.status_code == 422
    assert response.json() == {"detail": "Invalid history request"}


async def test_history_requires_authentication(history_environment):
    response = await history_environment.client.get(
        "/v1/history/records", headers={"x-api-key": "invalid"}
    )
    assert response.status_code == 401
    assert not history_environment.store.calls
