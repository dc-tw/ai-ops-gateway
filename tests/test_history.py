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
    sanitize_record,
    search_text,
    tenant_from_key,
)
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


class HistoryStore:
    def __init__(self):
        self.calls = []
        self.migration = {"status": "verified"}
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

    async def status(self, tenant):
        self.call("status", tenant)
        return {"records": 1, "events": 1, "source_cursor": "101-2"}

    async def purge(self, tenant, **kwargs):
        self.call("purge", tenant, **kwargs)
        return {"records": 1, "events": 0, "source_cursor": kwargs["before_cursor"]}


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
        "search",
        ("acme", query),
        {"kind": "goal", "after": "", "limit": 50},
    )


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
        "purge",
        ("other",),
        {"kind": "conversation", "before_cursor": "100-0"},
    )


@pytest.mark.parametrize("report", [None, {}, {"status": "prepared"}, {"status": "failed"}])
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
