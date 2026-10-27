import asyncio
import json
import os
import time
from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx
import pytest
from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

from app.agents import AgentCreate, AgentStore, apply_agent, router
from app.automations import AutomationPolicy
from app.core.config import Settings
from app.core.redis_runtime import RedisRuntime
from app.core.threat_detection import ThreatScan


def record(tenant="acme", agent_id="worker-1", **changes):
    now = datetime.now(UTC).isoformat()
    return {
        "agent_id": agent_id,
        "tenant_id": tenant,
        "name": "資安分析師",
        "description": "檢查證據",
        "persona": "以繁體中文提出具來源的資安分析。",
        "role": "worker",
        "model": "local",
        "enabled": True,
        "version": 1,
        "delegate_agent_ids": [],
        "policy": AutomationPolicy().model_dump(),
        "created_at": now,
        "updated_at": now,
        "created_by": "admin",
        **changes,
    }


class MemoryAgents:
    def __init__(self):
        self.records = {}

    async def get(self, tenant, agent_id):
        return deepcopy(self.records.get((tenant, agent_id)))

    async def list(self, tenant):
        return [deepcopy(row) for (owner, _), row in self.records.items() if owner == tenant]

    async def create(self, item):
        rows = [
            json.dumps(await self.get(item["tenant_id"], aid))
            if await self.get(item["tenant_id"], aid)
            else None
            for aid in item["delegate_agent_ids"]
        ]
        AgentStore._check_delegates(item, rows)
        self.records[(item["tenant_id"], item["agent_id"])] = deepcopy(item)
        return item

    async def update(self, item, expected_version):
        current = await self.get(item["tenant_id"], item["agent_id"])
        if not current or current["version"] != expected_version:
            return None
        item = {**item, "version": expected_version + 1}
        return await self.create(item)

    async def set_enabled(self, tenant, agent_id, enabled):
        item = await self.get(tenant, agent_id)
        if item is not None:
            item["enabled"] = enabled
            return await self.update(item, item["version"])
        return None


class FakeRuntime:
    def __init__(self):
        self.agent_store = MemoryAgents()
        self.goals = {}
        self.audit = []
        self.queue = []

    async def append_audit(self, tenant, event):
        self.audit.append((tenant, event))

    async def goal_execution_mutate(self, tenant, goal_id, change):
        mutation = change(deepcopy(self.goals.get((tenant, goal_id))))
        if mutation.goal is not None:
            self.goals[(tenant, goal_id)] = deepcopy(mutation.goal)
            self.audit.extend((tenant, item) for item in mutation.audits)
            if mutation.enqueue:
                self.queue.append((tenant, goal_id))
        return mutation.result


class FakeRouter:
    default_model = "local"

    def get(self, name):
        if name != "local":
            raise ValueError("Unknown model")
        return object()


class FakeDetector:
    async def ascan(self, content, **kwargs):
        return ThreatScan(kwargs["event_type"], kwargs["location"], (), "BLOCK_ME" in content)


def app():
    result = FastAPI()
    result.include_router(router)
    result.state.redis = FakeRuntime()
    result.state.model_router = FakeRouter()
    result.state.threat_detector = FakeDetector()
    result.state.settings = Settings(
        pii_redaction=False,
        api_keys_json=json.dumps(
            {
                key: {"subject": key, "tenant_id": tenant, "role": role}
                for key, tenant, role in [
                    ("admin", "acme", "admin"),
                    ("operator", "acme", "operator"),
                    ("viewer", "acme", "viewer"),
                    ("foreign", "other", "admin"),
                ]
            }
        ),
    )
    return result


@pytest.mark.parametrize(
    "fields",
    [
        {"planning_skills": [{"skill_id": "untrusted", "revision": 1}]},
        {
            "skill_selection_rules": [
                {"skill_id": "untrusted", "revision": 1, "field": "objective", "values": ["test"]}
            ]
        },
    ],
)
async def test_new_skill_management_features_are_disabled_by_default(fields):
    application = app()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://test",
        headers={"x-api-key": "admin"},
    ) as client:
        response = await client.post(
            "/v1/agents", json={"name": "bounded", "persona": "read", **fields}
        )
    assert response.status_code == 409 and "disabled" in response.json()["detail"]
    assert application.state.redis.agent_store.records == {}


async def test_selection_validation_does_not_echo_private_config_or_invalid_unicode():
    application = app()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://test",
        headers={"x-api-key": "admin"},
    ) as client:
        body = {
            "name": "test",
            "persona": "PRIVATE-PERSONA",
            "skill_selection_rules": [
                {
                    "skill_id": "test",
                    "revision": 1,
                    "field": "objective",
                    "values": ["PRIVATE\ud800"],
                }
            ],
        }
        response = await client.post(
            "/v1/agents", content=json.dumps(body), headers={"content-type": "application/json"}
        )
    assert response.status_code == 422 and response.json() == {"detail": "Invalid Agent request"}
    assert "PRIVATE" not in response.text


def test_chief_has_no_tools_and_workers_cannot_recursively_delegate():
    chief = AgentCreate(name="Chief", persona="規劃並彙整", role="chief")
    assert chief.policy.allowed_tools == []
    with pytest.raises(ValidationError):
        AgentCreate(
            name="Chief",
            persona="規劃",
            role="chief",
            policy={"allowed_tools": ["scrape_dynamic_page"]},
        )
    with pytest.raises(ValidationError):
        AgentCreate(name="Worker", persona="執行", delegate_agent_ids=["worker-2"])
    with pytest.raises(ValidationError):
        AgentCreate(
            name="Chief",
            persona="規劃",
            role="chief",
            delegate_agent_ids=[str(i) for i in range(9)],
        )


async def test_apply_agent_uses_saved_persona_model_identity_and_narrows_policy():
    runtime = FakeRuntime()
    agent = record(
        policy=AutomationPolicy(
            allowed_tools=["scrape_dynamic_page", "browser_start"],
            allowed_domains=["example.com", "docs.example.com"],
            token_budget=10_000,
        ).model_dump()
    )
    await runtime.agent_store.create(agent)
    goal = await apply_agent(
        runtime,
        "acme",
        "worker-1",
        {
            "agent_id": "forged",
            "tenant_id": "other",
            "model": "unlisted",
            "token_budget": 9_000,
            "agent_config": {"persona": "forged"},
        },
        job_policy=AutomationPolicy(
            allowed_tools=["scrape_dynamic_page", "browser_click"],
            allowed_domains=["example.com"],
            token_budget=8_000,
        ).model_dump(),
    )
    assert goal["agent_id"] == "worker-1"
    assert goal["tenant_id"] == "acme"
    assert goal["model"] == "local"
    assert goal["agent_config"]["persona"] == agent["persona"]
    assert goal["policy"]["allowed_tools"] == ["scrape_dynamic_page"]
    assert goal["policy"]["allowed_domains"] == ["example.com"]
    assert goal["token_budget"] == 8_000
    assert (await runtime.agent_store.get("acme", "worker-1"))["policy"]["token_budget"] == 10_000


async def test_domain_intersection_never_turns_disjoint_scope_into_unrestricted():
    runtime = FakeRuntime()
    agent = record(policy=AutomationPolicy(allowed_domains=["example.com"]).model_dump())
    await runtime.agent_store.create(agent)
    with pytest.raises(HTTPException) as error:
        await apply_agent(
            runtime,
            "acme",
            "worker-1",
            {},
            job_policy=AutomationPolicy(allowed_domains=["other.test"]).model_dump(),
        )
    assert error.value.status_code == 409
    scoped = await apply_agent(
        runtime,
        "acme",
        "worker-1",
        {},
        job_policy=AutomationPolicy(allowed_domains=[]).model_dump(),
    )
    assert scoped["policy"]["allowed_domains"] == ["example.com"]
    with pytest.raises(HTTPException) as error:
        await apply_agent(runtime, "other", "worker-1", {})
    assert error.value.status_code == 404
    runtime.agent_store.records[("acme", "worker-1")]["enabled"] = False
    with pytest.raises(HTTPException) as error:
        await apply_agent(runtime, "acme", "worker-1", {})
    assert error.value.status_code == 409


async def test_agent_api_enforces_roles_allowlisted_model_and_tenant_isolation():
    application = app()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application), base_url="http://test"
    ) as client:
        body = {"name": "Reviewer", "persona": "繁體中文分析證據", "model": "local"}
        denied = await client.post("/v1/agents", json=body, headers={"x-api-key": "operator"})
        assert denied.status_code == 403
        unknown = await client.post(
            "/v1/agents", json={**body, "model": "unknown"}, headers={"x-api-key": "admin"}
        )
        assert unknown.status_code == 422
        created = await client.post("/v1/agents", json=body, headers={"x-api-key": "admin"})
        assert created.status_code == 200
        agent_id = created.json()["agent_id"]
        assert created.json()["created_by"] == "admin"
        visible = await client.get(f"/v1/agents/{agent_id}", headers={"x-api-key": "viewer"})
        assert visible.status_code == 200
        foreign = await client.get(f"/v1/agents/{agent_id}", headers={"x-api-key": "foreign"})
        assert foreign.status_code == 404
        forbidden = await client.post(
            f"/v1/agents/{agent_id}/goals",
            json={"objective": "分析"},
            headers={"x-api-key": "viewer"},
        )
        assert forbidden.status_code == 403
        submitted = await client.post(
            f"/v1/agents/{agent_id}/goals",
            json={"objective": "分析部署失敗", "model": "client-override"},
            headers={"x-api-key": "operator"},
        )
        assert submitted.status_code == 200
        assert submitted.json()["model"] == "local"
        assert submitted.json()["agent_config"]["persona"] == body["persona"]
        assert len(application.state.redis.queue) == 1


async def test_agent_goals_scan_input_and_chief_delegates_must_be_enabled_same_tenant_workers():
    application = app()
    runtime = application.state.redis
    await runtime.agent_store.create(record())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application), base_url="http://test"
    ) as client:
        blocked = await client.post(
            "/v1/agents/worker-1/goals",
            json={"objective": "BLOCK_ME"},
            headers={"x-api-key": "operator"},
        )
        assert blocked.status_code == 403
        assert runtime.queue == []
        chief = {
            "name": "Chief",
            "persona": "規劃並彙整",
            "role": "chief",
            "delegate_agent_ids": ["worker-1"],
        }
        accepted = await client.post("/v1/agents", json=chief, headers={"x-api-key": "admin"})
        assert accepted.status_code == 200
        invalid = await client.post(
            "/v1/agents",
            json={**chief, "delegate_agent_ids": ["missing"]},
            headers={"x-api-key": "admin"},
        )
        assert invalid.status_code == 422


@pytest.fixture
async def real_agents():
    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("Real Agent persistence tests require TEST_REDIS_URL")
    runtime = RedisRuntime(url)
    await runtime.ping()
    tenant = f"agents-test-{uuid4().hex}"
    try:
        yield AgentStore(runtime), tenant
    finally:
        keys = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}:*")]
        if keys:
            await runtime._client.delete(*keys)
        await runtime._client.srem("agent:goal:tenants", tenant)
        await runtime.close()


async def test_agent_store_concurrent_config_updates_have_one_cas_winner(real_agents):
    store, tenant = real_agents
    original = await store.create(record(tenant))
    results = await asyncio.gather(
        *[store.update({**original, "persona": f"Persona {index}"}, 1) for index in range(20)]
    )
    assert sum(item is not None for item in results) == 1
    current = await store.get(tenant, "worker-1")
    assert current["version"] == 2
    assert current["delegate_agent_ids"] == []
    assert await store.get(f"{tenant}-foreign", "worker-1") is None
    assert len(await store.list(tenant)) == 1
    audit = await store._client.xrange(store.runtime._tenant_key(tenant, "audit"))
    assert "Persona " not in json.dumps(audit)


async def test_agent_store_can_disable_chief_after_worker_disabled(real_agents):
    store, tenant = real_agents
    await store.create(record(tenant))
    chief = record(
        tenant,
        "chief-1",
        role="chief",
        delegate_agent_ids=["worker-1"],
        policy=AutomationPolicy(allowed_tools=[]).model_dump(),
    )
    await store.create(chief)
    await store.set_enabled(tenant, "worker-1", False)
    assert (await store.set_enabled(tenant, "chief-1", False))["enabled"] is False
    with pytest.raises(ValueError, match="enabled worker"):
        await store.set_enabled(tenant, "chief-1", True)
    assert (await store.get(tenant, "chief-1"))["enabled"] is False


async def test_agent_daily_cap_is_atomic_across_goals_and_resume_is_idempotent(real_agents):
    store, tenant = real_agents
    await store.create(record(tenant, policy=AutomationPolicy(max_runs_per_day=1).model_dump()))
    for index in (1, 2):
        await store.runtime.save_goal(
            f"goal-{index}",
            {
                "goal_id": f"goal-{index}",
                "tenant_id": tenant,
                "agent_id": "worker-1",
                "status": "running",
                "execution_owner": f"owner-{index}",
                "execution_lease_until": time.time() + 60,
            },
            tenant,
        )
    results = await asyncio.gather(
        *[
            store.admit_goal(tenant, "worker-1", f"goal-{index}", f"owner-{index}")
            for index in (1, 2)
        ]
    )
    assert results.count(True) == 1
    winner = results.index(True) + 1
    saved = await store.runtime.get_goal(f"goal-{winner}", tenant)
    assert saved["agent_admitted"] is True
    expected_day = datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d")
    assert saved["agent_admitted_day"] == expected_day
    assert datetime.fromisoformat(saved["agent_admitted_at"]).tzinfo is not None
    saved["execution_owner"] = "recovered-owner"
    saved["execution_lease_until"] = time.time() + 60
    await store.runtime.save_goal(f"goal-{winner}", saved, tenant)
    assert await store.admit_goal(tenant, "worker-1", f"goal-{winner}", "recovered-owner")
    counter = store.runtime._tenant_key(tenant, f"agents:daily-runs:worker-1:{expected_day}")
    assert int(await store._client.get(counter)) == 1
    assert 3 * 86400 - 5 <= await store._client.ttl(counter) <= 3 * 86400


async def test_agent_admission_rejects_wrong_tenant_owner_expired_lease_and_disabled(real_agents):
    store, tenant = real_agents
    await store.create(record(tenant))
    initial = {
        "goal_id": "goal-1",
        "tenant_id": tenant,
        "agent_id": "worker-1",
        "status": "running",
        "execution_owner": "live-owner",
        "execution_lease_until": time.time() + 60,
    }
    await store.runtime.save_goal("goal-1", initial, tenant)
    assert not await store.admit_goal(tenant, "worker-1", "goal-1", "stale-owner")
    assert not await store.admit_goal(f"{tenant}-foreign", "worker-1", "goal-1", "live-owner")
    expired = {**initial, "execution_lease_until": time.time() - 1}
    await store.runtime.save_goal("goal-1", expired, tenant)
    assert not await store.admit_goal(tenant, "worker-1", "goal-1", "live-owner")
    await store.runtime.save_goal("goal-1", initial, tenant)
    await store.set_enabled(tenant, "worker-1", False)
    assert not await store.admit_goal(tenant, "worker-1", "goal-1", "live-owner")
    counters = [
        key
        async for key in store._client.scan_iter(
            store.runtime._tenant_key(tenant, "agents:daily-runs:*")
        )
    ]
    assert counters == []
    assert not (await store.runtime.get_goal("goal-1", tenant)).get("agent_admitted")
