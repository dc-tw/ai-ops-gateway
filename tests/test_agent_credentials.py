"""Real-Redis Agent credential lifecycle and route-bound identity tests."""

from __future__ import annotations

import asyncio
import json
import os
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI

from app.agent_credentials import router as credential_router
from app.agents import router as agent_router
from app.auth import router as auth_router
from app.core.agent_credentials import AgentCredentialStore
from app.core.config import Settings
from app.core.durable_outbox import maintenance_key, outbox_key
from app.core.redis_runtime import RedisRuntime
from app.core.threat_detection import ThreatScan
from app.goals import router as goal_router


class ModelRouter:
    default_model = "local"

    @staticmethod
    def get(name: str):
        if name != "local":
            raise ValueError("Unknown model")
        return object()


class Detector:
    @staticmethod
    async def ascan(content, *, event_type, location, fields):
        return ThreatScan(event_type, location, (), False)


@pytest.fixture
async def credential_env():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Agent credential integration requires TEST_REDIS_URL")
    runtime = RedisRuntime(url)
    await runtime.ping()
    tenant = "credential-" + uuid4().hex
    app = FastAPI()
    app.include_router(auth_router)
    app.include_router(agent_router)
    app.include_router(credential_router)
    app.include_router(goal_router)
    app.state.redis = runtime
    app.state.model_router = ModelRouter()
    app.state.threat_detector = Detector()
    app.state.settings = Settings(
        _env_file=None,
        app_env="production",
        api_keys_json=json.dumps(
            {
                "admin": {
                    "subject": "security-admin",
                    "tenant_id": tenant,
                    "role": "admin",
                },
                "foreign": {
                    "subject": "foreign-admin",
                    "tenant_id": tenant + "-other",
                    "role": "admin",
                },
            }
        ),
    )
    value = SimpleNamespace(app=app, runtime=runtime, tenant=tenant, url=url)
    try:
        yield value
    finally:
        indexes = [
            key async for key in runtime._client.scan_iter(f"tenant:{tenant}:agents:credentials:*")
        ]
        credential_ids: set[str] = set()
        for index in indexes:
            credential_ids.update(await runtime._client.smembers(index))
        if credential_ids:
            await runtime._client.delete(
                *(f"gateway:agent-credential:{item}" for item in credential_ids)
            )
        owned = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}:*")]
        if owned:
            await runtime._client.delete(*owned)
        await runtime.close()


async def _create_agent(client: httpx.AsyncClient, name: str = "worker") -> dict:
    response = await client.post(
        "/v1/agents",
        headers={"x-api-key": "admin"},
        json={"name": name, "persona": "只執行已核可且可稽核的工作。"},
    )
    assert response.status_code == 200, response.text
    return response.json()


async def test_issue_rotate_revoke_and_route_scope(credential_env):
    transport = httpx.ASGITransport(app=credential_env.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        agent = await _create_agent(client)
        other = await _create_agent(client, "other")
        issue = await client.post(
            f"/v1/agents/{agent['agent_id']}/credentials",
            headers={"x-api-key": "admin"},
            json={"label": "runtime", "expires_in_seconds": 3600},
        )
        assert issue.status_code == 201, issue.text
        issued = issue.json()
        token = issued["token"]
        credential_id = issued["credential"]["credential_id"]
        assert issued["token_visible_once"] is True
        assert issued["credential"]["status"] == "active"
        assert "secret" not in json.dumps(issued["credential"]).lower()

        stored = await credential_env.runtime._client.get(
            f"gateway:agent-credential:{credential_id}"
        )
        assert token not in stored
        assert "secret_sha256" in stored

        auth = {"x-agent-key": token}
        session = await client.get("/auth/session", headers=auth)
        own = await client.get(f"/v1/agents/{agent['agent_id']}", headers=auth)
        foreign_agent = await client.get(f"/v1/agents/{other['agent_id']}", headers=auth)
        all_agents = await client.get("/v1/agents", headers=auth)
        approval = await client.post(
            "/v1/goals/not-owned/approval",
            headers=auth,
            json={"approval_id": "one", "approved": True},
        )
        assert session.status_code == 200
        assert session.json()["actor_type"] == "agent"
        assert own.status_code == 200
        assert foreign_agent.status_code == 403
        assert all_agents.status_code == 403
        assert approval.status_code == 403

        admin_goal = await client.post(
            "/v1/goals",
            headers={"x-api-key": "admin"},
            json={"objective": "admin-only goal", "model": "local"},
        )
        own_goal = await client.post(
            f"/v1/agents/{agent['agent_id']}/goals",
            headers=auth,
            json={"objective": "agent-owned goal", "model": "local"},
        )
        assert admin_goal.status_code == 200, admin_goal.text
        assert own_goal.status_code == 200, own_goal.text
        assert own_goal.json()["created_by_type"] == "agent"
        goals = await client.get("/v1/goals", headers=auth)
        assert [item["goal_id"] for item in goals.json()["goals"]] == [
            own_goal.json()["goal_id"]
        ]
        assert (
            await client.get(f"/v1/goals/{own_goal.json()['goal_id']}", headers=auth)
        ).status_code == 200
        assert (
            await client.get(f"/v1/goals/{admin_goal.json()['goal_id']}", headers=auth)
        ).status_code == 404

        duplicate = await client.post(
            f"/v1/agents/{agent['agent_id']}/credentials",
            headers={"x-api-key": "admin"},
            json={"label": "duplicate"},
        )
        assert duplicate.status_code == 409

        rotated = await client.post(
            f"/v1/agents/{agent['agent_id']}/credentials/rotate",
            headers={"x-api-key": "admin"},
            json={"label": "rotated", "expires_in_seconds": 7200},
        )
        assert rotated.status_code == 201, rotated.text
        new_token = rotated.json()["token"]
        new_id = rotated.json()["credential"]["credential_id"]
        assert (
            await client.get(f"/v1/agents/{agent['agent_id']}", headers=auth)
        ).status_code == 401
        assert (
            await client.get("/auth/session", headers={"x-agent-key": new_token})
        ).status_code == 200

        listing = await client.get(
            f"/v1/agents/{agent['agent_id']}/credentials",
            headers={"x-api-key": "admin"},
        )
        assert listing.status_code == 200
        by_id = {row["credential_id"]: row for row in listing.json()["credentials"]}
        assert by_id[credential_id]["status"] == "revoked"
        assert by_id[new_id]["status"] == "active"
        assert token not in listing.text and new_token not in listing.text
        assert "sha256" not in listing.text
        audits = await credential_env.runtime._client.xrange(
            credential_env.runtime._tenant_key(
                credential_env.tenant, "security:credential-audit"
            )
        )
        encoded_audit = json.dumps(audits)
        assert token not in encoded_audit and new_token not in encoded_audit
        assert "secret_sha256" not in encoded_audit
        assert {"agent.credential.issue", "agent.credential.rotate"} <= {
            json.loads(fields["payload"])["action"] for _, fields in audits
        }

        revoked = await client.delete(
            f"/v1/agents/{agent['agent_id']}/credentials/{new_id}",
            headers={"x-api-key": "admin"},
        )
        assert revoked.status_code == 200
        assert revoked.json()["status"] == "revoked"
        assert (
            await client.get(
                f"/v1/agents/{agent['agent_id']}", headers={"x-agent-key": new_token}
            )
        ).status_code == 401


async def test_disable_is_immediate_and_tenant_boundary_is_hidden(credential_env):
    transport = httpx.ASGITransport(app=credential_env.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        agent = await _create_agent(client)
        issue = await client.post(
            f"/v1/agents/{agent['agent_id']}/credentials",
            headers={"x-api-key": "admin"},
            json={},
        )
        token = issue.json()["token"]
        assert (
            await client.get("/auth/session", headers={"x-agent-key": token})
        ).status_code == 200

        disabled = await client.post(
            f"/v1/agents/{agent['agent_id']}/enabled",
            headers={"x-api-key": "admin"},
            json={"enabled": False},
        )
        assert disabled.status_code == 200
        assert (
            await client.get(
                f"/v1/agents/{agent['agent_id']}", headers={"x-agent-key": token}
            )
        ).status_code == 401

        hidden = await client.get(
            f"/v1/agents/{agent['agent_id']}/credentials",
            headers={"x-api-key": "foreign"},
        )
        assert hidden.status_code == 404


async def test_expiry_and_ambiguous_auth_fail_closed(credential_env):
    transport = httpx.ASGITransport(app=credential_env.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        agent = await _create_agent(client)
        issue = await client.post(
            f"/v1/agents/{agent['agent_id']}/credentials",
            headers={"x-api-key": "admin"},
            json={},
        )
        token = issue.json()["token"]
        credential_id = issue.json()["credential"]["credential_id"]
        ambiguous = await client.get(
            f"/v1/agents/{agent['agent_id']}",
            headers={"x-agent-key": token, "x-api-key": "admin"},
        )
        assert ambiguous.status_code == 400

        key = f"gateway:agent-credential:{credential_id}"
        record = json.loads(await credential_env.runtime._client.get(key))
        record["expires_at_epoch"] = 0
        await credential_env.runtime._client.set(key, json.dumps(record))
        expired = await client.get(
            f"/v1/agents/{agent['agent_id']}", headers={"x-agent-key": token}
        )
        assert expired.status_code == 401


async def test_repeated_rotation_prunes_only_inactive_secret_metadata(credential_env):
    transport = httpx.ASGITransport(app=credential_env.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        agent = await _create_agent(client)
        response = await client.post(
            f"/v1/agents/{agent['agent_id']}/credentials",
            headers={"x-api-key": "admin"},
            json={},
        )
        latest = response.json()["token"]
        for number in range(12):
            response = await client.post(
                f"/v1/agents/{agent['agent_id']}/credentials/rotate",
                headers={"x-api-key": "admin"},
                json={"label": f"rotation-{number}"},
            )
            assert response.status_code == 201
            latest = response.json()["token"]
        listing = await client.get(
            f"/v1/agents/{agent['agent_id']}/credentials",
            headers={"x-api-key": "admin"},
        )
        assert len(listing.json()["credentials"]) == 10
        assert sum(row["status"] == "active" for row in listing.json()["credentials"]) == 1
        assert (
            await client.get("/auth/session", headers={"x-agent-key": latest})
        ).status_code == 200


async def test_concurrent_initial_issue_has_one_winner(credential_env):
    transport = httpx.ASGITransport(app=credential_env.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        agent = await _create_agent(client)
    store = AgentCredentialStore(credential_env.runtime)

    async def issue(number):
        return await store.issue(
            credential_env.tenant,
            agent["agent_id"],
            label=f"concurrent-{number}",
            expires_in_seconds=3600,
            issued_by="security-admin",
        )

    results = await asyncio.gather(issue(1), issue(2), return_exceptions=True)
    assert sum(isinstance(item, tuple) for item in results) == 1
    assert sum(isinstance(item, FileExistsError) for item in results) == 1


async def test_revocation_authority_does_not_depend_on_postgres_history_fence(
    credential_env,
):
    transport = httpx.ASGITransport(app=credential_env.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        agent = await _create_agent(client)
    source = "credential-" + uuid4().hex
    durable = RedisRuntime(credential_env.url, durable_source_id=source)
    await durable._client.set(maintenance_key(source), "maintenance")
    try:
        store = AgentCredentialStore(durable)
        credential, token = await store.issue(
            credential_env.tenant,
            agent["agent_id"],
            label="maintenance-safe",
            expires_in_seconds=3600,
            issued_by="security-admin",
        )
        assert await store.resolve(token) is not None
        revoked = await store.revoke(
            credential_env.tenant,
            agent["agent_id"],
            credential["credential_id"],
            revoked_by="security-admin",
        )
        assert revoked["status"] == "revoked"
        assert await store.resolve(token) is None
        assert not await durable._client.exists(outbox_key(source))
    finally:
        await durable._client.delete(maintenance_key(source), outbox_key(source))
        await durable.close()
