"""Real-Redis MCP/ATR policy reference publication, binding, and revocation."""

from __future__ import annotations

import json
import os
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from app.agents import apply_agent
from app.agents import router as agent_router
from app.core.config import Settings
from app.core.durable_outbox import maintenance_key, outbox_key
from app.core.redis_runtime import RedisRuntime
from app.core.security_policies import SecurityPolicyStore, guard_agent_security_policies
from app.core.threat_detection import ThreatDetector
from app.security_policies import router as policy_router


class ModelRouter:
    default_model = "local"

    @staticmethod
    def get(name: str):
        if name != "local":
            raise ValueError("Unknown model")
        return object()


@pytest.fixture
async def policy_env():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Security policy integration requires TEST_REDIS_URL")
    runtime = RedisRuntime(url)
    await runtime.ping()
    tenant = "policy-" + uuid4().hex
    settings = Settings(
        _env_file=None,
        app_env="production",
        agent_tool_transport="mcp",
        atr_blocking=True,
        api_keys_json=json.dumps(
            {
                "admin": {"subject": "admin", "tenant_id": tenant, "role": "admin"},
                "foreign": {
                    "subject": "admin",
                    "tenant_id": tenant + "-other",
                    "role": "admin",
                },
            }
        ),
    )
    app = FastAPI()
    app.include_router(policy_router)
    app.include_router(agent_router)
    app.state.redis = runtime
    app.state.settings = settings
    app.state.threat_detector = ThreatDetector.from_settings(settings)
    app.state.model_router = ModelRouter()
    value = SimpleNamespace(
        app=app,
        runtime=runtime,
        tenant=tenant,
        settings=settings,
        detector=app.state.threat_detector,
        url=url,
    )
    try:
        yield value
    finally:
        owned = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}:*")]
        if owned:
            await runtime._client.delete(*owned)
        await runtime.close()


async def _post(client, path, body):
    return await client.post(path, headers={"x-api-key": "admin"}, json=body)


async def test_publish_bind_guard_and_revoke_server_policies(policy_env):
    transport = httpx.ASGITransport(app=policy_env.app)
    active_rule = next(
        rule.id
        for rule in policy_env.detector.engine.rules
        if rule.status not in {"draft", "deprecated"}
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        mcp = await _post(
            client,
            "/v1/security-policies",
            {
                "name": "production MCP boundary",
                "config": {
                    "kind": "mcp",
                    "transport_ref": "configured-mcp",
                    "allowed_tools": ["scrape_dynamic_page", "browser_start"],
                    "allowed_domains": ["example.com"],
                },
            },
        )
        atr = await _post(
            client,
            "/v1/security-policies",
            {
                "name": "production ATR boundary",
                "config": {
                    "kind": "atr",
                    "mode": "blocking",
                    "minimum_severity": "medium",
                    "block_severity": "high",
                    "required_rule_ids": [active_rule],
                },
            },
        )
        assert mcp.status_code == 201, mcp.text
        assert atr.status_code == 201, atr.text
        refs = {
            "mcp_policy_ref": {
                "policy_id": mcp.json()["policy_id"],
                "revision": 1,
            },
            "atr_policy_ref": {
                "policy_id": atr.json()["policy_id"],
                "revision": 1,
            },
        }
        agent = await _post(
            client,
            "/v1/agents",
            {
                "name": "bounded worker",
                "persona": "只依 server-approved policy 工作。",
                "policy": {
                    "allowed_tools": ["scrape_dynamic_page"],
                    "allowed_domains": ["example.com"],
                },
                **refs,
            },
        )
        assert agent.status_code == 200, agent.text
        assert len(agent.json()["security_policy_snapshots"]) == 2

        def keys(value):
            if isinstance(value, dict):
                return set(value) | set().union(*(keys(item) for item in value.values()))
            if isinstance(value, list):
                return set().union(*(keys(item) for item in value))
            return set()

        assert not {"endpoint", "secret", "token", "credential"} & keys(agent.json())

        goal = await apply_agent(
            policy_env.runtime,
            policy_env.tenant,
            agent.json()["agent_id"],
            {"goal_id": "policy-goal", "token_budget": 1000},
        )
        await guard_agent_security_policies(
            policy_env.runtime,
            policy_env.tenant,
            goal["agent_config"],
            policy_env.settings,
            policy_env.detector,
        )

        original_fingerprint = policy_env.detector.ruleset_fingerprint
        policy_env.detector.ruleset_fingerprint = "changed-outside-policy"
        with pytest.raises(PermissionError, match="differs"):
            await guard_agent_security_policies(
                policy_env.runtime,
                policy_env.tenant,
                goal["agent_config"],
                policy_env.settings,
                policy_env.detector,
            )
        policy_env.detector.ruleset_fingerprint = original_fingerprint

        disabled = await _post(
            client,
            f"/v1/security-policies/{atr.json()['policy_id']}/enabled",
            {"enabled": False},
        )
        assert disabled.status_code == 200 and disabled.json()["enabled"] is False
        with pytest.raises(HTTPException) as error:
            await apply_agent(
                policy_env.runtime,
                policy_env.tenant,
                agent.json()["agent_id"],
                {"goal_id": "revoked"},
            )
        assert error.value.status_code == 409

        hidden = await client.get(
            f"/v1/security-policies/{mcp.json()['policy_id']}",
            headers={"x-api-key": "foreign"},
        )
        assert hidden.status_code == 404


async def test_policy_schema_and_server_posture_fail_closed(policy_env):
    transport = httpx.ASGITransport(app=policy_env.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        secret = await _post(
            client,
            "/v1/security-policies",
            {
                "name": "must reject secret",
                "config": {
                    "kind": "mcp",
                    "allowed_tools": ["scrape_dynamic_page"],
                    "endpoint": "https://secret.invalid",
                    "token": "DO-NOT-STORE",
                },
            },
        )
        assert secret.status_code == 422
        assert "DO-NOT-STORE" not in secret.text

        policy_env.settings.agent_tool_transport = "local"
        unavailable_mcp = await _post(
            client,
            "/v1/security-policies",
            {
                "name": "no MCP",
                "config": {"kind": "mcp", "allowed_tools": ["scrape_dynamic_page"]},
            },
        )
        assert unavailable_mcp.status_code == 409
        policy_env.settings.agent_tool_transport = "mcp"

        stable_kind = await _post(
            client,
            "/v1/security-policies",
            {
                "name": "immutable kind",
                "config": {"kind": "mcp", "allowed_tools": ["scrape_dynamic_page"]},
            },
        )
        changed_kind = await client.put(
            f"/v1/security-policies/{stable_kind.json()['policy_id']}",
            headers={"x-api-key": "admin"},
            json={
                "name": "must remain MCP",
                "expected_version": 1,
                "config": {"kind": "atr", "mode": "detection_only"},
            },
        )
        assert changed_kind.status_code == 409

        policy_env.detector.blocking = False
        weak_atr = await _post(
            client,
            "/v1/security-policies",
            {
                "name": "blocking required",
                "config": {"kind": "atr", "mode": "blocking"},
            },
        )
        assert weak_atr.status_code == 409


async def test_mcp_reference_cannot_expand_agent_tools_or_domains(policy_env):
    transport = httpx.ASGITransport(app=policy_env.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        policy = await _post(
            client,
            "/v1/security-policies",
            {
                "name": "narrow MCP",
                "config": {
                    "kind": "mcp",
                    "allowed_tools": ["scrape_dynamic_page"],
                    "allowed_domains": ["example.com"],
                },
            },
        )
        reference = {"policy_id": policy.json()["policy_id"], "revision": 1}
        for body in (
            {
                "allowed_tools": ["browser_click"],
                "allowed_domains": ["example.com"],
            },
            {"allowed_tools": ["scrape_dynamic_page"], "allowed_domains": []},
            {
                "allowed_tools": ["scrape_dynamic_page"],
                "allowed_domains": ["other.example"],
            },
        ):
            response = await _post(
                client,
                "/v1/agents",
                {
                    "name": "escape attempt",
                    "persona": "bounded",
                    "policy": body,
                    "mcp_policy_ref": reference,
                },
            )
            assert response.status_code == 422


async def test_policy_revocation_does_not_depend_on_history_maintenance(policy_env):
    transport = httpx.ASGITransport(app=policy_env.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        policy = await _post(
            client,
            "/v1/security-policies",
            {
                "name": "revocable MCP",
                "config": {"kind": "mcp", "allowed_tools": ["scrape_dynamic_page"]},
            },
        )
    source = "policy-" + uuid4().hex
    durable = RedisRuntime(policy_env.url, durable_source_id=source)
    await durable._client.set(maintenance_key(source), "maintenance")
    try:
        disabled = await SecurityPolicyStore(durable).set_enabled(
            policy_env.tenant, policy.json()["policy_id"], False, "admin"
        )
        assert disabled["enabled"] is False
        assert not await durable._client.exists(outbox_key(source))
    finally:
        await durable._client.delete(maintenance_key(source), outbox_key(source))
        await durable.close()
