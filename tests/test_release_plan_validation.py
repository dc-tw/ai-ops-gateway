"""HTTP validation rejects private or malformed drafts before any planning writes."""

import json
import os
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from app.agents import AgentStore
from app.automations import AutomationPolicy
from app.core.config import Settings
from app.core.planning_store import MAX_PLAN_BYTES
from app.core.redis_runtime import RedisRuntime
from app.core.skill_store import SkillContent, SkillStore
from app.core.threat_detection import ThreatScan
from app.llm.client import ModelRouter
from app.main import create_app

PRIVATE = "PRIVATE-RELEASE-PLAN-INPUT"


class Detector:
    async def ascan(self, text, **kwargs):
        return ThreatScan(kwargs["event_type"], kwargs["location"], (), False)


def plan(**changes):
    return {
        "objective": "Review the supplied evidence",
        "context": "Only the supplied evidence is in scope.",
        "steps": [{"query": "Summarize the evidence", "target_url": None}],
        "assignments": [],
        "max_retries": 2,
        "max_tool_rounds": 8,
        "step_deadline_seconds": 300.0,
        **changes,
    }


def request_bytes(body):
    # Escaped raw JSON can carry surrogate code points which httpx.json rejects
    # before the server sees them. Exercise the actual request parser instead.
    return json.dumps(body, ensure_ascii=True).encode("ascii")


async def test_planning_validation_errors_are_safe_and_scoped_without_redis():
    settings = Settings(api_key="validation-test")
    app = create_app(settings=settings)
    app.state.settings, app.state.redis = settings, None
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://test"
    ) as client:
        for body in (
            {"expected_version": 1, "plan": plan(objective=PRIVATE + "\ud800")},
            {"expected_version": 1, "plan": plan(), PRIVATE + "\udfff": PRIVATE},
        ):
            response = await client.post(
                "/v1/planning-sessions/example/plans",
                content=request_bytes(body),
                headers={"content-type": "application/json"},
            )
            assert response.status_code == 422
            assert response.json() == {"detail": "Invalid planning request"}
            assert PRIVATE not in response.text
        legacy = await client.post("/v1/agent/stream", json={"query": []})
        assert legacy.status_code == 422
        assert isinstance(legacy.json()["detail"], list)


@pytest.fixture
async def environment():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Release validation checks require dedicated TEST_REDIS_URL")
    runtime = RedisRuntime(url)
    await runtime.ping()
    tenant, agent_id = f"release-plan-{uuid4().hex}", str(uuid4())
    settings = Settings(
        api_keys_json=json.dumps(
            {"operator": {"subject": "operator", "tenant_id": tenant, "role": "operator"}}
        ),
        pii_redaction=False,
        tenant_requests_per_minute=10_000,
    )
    app = create_app(settings=settings, redis_runtime=runtime)
    app.state.settings, app.state.redis = settings, runtime
    app.state.model_router, app.state.threat_detector = ModelRouter(settings), Detector()
    now = datetime.now(UTC).isoformat()
    await AgentStore(runtime).create(
        {
            "agent_id": agent_id,
            "tenant_id": tenant,
            "name": "Release validation",
            "description": "Provided evidence only",
            "persona": "Preserve the evidence and approval boundaries.",
            "role": "worker",
            "model": settings.llm_model,
            "enabled": True,
            "version": 1,
            "delegate_agent_ids": [],
            "skills": [],
            "policy": AutomationPolicy(allowed_tools=[]).model_dump(),
            "created_by": "admin",
            "created_at": now,
            "updated_at": now,
        }
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://test",
            headers={"x-api-key": "operator"},
        ) as client:
            response = await client.post(f"/v1/agents/{agent_id}/planning-sessions")
            assert response.status_code == 200, response.text
            session = response.json()
            token_key = runtime._tenant_key(tenant, "quota:tokens:test-day")
            await runtime._client.set(token_key, 12345)
            yield SimpleNamespace(
                runtime=runtime,
                tenant=tenant,
                client=client,
                session=session,
                path=f"/v1/planning-sessions/{session['session_id']}",
                app=app,
                settings=settings,
                agent_id=agent_id,
            )
    finally:
        keys = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}:*")]
        if keys:
            await runtime._client.delete(*keys)
        await runtime.close()


async def tenant_state(env):
    result = {}
    client = env.runtime._client
    for key in sorted([key async for key in client.scan_iter(f"tenant:{env.tenant}:*")]):
        kind = await client.type(key)
        if kind == "string":
            value = await client.get(key)
        elif kind == "stream":
            value = await client.xrange(key)
        elif kind == "set":
            value = sorted(await client.smembers(key))
        else:
            pytest.fail(f"Unexpected planning persistence type: {kind}")
        result[key] = value
    return result


@pytest.mark.parametrize("policy", ["pii", "content", "atr"])
async def test_refresh_rechecks_current_input_policy_without_rewriting_history(environment, policy):
    env = environment
    saved = await env.client.post(
        env.path + "/plans",
        json={"expected_version": 1, "plan": plan(context="Contact release.person@example.com")},
    )
    assert saved.status_code == 200
    current = saved.json()
    original = (await env.client.get(env.path + "/plans/1")).json()
    if policy == "pii":
        env.settings.pii_redaction = True
        expected = 409
    elif policy == "content":
        env.settings.content_blocked_patterns = ["Review the supplied evidence"]
        expected = 403
    else:

        class BlockedDetector:
            async def ascan(self, text, **kwargs):
                return ThreatScan(kwargs["event_type"], kwargs["location"], (), True)

        env.app.state.threat_detector = BlockedDetector()
        expected = 403
    response = await env.client.post(
        env.path + "/refresh", json={"expected_version": current["version"]}
    )
    assert response.status_code == expected
    assert (await env.client.get(env.path + "/plans/1")).json() == original
    persisted = (await env.client.get(env.path)).json()
    assert persisted["version"] == current["version"] and persisted["plan_revision"] == 1


@pytest.mark.parametrize("source", ["agent", "skill"])
async def test_same_execute_mode_cannot_bypass_changed_or_revoked_configuration(
    environment, source
):
    env = environment
    session = env.session
    skill = None
    if source == "skill":
        skill = await SkillStore(env.runtime).create(
            env.tenant,
            SkillContent(
                name="release-evidence",
                display_name="Release evidence",
                description="Summarize supplied evidence.",
                instructions="Preserve uncertainty and the current approval boundary.",
            ).model_dump(),
            "admin",
        )
        agent = await AgentStore(env.runtime).get(env.tenant, env.agent_id)
        agent["skills"] = [{"skill_id": skill["skill_id"], "revision": 1}]
        await AgentStore(env.runtime).update(agent, agent["version"])
        session = (await env.client.post(f"/v1/agents/{env.agent_id}/planning-sessions")).json()
    path = f"/v1/planning-sessions/{session['session_id']}"
    saved = await env.client.post(path + "/plans", json={"expected_version": 1, "plan": plan()})
    assert saved.status_code == 200
    accepted = await env.client.post(
        path + "/mode", json={"expected_version": 2, "mode": "execute", "plan_revision": 1}
    )
    assert accepted.status_code == 200
    if source == "agent":
        await AgentStore(env.runtime).set_enabled(env.tenant, env.agent_id, False)
    else:
        await SkillStore(env.runtime).set_enabled(env.tenant, skill["skill_id"], False, 1, "admin")
    denied = await env.client.post(
        path + "/mode", json={"expected_version": 3, "mode": "execute", "plan_revision": 1}
    )
    assert denied.status_code == 409
    unchanged = (await env.client.get(path)).json()
    assert unchanged["mode"] == "execute" and unchanged["version"] == 3
    assert unchanged["configuration_status"] == "stale"


@pytest.mark.parametrize(
    "body",
    [
        {"expected_version": 1, "plan": plan(objective=PRIVATE + "\ud800")},
        {"expected_version": 1, "plan": plan(context=PRIVATE + "\udfff")},
        {"expected_version": 1, "plan": plan(steps=[{"query": PRIVATE + "\ud800"}])},
        {"expected_version": 1, "plan": plan() | {PRIVATE + "\ud800": PRIVATE}},
        {"expected_version": 1, "plan": plan(), PRIVATE + "\udfff": PRIVATE},
        {"expected_version": PRIVATE, "plan": plan(context=PRIVATE)},
        {"expected_version": 1, "plan": plan(step_deadline_seconds=float("nan"), context=PRIVATE)},
    ],
    ids=["objective", "context", "step", "extra-plan", "extra-body", "version", "nan"],
)
async def test_invalid_json_values_never_leak_or_mutate_planning_state(environment, body):
    env = environment
    before = await tenant_state(env)
    response = await env.client.post(
        env.path + "/plans",
        content=request_bytes(body),
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 422, response.text
    assert response.json() == {"detail": "Invalid planning request"}
    assert PRIVATE not in response.text
    # This also covers request quota: validation occurs before authentication,
    # rate limiting, ATR findings, session/revision writes and audit events.
    assert await tenant_state(env) == before
    assert await env.runtime.list_goal_ids(env.tenant) == []


def utf8_boundary_plan():
    value = plan(
        objective="中" * 8_000,
        context="😀" * 8_000,
        steps=[
            {"query": "q" * 8_000, "target_url": None},
            {"query": "", "target_url": None},
        ],
    )
    overhead = len(json.dumps(value, ensure_ascii=False).encode("utf-8"))
    remaining = MAX_PLAN_BYTES - overhead
    assert 1 <= remaining < 8_000
    value["steps"][1]["query"] = "p" * remaining
    assert len(json.dumps(value, ensure_ascii=False).encode("utf-8")) == MAX_PLAN_BYTES
    return value


async def test_multibyte_and_emoji_exact_utf8_limit_is_accepted(environment):
    env = environment
    value = utf8_boundary_plan()
    response = await env.client.post(
        env.path + "/plans", json={"expected_version": 1, "plan": value}
    )
    assert response.status_code == 200, response.text
    revision = await env.client.get(env.path + "/plans/1")
    assert revision.status_code == 200 and revision.json()["plan"] == value


@pytest.mark.parametrize("extra", ["x", "中", "😀"], ids=["one-byte", "three-bytes", "four-bytes"])
async def test_over_utf8_limit_never_mutates_history_audit_or_quotas(environment, extra):
    env = environment
    accepted = await env.client.post(
        env.path + "/plans", json={"expected_version": 1, "plan": plan()}
    )
    assert accepted.status_code == 200
    before = await tenant_state(env)
    value = utf8_boundary_plan()
    value["steps"][1]["query"] += extra
    response = await env.client.post(
        env.path + "/plans", json={"expected_version": 2, "plan": value}
    )
    assert response.status_code == 422, response.text
    assert response.json() == {"detail": "Invalid planning request"}
    assert await tenant_state(env) == before
    assert (await env.client.get(env.path + "/plans")).json()["plans"][0]["plan"] == plan()
