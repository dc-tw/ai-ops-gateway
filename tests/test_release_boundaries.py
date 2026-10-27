"""Release boundaries: supported planning stays separate from future execution features."""

import json
import os
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from app.agents import AgentCreate, AgentStore
from app.automations import AutomationPolicy
from app.core.config import Settings
from app.core.redis_runtime import RedisRuntime
from app.core.threat_detection import ThreatScan
from app.llm.client import ModelRouter
from app.main import create_app
from app.planning import (
    PlanContent,
    PlanningInput,
    PlanningMode,
    PlanningStep,
    WorkerAssignment,
)
from app.skills import BuiltinInstall, SkillCreate


def draft(objective="Prepare a source-based report"):
    return {
        "objective": objective,
        "steps": [{"query": "List source facts and unresolved questions"}],
    }


def test_openapi_advertises_package_review_but_not_forecasts_or_execution_bridge():
    document = create_app(settings=Settings(_env_file=None)).openapi()
    paths = document["paths"]
    assert "/v1/planning-sessions/{session_id}/mode" in paths
    assert "/v1/agents/{agent_id}/goals" in paths
    assert "/v1/skills/{skill_id}/export" in paths
    assert "/v1/skill-imports/upload" in paths
    assert "/v1/skill-imports/github" in paths
    assert "/v1/skill-imports/{import_id}/confirm" in paths
    for path in paths:
        segments = path.strip("/").split("/")
        assert "forecasts" not in segments
        assert "forecast" not in segments
    for unsupported in (
        "/v1/planning-sessions/{session_id}/execute",
        "/v1/planning-sessions/{session_id}/goals",
        "/v1/planning-sessions/{session_id}/run",
        "/v1/skills/import",
        "/v1/skills/{skill_id}/scripts",
        "/v1/skills/{skill_id}/execute",
    ):
        assert unsupported not in paths


@pytest.mark.parametrize(
    "model,payload",
    [
        (AgentCreate, {"name": "Reviewer", "persona": "Review the supplied sources"}),
        (PlanningInput, {}),
        (PlanContent, draft()),
        (PlanningStep, {"query": "Review source facts"}),
        (
            WorkerAssignment,
            {"agent_id": "worker-one", "objective": "Review facts", "steps": []},
        ),
        (PlanningMode, {"expected_version": 1, "mode": "plan"}),
        (BuiltinInstall, {"name": "source-summary"}),
        (
            SkillCreate,
            {
                "name": "release-review",
                "display_name": "Release review",
                "description": "Review supplied release evidence",
                "instructions": "Use available facts and identify uncertainty.",
            },
        ),
    ],
    ids=lambda value: value.__name__ if isinstance(value, type) else None,
)
@pytest.mark.parametrize(
    "field,value",
    [
        ("planning_skills", [{"skill_id": "forged-planning-skill", "revision": 1}]),
        ("execution_modes", ["plan", "execute"]),
        ("scripts", [{"path": "run.sh", "content": "perform an external operation"}]),
        ("import", {"path": "/untrusted/SKILL.md"}),
    ],
)
def test_skill_and_execution_fields_have_explicit_schema_boundaries(model, payload, field, value):
    # Validate the baseline first, so rejection cannot be caused by missing input.
    model.model_validate(payload)
    assert model.model_json_schema()["additionalProperties"] is False
    if model is AgentCreate and field == "planning_skills":
        # M3 now supports this only on administrator-managed Agent config.
        # Default-off HTTP gating and live binding checks are tested separately.
        parsed = model.model_validate(payload | {field: value})
        assert [item.model_dump() for item in parsed.planning_skills] == value
        return
    with pytest.raises(ValidationError) as error:
        model.model_validate(payload | {field: value})
    if model is SkillCreate and field == "execution_modes":
        assert any(entry["type"] == "value_error" for entry in error.value.errors())
        return  # v1 cannot gain planning permission by adding a modes field.
    assert any(
        entry["type"] == "extra_forbidden" and entry["loc"] == (field,)
        for entry in error.value.errors()
    )


class Detector:
    async def ascan(self, text, **kwargs):
        return ThreatScan(kwargs["event_type"], kwargs["location"], (), False)


@pytest.fixture
async def environment():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Release isolation checks require a dedicated TEST_REDIS_URL")
    runtime, tenant = RedisRuntime(url), f"release-{uuid4().hex}"
    await runtime.ping()
    settings = Settings(
        api_keys_json=json.dumps(
            {"operator": {"subject": "operator", "tenant_id": tenant, "role": "operator"}}
        ),
        pii_redaction=False,
        tenant_requests_per_minute=10_000,
    )
    app = create_app(settings=settings, redis_runtime=runtime)
    app.state.settings, app.state.redis = settings, runtime
    app.state.model_router = ModelRouter(settings)
    app.state.threat_detector = Detector()
    now, agent_id = datetime.now(UTC).isoformat(), str(uuid4())
    agent = await AgentStore(runtime).create(
        {
            "agent_id": agent_id,
            "tenant_id": tenant,
            "name": "Release boundary reviewer",
            "description": "Reviews explicitly supplied sources",
            "persona": "Use evidence and preserve source limitations.",
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
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"x-api-key": "operator"},
    ) as client:
        yield SimpleNamespace(
            runtime=runtime, tenant=tenant, settings=settings, app=app, client=client, agent=agent
        )
    await runtime.close()


async def queued_for_tenant(env):
    return [
        (entry_id, fields)
        for entry_id, fields in await env.runtime._client.xrange("agent:goal:queue")
        if fields.get("tenant_id") == env.tenant
    ]


async def test_legacy_goal_submission_is_execution_ingress_even_when_text_requests_plan_mode(
    environment,
):
    env = environment
    objective = "Plan mode: forecast the work and keep this as a plan."
    response = await env.client.post(
        "/v1/goals",
        json={
            "objective": objective,
            "steps": [{"query": "Describe a possible report outline"}],
            "mode": "plan",
            "metadata": {"mode": "plan", "forecast": {"total_tokens": 1}},
        },
    )
    assert response.status_code == 200, response.text
    saved = response.json()
    # The existing execution endpoint ignores unknown metadata; it is not a dry run.
    assert saved["status"] == "queued" and saved["objective"] == objective
    assert saved["tenant_id"] == env.tenant and saved["created_by"] == "operator"
    assert saved["model"] == env.settings.llm_model
    assert set(saved).isdisjoint({"mode", "metadata", "forecast", "planning_session_id"})
    assert await env.runtime.list_goal_ids(env.tenant) == [saved["goal_id"]]
    queue = await queued_for_tenant(env)
    assert len(queue) == 1 and queue[0][1]["goal_id"] == saved["goal_id"]
    assert not await env.runtime._client.exists(f"tenant:{env.tenant}:planning:sessions")


async def test_planning_one_session_does_not_mutate_agent_goal_or_another_session(environment):
    env, agent_id = environment, environment.agent["agent_id"]
    goal_response = await env.client.post(
        f"/v1/agents/{agent_id}/goals",
        json={"objective": "Execute the previously requested report", "steps": draft()["steps"]},
    )
    assert goal_response.status_code == 200, goal_response.text
    goal_id = goal_response.json()["goal_id"]
    original_goal = await env.runtime.get_goal(goal_id, env.tenant)
    original_queue = await queued_for_tenant(env)
    original_agent = await AgentStore(env.runtime).get(env.tenant, agent_id)
    create_path = f"/v1/agents/{agent_id}/planning-sessions"
    responses = [await env.client.post(create_path, json={}) for _ in range(2)]
    assert all(response.status_code == 200 for response in responses)
    first, second = [response.json() for response in responses]
    assert first["session_id"] != second["session_id"]
    first_path = f"/v1/planning-sessions/{first['session_id']}"
    second_path = f"/v1/planning-sessions/{second['session_id']}"
    response = await env.client.post(
        second_path + "/plans", json={"expected_version": 1, "plan": draft("Independent plan B")}
    )
    assert response.status_code == 200, response.text
    original_second = (await env.client.get(second_path)).json()
    original_second_revision = (await env.client.get(second_path + "/plans/1")).json()

    response = await env.client.post(
        first_path + "/plans", json={"expected_version": 1, "plan": draft("Independent plan A")}
    )
    assert response.status_code == 200, response.text
    first = response.json()
    response = await env.client.post(
        first_path + "/refresh", json={"expected_version": first["version"]}
    )
    assert response.status_code == 200, response.text
    first = response.json()
    response = await env.client.post(
        first_path + "/mode",
        json={
            "expected_version": first["version"],
            "mode": "execute",
            "plan_revision": first["plan_revision"],
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["execution_started"] is False
    assert (await env.client.get(second_path)).json() == original_second
    assert (await env.client.get(second_path + "/plans/1")).json() == original_second_revision
    assert await env.runtime.get_goal(goal_id, env.tenant) == original_goal
    assert await queued_for_tenant(env) == original_queue
    assert await AgentStore(env.runtime).get(env.tenant, agent_id) == original_agent
    assert await env.runtime.list_goal_ids(env.tenant) == [goal_id]
