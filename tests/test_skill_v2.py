"""Skill v2 compatibility, server-owned modes and complete read-only context."""

import hashlib
import json
import os
from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.agents import AgentStore, apply_agent
from app.automations import AutomationPolicy
from app.core.execution_policy import BudgetedModel
from app.core.redis_runtime import RedisRuntime
from app.core.skill_store import (
    CONTENT_FIELDS,
    SkillContent,
    SkillStore,
    content_fields,
    guard_goal_skills,
    render_skill_instructions,
    resolve_skill_bindings,
    skill_text,
    snapshot_content_hash,
    validate_skill_snapshots,
)


def content(**changes):
    return SkillContent.model_validate(
        {
            "name": "evidence-v2",
            "display_name": "證據整理",
            "description": "只讀技能",
            "instructions": "保留證據與未知事項。",
            "schema_version": 2,
            "resources": [{"path": "references/example.md", "content": "RESOURCE-CANARY"}],
            **changes,
        }
    ).model_dump()


def snapshot(**changes):
    value = content(**changes)
    return value | {
        "tenant_id": "acme",
        "skill_id": "v2",
        "revision": 1,
        "revocation_epoch": 0,
        "content_sha256": snapshot_content_hash(value),
    }


def test_v1_serialization_and_hash_are_byte_for_byte_legacy_compatible():
    v1 = SkillContent(
        name="legacy", display_name="Legacy", description="test", instructions="Keep unchanged"
    ).model_dump()
    assert set(v1) == set(CONTENT_FIELDS)
    canonical = json.dumps(v1, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    assert snapshot_content_hash(v1) == hashlib.sha256(canonical.encode()).hexdigest()
    assert content_fields(v1 | {"schema_version": 1}) == v1


@pytest.mark.parametrize(
    "path",
    [
        "../secret.md",
        "/tmp/file.md",
        "a/../secret.md",
        "a//b.md",
        "scripts/setup.md",
        "refs/.hidden.txt",
        "setup.py",
        "run.sh",
        "node_modules/file.json",
        "a\\b.txt",
        "refs/\x00.txt",
    ],
)
def test_resources_reject_unsafe_and_executable_paths(path):
    with pytest.raises(ValidationError):
        content(resources=[{"path": path, "content": "test"}])


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": 1},
        {"resources": [{"path": "one.md", "content": "x"}, {"path": "one.md", "content": "y"}]},
        {"resources": [{"path": "one.md", "content": "中" * 5_000}]},
        {"execution_modes": ["plan"], "required_tools": ["browser_close"]},
        {"name": "plan-forecast", "execution_modes": ["execute"]},
        {"name": "plan-forecast", "execution_modes": ["execute", "plan"]},
    ],
)
def test_v2_shape_capacity_and_forecast_boundaries(changes):
    with pytest.raises(ValidationError):
        content(**changes)


def test_resources_are_hashed_scanned_rendered_and_charged_as_the_same_text():
    item = snapshot()
    changed = deepcopy(item)
    changed["resources"][0]["content"] += "modified"
    assert snapshot_content_hash(changed) != item["content_sha256"]
    assert "RESOURCE-CANARY" in skill_text(item)
    assert skill_text(item) in render_skill_instructions([item])
    model = BudgetedModel(SimpleNamespace(), 20_000, skill_snapshots=[item])
    assert "RESOURCE-CANARY" in model.skill_instructions
    with pytest.raises(ValueError, match="hash"):
        validate_skill_snapshots(
            [changed], [{"skill_id": "v2", "revision": 1}], {"allowed_tools": []}, "worker"
        )


def test_planning_forecast_cannot_be_rendered_or_bound_for_execution():
    item = snapshot(name="plan-forecast", execution_modes=["plan"])
    bindings = [{"skill_id": "v2", "revision": 1}]
    validate_skill_snapshots([item], bindings, {"allowed_tools": []}, "worker", mode="plan")
    with pytest.raises(ValueError, match="mode"):
        validate_skill_snapshots([item], bindings, {"allowed_tools": []}, "worker")
    with pytest.raises(ValueError, match="mode"):
        BudgetedModel(SimpleNamespace(), 20_000, skill_snapshots=[item])


async def test_readonly_resource_context_increases_actual_budget_charge():
    class Provider:
        async def complete(self, system, prompt):
            return "Synthetic answer"

    resource = snapshot()
    plain = snapshot(resources=[])
    models = [
        BudgetedModel(Provider(), 20_000, skill_snapshots=[item]) for item in (resource, plain)
    ]
    for model in models:
        await model.complete("system", "prompt")
    delta = len(render_skill_instructions([resource]).encode()) - len(
        render_skill_instructions([plain]).encode()
    )
    assert models[0].usage["input_tokens"] - models[1].usage["input_tokens"] == delta > 0


@pytest.fixture
async def env():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Skill v2 persistence requires TEST_REDIS_URL")
    runtime = RedisRuntime(url)
    await runtime.ping()
    tenant = "skill-v2-" + uuid4().hex
    try:
        yield SimpleNamespace(
            runtime=runtime, client=runtime._client, tenant=tenant, store=SkillStore(runtime)
        )
    finally:
        keys = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}:*")]
        if keys:
            await runtime._client.delete(*keys)
        await runtime.close()


async def test_v1_upgrade_and_rollback_preserve_old_revisions_without_v2_residue(env):
    legacy = SkillContent(
        name="upgrade", display_name="old", description="old", instructions="legacy"
    ).model_dump()
    first = await env.store.create(env.tenant, legacy, "admin")
    sid = first["skill_id"]
    second = await env.store.publish(env.tenant, sid, content(name="upgrade"), 1, "admin")
    assert second["schema_version"] == 2
    third = await env.store.publish(env.tenant, sid, legacy, 2, "admin", restored_from=1)
    assert not set(third) & {"schema_version", "execution_modes", "resources", "provenance"}
    assert third["content_sha256"] == first["content_sha256"]
    assert await env.store.get_revision(env.tenant, sid, 1) == env.store._revision(first, "admin")
    assert (await env.store.get_revision(env.tenant, sid, 2))["resources"] == second["resources"]


async def test_resource_snapshots_revoke_and_cannot_mutate_after_admission(env):
    skill = await env.store.create(env.tenant, content(), "admin")
    bindings = [{"skill_id": skill["skill_id"], "revision": 1}]
    snapshots = await resolve_skill_bindings(
        env.runtime, env.tenant, bindings, {"allowed_tools": []}, "worker"
    )
    goal = {
        "tenant_id": env.tenant,
        "policy": {"allowed_tools": []},
        "agent_config": {"role": "worker", "skills": bindings, "skill_snapshots": snapshots},
    }
    await guard_goal_skills(env.runtime, goal)
    tampered = deepcopy(goal)
    tampered["agent_config"]["skill_snapshots"][0]["resources"][0]["content"] = "tampered"
    with pytest.raises(PermissionError):
        await guard_goal_skills(env.runtime, tampered)
    await env.store.set_enabled(env.tenant, skill["skill_id"], False, 1, "admin")
    await env.store.set_enabled(env.tenant, skill["skill_id"], True, 2, "admin")
    with pytest.raises(PermissionError):
        await guard_goal_skills(env.runtime, goal)


async def test_agent_planning_bindings_never_enter_execution_snapshot(env):
    skill = await env.store.create(
        env.tenant, content(name="plan-forecast", execution_modes=["plan"]), "admin"
    )
    binding = {"skill_id": skill["skill_id"], "revision": 1}
    agent = {
        "agent_id": "worker",
        "tenant_id": env.tenant,
        "name": "worker",
        "persona": "read only",
        "description": "",
        "role": "worker",
        "model": "local",
        "enabled": True,
        "delegate_agent_ids": [],
        "skills": [],
        "planning_skills": [binding],
        "policy": AutomationPolicy(allowed_tools=[]).model_dump(),
        "created_by": "admin",
        "created_at": "now",
        "updated_at": "now",
    }
    saved = await AgentStore(env.runtime).create(agent)
    assert saved["planning_skills"] == [binding]
    goal = await apply_agent(
        env.runtime, env.tenant, "worker", {"objective": "mention plan-forecast"}
    )
    assert "planning_skills" not in goal["agent_config"]
    assert goal["agent_config"]["skill_snapshots"] == []
    with pytest.raises(ValueError, match="mode"):
        await AgentStore(env.runtime).create(agent | {"agent_id": "invalid", "skills": [binding]})


async def test_auto_whitelist_capacity_is_rejected_before_agent_is_saved(env):
    first = await env.store.create(
        env.tenant,
        content(
            name="large-a",
            instructions="i" * 1_000,
            resources=[{"path": "one.txt", "content": "r" * 8_000}],
        ),
        "admin",
    )
    second = await env.store.create(
        env.tenant,
        content(
            name="large-b",
            instructions="i" * 1_000,
            resources=[{"path": "one.txt", "content": "r" * 8_000}],
        ),
        "admin",
    )
    agent = {
        "agent_id": "too-large",
        "tenant_id": env.tenant,
        "role": "worker",
        "skills": [{"skill_id": first["skill_id"], "revision": 1}],
        "skill_selection_rules": [
            {
                "skill_id": second["skill_id"],
                "revision": 1,
                "field": "objective",
                "values": ["unlikely"],
            }
        ],
        "policy": {"allowed_tools": []},
        "created_by": "admin",
    }
    with pytest.raises(ValueError, match="Bound Skills"):
        await AgentStore(env.runtime).create(agent)
    assert await AgentStore(env.runtime).get(env.tenant, "too-large") is None
