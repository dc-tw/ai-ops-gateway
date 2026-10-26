import asyncio
import json
import os
from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import yaml
from pydantic import ValidationError

from app.core.config import Settings
from app.core.redis_runtime import RedisRuntime
from app.core.skill_store import (
    MAX_BOUND_SKILL_BYTES,
    SkillBinding,
    SkillConflict,
    SkillContent,
    SkillStore,
    guard_goal_skills,
    resolve_skill_bindings,
    skill_refs,
    snapshot_content_hash,
    validate_skill_snapshots,
)
from app.llm.client import ModelRouter
from app.main import create_app


def content(**changes):
    return SkillContent(
        name="evidence-notes",
        display_name="證據筆記",
        description="根據資料整理證據。",
        instructions="只使用已提供資料，保留不確定性，不宣稱已執行外部操作。",
        **changes,
    ).model_dump()


class Detector:
    def __init__(self, blocked=False):
        self.blocked = blocked

    async def ascan(self, *args, **kwargs):
        return SimpleNamespace(findings=[], blocked=self.blocked)


@pytest.fixture
async def environment():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL to a dedicated Redis test server")
    runtime = RedisRuntime(url)
    tenant, foreign = f"skills-{uuid4().hex}", f"foreign-{uuid4().hex}"
    await runtime.ping()
    keys = {
        key: {"subject": key, "tenant_id": scope, "role": role}
        for key, scope, role in (
            ("admin", tenant, "admin"),
            ("viewer", tenant, "viewer"),
            ("operator", tenant, "operator"),
            ("foreign", foreign, "admin"),
        )
    }
    settings = Settings(api_keys_json=json.dumps(keys), pii_redaction=False)
    app = create_app(settings=settings, redis_runtime=runtime)
    app.state.settings, app.state.redis = settings, runtime
    app.state.threat_detector = Detector()
    app.state.model_router = ModelRouter(settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"x-api-key": "admin"},
    ) as client:
        yield SimpleNamespace(
            runtime=runtime,
            store=SkillStore(runtime),
            tenant=tenant,
            foreign=foreign,
            app=app,
            client=client,
        )
    await runtime.close()


@pytest.mark.parametrize("extra", [{"policy": {}}, {"scripts": []}, {"tenant_id": "other"}])
def test_skill_content_rejects_execution_or_identity_fields(extra):
    with pytest.raises(ValidationError):
        SkillContent.model_validate(content() | extra)


@pytest.mark.parametrize("name", ["Upper", "two--hyphens", "../path", "bad_name", "-edge"])
def test_skill_name_is_portable_slug(name):
    with pytest.raises(ValidationError):
        SkillContent.model_validate(content() | {"name": name})


def test_unicode_byte_limit_and_strict_revision():
    with pytest.raises(ValidationError):
        SkillContent.model_validate(content() | {"instructions": "中" * 4_001})
    for invalid in (True, "1", 0):
        with pytest.raises(ValidationError):
            SkillBinding(skill_id="skill-id", revision=invalid)


async def test_corrupted_snapshot_errors_do_not_leak_instruction_text():
    private = "PRIVATE-INSTRUCTION-SENTINEL"
    value = content() | {
        "skill_id": "skill-1",
        "tenant_id": "acme",
        "revision": 1,
        "content_sha256": "0" * 64,
        "revocation_epoch": 0,
        "instructions": private + "x" * 8_001,
    }
    goal = {
        "tenant_id": "acme",
        "policy": {"allowed_tools": []},
        "agent_config": {
            "role": "worker",
            "skills": [{"skill_id": "skill-1", "revision": 1}],
            "skill_snapshots": [value],
        },
    }
    with pytest.raises(PermissionError) as error:
        await guard_goal_skills(object(), goal)
    assert private not in str(error.value)
    assert private not in str(error.value.__cause__)


def test_skill_audit_references_fail_safe_for_corrupt_metadata():
    valid = {
        "skill_id": "skill-1",
        "revision": 1,
        "content_sha256": "a" * 64,
        "revocation_epoch": 0,
        "instructions": "PRIVATE-INSTRUCTIONS",
    }
    assert (
        skill_refs(
            [
                None,
                valid | {"content_sha256": {"instructions": "PRIVATE"}},
                valid | {"skill_id": "PRIVATE with spaces"},
            ]
        )
        == []
    )
    assert "PRIVATE" not in json.dumps(skill_refs([valid]))


async def test_catalog_install_rbac_idempotency_and_tenant_isolation(environment):
    env, client = environment, environment.client
    catalog = await client.get("/v1/skills/builtins", headers={"x-api-key": "viewer"})
    assert catalog.status_code == 200
    assert len(catalog.json()["skills"]) == 10
    assert all("instructions" not in item for item in catalog.json()["skills"])
    for role in ("viewer", "operator"):
        denied = await client.post(
            "/v1/skills/install", json={"name": "source-summary"}, headers={"x-api-key": role}
        )
        assert denied.status_code == 403
    first = await client.post("/v1/skills/install", json={"name": "source-summary"})
    repeated = await client.post("/v1/skills/install", json={"name": "source-summary"})
    assert first.status_code == repeated.status_code == 200
    skill = first.json()
    assert skill == repeated.json()
    assert skill["tenant_id"] == env.tenant and skill["origin"] == "builtin"
    skill_id = skill["skill_id"]
    for path in (
        f"/{skill_id}",
        f"/{skill_id}/revisions",
        f"/{skill_id}/revisions/1",
        f"/{skill_id}/export",
    ):
        assert (
            await client.get("/v1/skills" + path, headers={"x-api-key": "foreign"})
        ).status_code == 404
    for suffix, body in (
        ("/enabled", {"enabled": False, "expected_version": 1}),
        ("/rollback", {"revision": 1, "expected_version": 1}),
        ("/revisions", content() | {"expected_version": 1}),
    ):
        assert (
            await client.post(
                f"/v1/skills/{skill_id}" + suffix, json=body, headers={"x-api-key": "foreign"}
            )
        ).status_code == 404
    foreign_install = await client.post(
        "/v1/skills/install", json={"name": "source-summary"}, headers={"x-api-key": "foreign"}
    )
    assert foreign_install.json()["skill_id"] != skill_id
    assert len((await client.get("/v1/skills")).json()["skills"]) == 1
    assert (await client.get("/v1/skills/builtins/missing")).status_code == 404


async def test_crud_immutable_revisions_rollback_and_export(environment):
    client = environment.client
    first = (await client.post("/v1/skills", json=content())).json()
    skill_id, old_hash = first["skill_id"], first["content_sha256"]
    path = f"/v1/skills/{skill_id}"
    original = (await client.get(path + "/revisions/1")).json()
    replacement = content() | {"instructions": "以表格整理現有事實與未知。", "expected_version": 1}
    published = await client.post(path + "/revisions", json=replacement)
    assert published.status_code == 200
    assert published.json()["revision"] == 2 and published.json()["version"] == 2
    assert published.json()["content_sha256"] != old_hash
    assert (await client.get(path + "/revisions/1")).json() == original
    assert (await client.put(path, json=replacement)).status_code == 409
    assert (
        await client.put(path, json=replacement | {"name": "new-name", "expected_version": 2})
    ).status_code == 422
    disabled = await client.post(path + "/enabled", json={"enabled": False, "expected_version": 2})
    assert disabled.json()["revocation_epoch"] == 1
    assert disabled.json()["revision"] == 2
    assert (
        await client.post(path + "/enabled", json={"enabled": True, "expected_version": 2})
    ).status_code == 409
    enabled = await client.post(path + "/enabled", json={"enabled": True, "expected_version": 3})
    assert enabled.json()["revocation_epoch"] == 1
    restored = await client.post(path + "/rollback", json={"revision": 1, "expected_version": 4})
    assert restored.status_code == 200
    assert restored.json()["revision"] == 3 and restored.json()["version"] == 5
    assert restored.json()["content_sha256"] == old_hash
    revision = (await client.get(path + "/revisions/3")).json()
    assert revision["restored_from_revision"] == 1
    page = (await client.get(path + "/revisions?limit=2")).json()
    assert [item["revision"] for item in page["revisions"]] == [1, 2]
    assert page["next_after"] == 2
    assert (await client.get(path + "/revisions?after=2")).json()["next_after"] is None
    exported = await client.get(path + "/export?revision=1")
    frontmatter, instructions = exported.text[4:].split("\n---\n\n", 1)
    metadata = yaml.safe_load(frontmatter)
    assert metadata["name"] == original["name"]
    assert metadata["metadata"]["gateway-revision"] == "1"
    assert "allowed-tools" not in metadata  # No exported permission grant.
    assert instructions.rstrip("\n") == original["instructions"]
    assert exported.headers["content-type"].startswith("text/markdown")
    assert (await client.delete(path)).status_code == 405


async def test_schema_content_checks_and_rbac(environment):
    client = environment.client
    for extra in ({"tenant_id": "foreign"}, {"scripts": ["run.sh"]}, {"policy": {}}):
        assert (await client.post("/v1/skills", json=content() | extra)).status_code == 422
    for role in ("viewer", "operator"):
        assert (
            await client.post("/v1/skills", json=content(), headers={"x-api-key": role})
        ).status_code == 403
    environment.app.state.settings.content_blocked_patterns = ["UNSAFE-MARKER"]
    assert (
        await client.post("/v1/skills", json=content() | {"instructions": "UNSAFE-MARKER"})
    ).status_code == 403
    environment.app.state.settings.content_blocked_patterns = []
    environment.app.state.threat_detector = Detector(blocked=True)
    assert (await client.post("/v1/skills", json=content())).status_code == 403
    assert await environment.store.list(environment.tenant) == []


async def test_compare_and_swap_name_uniqueness_and_audit_privacy(environment):
    store, tenant = environment.store, environment.tenant
    results = await asyncio.gather(
        *[store.create(tenant, content(), "admin") for _ in range(20)], return_exceptions=True
    )
    winners = [value for value in results if isinstance(value, dict)]
    assert len(winners) == 1
    assert sum(isinstance(value, SkillConflict) for value in results) == 19
    first = winners[0]
    marker = "PRIVATE-INSTRUCTION-MARKER"
    results = await asyncio.gather(
        *[
            store.publish(
                tenant, first["skill_id"], content() | {"instructions": marker}, 1, "admin"
            )
            for _ in range(20)
        ],
        return_exceptions=True,
    )
    assert sum(isinstance(value, dict) for value in results) == 1
    assert sum(isinstance(value, SkillConflict) for value in results) == 19
    assert (await store.get_revision(tenant, first["skill_id"], 1))["instructions"] == content()[
        "instructions"
    ]
    audits = await environment.runtime._client.xrange(f"tenant:{tenant}:audit")
    assert marker not in json.dumps(audits)
    assert first["skill_id"] in json.dumps(audits)


async def test_snapshot_pin_update_revocation_and_new_goal_epoch(environment):
    store, runtime, tenant = environment.store, environment.runtime, environment.tenant
    record = await store.create(tenant, content(), "admin")
    binding = [{"skill_id": record["skill_id"], "revision": 1}]
    snapshots = await resolve_skill_bindings(
        runtime, tenant, binding, {"allowed_tools": []}, "worker"
    )
    goal = {
        "tenant_id": tenant,
        "policy": {"allowed_tools": []},
        "agent_config": {"role": "worker", "skills": binding, "skill_snapshots": snapshots},
    }
    await store.publish(
        tenant, record["skill_id"], content() | {"instructions": "新版指令"}, 1, "admin"
    )
    await guard_goal_skills(runtime, goal)
    assert snapshots[0]["instructions"] == content()["instructions"]
    await store.set_enabled(tenant, record["skill_id"], False, 2, "admin")
    with pytest.raises(PermissionError):
        await guard_goal_skills(runtime, goal)
    await store.set_enabled(tenant, record["skill_id"], True, 3, "admin")
    with pytest.raises(PermissionError):
        await guard_goal_skills(runtime, goal)
    fresh = await resolve_skill_bindings(runtime, tenant, binding, {"allowed_tools": []}, "worker")
    assert fresh[0]["revocation_epoch"] == 1
    assert fresh[0]["instructions"] == snapshots[0]["instructions"]
    await guard_goal_skills(
        runtime, goal | {"agent_config": goal["agent_config"] | {"skill_snapshots": fresh}}
    )


async def test_tool_role_hash_and_aggregate_limits_fail_closed(environment):
    runtime, store, tenant = environment.runtime, environment.store, environment.tenant
    record = await store.create(
        tenant, content(required_tools=["scrape_dynamic_page"], agent_roles=["worker"]), "admin"
    )
    bindings = [{"skill_id": record["skill_id"], "revision": 1}]
    with pytest.raises(ValueError, match="tools"):
        await resolve_skill_bindings(runtime, tenant, bindings, {"allowed_tools": []}, "worker")
    with pytest.raises(ValueError, match="role"):
        await resolve_skill_bindings(
            runtime, tenant, bindings, {"allowed_tools": ["scrape_dynamic_page"]}, "chief"
        )
    snapshots = await resolve_skill_bindings(
        runtime, tenant, bindings, {"allowed_tools": ["scrape_dynamic_page"]}, "worker"
    )
    tampered = deepcopy(snapshots)
    tampered[0]["instructions"] = "changed"
    with pytest.raises(ValueError, match="hash"):
        validate_skill_snapshots(
            tampered, bindings, {"allowed_tools": ["scrape_dynamic_page"]}, "worker"
        )
    big_snapshots, big_bindings = [], []
    for i in range(2):
        value = deepcopy(snapshots[0]) | {
            "skill_id": f"large-{i}",
            "instructions": "x" * (MAX_BOUND_SKILL_BYTES // 2 + 1),
        }
        value["content_sha256"] = snapshot_content_hash(value)
        big_snapshots.append(value)
        big_bindings.append({"skill_id": value["skill_id"], "revision": 1})
    # Character max is 8,000, so use multibyte text to reach the aggregate byte boundary.
    for value in big_snapshots:
        value["instructions"] = "中" * 2_700
        value["content_sha256"] = snapshot_content_hash(value)
    with pytest.raises(ValueError, match="exceed"):
        validate_skill_snapshots(
            big_snapshots, big_bindings, {"allowed_tools": ["scrape_dynamic_page"]}, "worker"
        )


async def test_no_redis_returns_503_not_in_memory_management(environment):
    environment.app.state.redis = None
    assert (await environment.client.get("/v1/skills")).status_code == 503
    assert (await environment.client.get("/v1/skills/builtins")).status_code == 200


INVALID_TEXT_FIELDS = (
    {"name": "invalid-\ud800"},
    {"display_name": "PRIVATE-SKILL-UTF8\udfff"},
    {"description": "PRIVATE-SKILL-UTF8\ud800"},
    {"instructions": "PRIVATE-SKILL-UTF8\udfff"},
    {"required_tools": ["scrape_dynamic_page", "PRIVATE-SKILL-UTF8\ud800"]},
    {"agent_roles": ["worker", "PRIVATE-SKILL-UTF8\udfff"]},
    {"tags": ["valid", "PRIVATE-SKILL-UTF8\ud800"]},
)


@pytest.mark.parametrize(
    "changes",
    INVALID_TEXT_FIELDS,
    ids=["name", "display-name", "description", "instructions", "tools", "roles", "tags"],
)
def test_every_skill_text_field_rejects_unencodable_unicode(changes):
    with pytest.raises(ValidationError) as error:
        SkillContent.model_validate(content() | changes)
    assert "Skill text must be valid UTF-8" in str(error.value)
    assert "PRIVATE-SKILL-UTF8" not in str(error.value)


def test_v1_hash_of_valid_unicode_content_is_unchanged():
    value = SkillContent.model_validate(
        content() | {"display_name": "證據筆記 🧪", "tags": ["繁體中文", "emoji-😀"]}
    ).model_dump()
    # Captured before the UTF-8 validator: seven-field v1 canonicalization and
    # normalization of valid content must remain exactly the same.
    assert snapshot_content_hash(value) == (
        "9bd284a959f694c69e1ce86c97b62872d963c4420c6c2e81d7c95839de32494a"
    )
    assert value["display_name"] == "證據筆記 🧪"
    assert value["tags"] == ["emoji-😀", "繁體中文"]


async def skill_persistence_state(env):
    state = {}
    client = env.runtime._client
    keys = [key async for key in client.scan_iter(f"tenant:{env.tenant}:*")]
    for key in sorted(keys):
        kind = await client.type(key)
        if kind == "string":
            value = await client.get(key)
        elif kind == "stream":
            value = await client.xrange(key)
        elif kind == "set":
            value = sorted(await client.smembers(key))
        else:
            pytest.fail(f"Unexpected Skill persistence type: {kind}")
        state[key] = value
    return state


async def assert_invalid_skill_request(env, method, path, body):
    before = await skill_persistence_state(env)
    response = await env.client.request(
        method,
        path,
        content=json.dumps(body, ensure_ascii=True).encode("ascii"),
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 422, response.text
    assert response.json() == {"detail": "Invalid Skill request"}
    assert "PRIVATE-SKILL" not in response.text
    assert await skill_persistence_state(env) == before


@pytest.mark.parametrize(
    "changes",
    INVALID_TEXT_FIELDS,
    ids=["name", "display-name", "description", "instructions", "tools", "roles", "tags"],
)
async def test_raw_skill_unicode_rejection_never_changes_content_or_history(environment, changes):
    env = environment
    original = (await env.client.post("/v1/skills", json=content())).json()
    skill_id = original["skill_id"]
    for method, path, body in (
        ("POST", "/v1/skills", content() | changes),
        ("PUT", f"/v1/skills/{skill_id}", content() | changes | {"expected_version": 1}),
        (
            "POST",
            f"/v1/skills/{skill_id}/revisions",
            content() | changes | {"expected_version": 1},
        ),
    ):
        await assert_invalid_skill_request(env, method, path, body)
    assert (await env.client.get(f"/v1/skills/{skill_id}")).json() == original
    revision = (await env.client.get(f"/v1/skills/{skill_id}/revisions/1")).json()
    assert revision["instructions"] == content()["instructions"]


async def test_skill_extra_keys_and_sensitive_version_errors_are_safe(environment):
    env = environment
    original = (await env.client.post("/v1/skills", json=content())).json()
    path = f"/v1/skills/{original['skill_id']}"
    private = "PRIVATE-SKILL-VERSION\ud800"
    for method, target, body in (
        ("POST", "/v1/skills", content() | {"PRIVATE-SKILL-EXTRA\udfff": private}),
        ("POST", "/v1/skills/install", {"name": "source-summary", private: private}),
        ("POST", "/v1/skills/install", {"name": private}),
        ("PUT", path, content() | {"expected_version": private}),
        ("POST", path + "/revisions", content() | {"expected_version": {"secret": private}}),
        ("POST", path + "/enabled", {"enabled": False, "expected_version": private}),
        ("POST", path + "/rollback", {"revision": 1, "expected_version": private}),
    ):
        await assert_invalid_skill_request(env, method, target, body)
    # The scoped Skill route does not replace error contracts on legacy APIs.
    legacy = await env.client.post("/v1/agent/stream", json={"query": []})
    assert legacy.status_code == 422 and isinstance(legacy.json()["detail"], list)
