import asyncio
import base64
import io
import json
import os
import stat
import zipfile
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI

from app.core.config import Settings
from app.core.redis_runtime import RedisRuntime
from app.core.resource_access import AccessStore
from app.core.security import Principal
from app.core.skill_packages import fetch_github, parse_package
from app.core.skill_store import SkillStore, snapshot_content_hash
from app.core.threat_detection import ThreatScan
from app.skill_imports import preview_key, router
from app.skills import router as skills_router

SKILL = (
    "---\nname: imported-review\ndescription: Read only evidence\n---\n"
    "Use supplied evidence only.\n"
)


def archive(files=None, *, prefix="", links=False):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as value:
        for name, content in (
            files or {"SKILL.md": SKILL, "references/data.md": "READONLY-CANARY"}
        ).items():
            if links and name != "SKILL.md":
                info = zipfile.ZipInfo(prefix + name)
                info.external_attr = (stat.S_IFLNK | 0o777) << 16
                value.writestr(info, content)
            else:
                value.writestr(prefix + name, content)
    return output.getvalue()


def test_upload_is_v2_readonly_and_provenance_covers_archive_bytes():
    raw = archive()
    body = parse_package(raw)
    assert body["schema_version"] == 2 and body["execution_modes"] == ["execute"]
    assert body["resources"] == [{"path": "references/data.md", "content": "READONLY-CANARY"}]
    assert body["provenance"]["source"] == "upload"
    assert snapshot_content_hash(body)


@pytest.mark.parametrize(
    "files",
    [
        {"SKILL.md": SKILL, "../escape.md": "bad"},
        {"SKILL.md": SKILL, "scripts/run.py": "bad"},
        {"SKILL.md": SKILL, "scripts/setup.md": "bad"},
        {"SKILL.md": SKILL, "assets/binary.png": b"\x00\x01"},
        {"SKILL.md": SKILL, "/tmp/escape.md": "bad"},
        {"SKILL.md": SKILL, "a\\b.txt": "bad"},
        {"SKILL.md": SKILL, "refs/data.md": "a" * 20_000},
        {"SKILL.md": SKILL, "refs/data.md": b"\xff\xfe"},
        {"README.md": "No Skill manifest"},
        {"SKILL.md": "---\nname: first\nname: second\ndescription: test\n---\nbody"},
        {"SKILL.md": "---\nname: &x test\ndescription: *x\n---\nbody"},
        {"SKILL.md": "---\nname: !!python/object/apply:os.system [test]\n---\nbody"},
        {"SKILL.md": SKILL.replace("description:", "scripts:")},
    ],
)
def test_invalid_packages_do_not_echo_content_or_paths(files):
    with pytest.raises(ValueError) as exc:
        parse_package(archive(files))
    assert str(exc.value) == "Invalid or unsafe read-only Skill package"


def test_links_duplicate_members_and_zip_bombs_are_rejected():
    with pytest.raises(ValueError):
        parse_package(archive(links=True))
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as value:
        value.writestr("SKILL.md", SKILL)
        with pytest.warns(UserWarning):
            value.writestr("SKILL.md", SKILL)
    with pytest.raises(ValueError):
        parse_package(output.getvalue())
    with pytest.raises(ValueError):
        parse_package(archive({"SKILL.md": SKILL, "bomb.md": "a" * 1_000_000}))
    with pytest.raises(ValueError):
        parse_package(archive({"SKILL.md": SKILL, "ratio.md": "a" * 12_000}))
    raw = archive({"SKILL.md": SKILL, "refs/data.mdXpayload.md": "text"})
    raw = raw.replace(b"data.mdXpayload.md", b"data.md\x00payload.md")
    with pytest.raises(ValueError):
        parse_package(raw)


async def test_github_requires_pinned_commit_and_only_exact_provider_redirect():
    commit = "a" * 40
    calls = []
    raw = archive(prefix="repo-commit/package/")

    def respond(request):
        calls.append(str(request.url))
        if "/git/commits/" in request.url.path:
            return httpx.Response(
                200, stream=httpx.ByteStream(json.dumps({"sha": commit}).encode())
            )
        if request.url.host == "api.github.com":
            return httpx.Response(
                302,
                headers={"location": f"https://codeload.github.com/acme/repo/legacy.zip/{commit}"},
            )
        return httpx.Response(200, stream=httpx.ByteStream(raw))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        received = await fetch_github("acme/repo", commit, client=client)
        body = parse_package(
            received, source="github", repository="acme/repo", commit=commit, directory="package"
        )
        assert body["provenance"]["commit"] == commit and len(calls) == 3
        for ref in ("main", "a" * 7, "../main"):
            with pytest.raises(ValueError):
                await fetch_github("acme/repo", ref, client=client)
        assert len(calls) == 3


@pytest.mark.parametrize(
    "target",
    [
        "http://127.0.0.1/private",
        "https://evil.test/private",
        "https://codeload.github.com@evil.test/archive",
        "https://codeload.github.com/acme/other/zip/" + "a" * 40,
        "https://codeload.github.com/acme/repo/zip/" + "a" * 40 + "?token=private",
    ],
)
async def test_github_does_not_follow_unsafe_redirects(target):
    calls = []

    def respond(request):
        calls.append(request.url)
        if "/git/commits/" in request.url.path:
            return httpx.Response(
                200, stream=httpx.ByteStream(json.dumps({"sha": "a" * 40}).encode())
            )
        return httpx.Response(302, headers={"location": target})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=True
    ) as client:
        with pytest.raises(ValueError):
            await fetch_github("acme/repo", "a" * 40, client=client)
    assert len(calls) == 2


async def test_hex_named_floating_ref_or_wrong_commit_object_never_downloads_archive():
    calls = []

    def respond(request):
        calls.append(request.url)
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps({"sha": "b" * 40}).encode()))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ValueError, match="identity"):
            await fetch_github("acme/repo", "a" * 40, client=client)
    assert len(calls) == 1 and "/git/commits/" in calls[0].path


@pytest.fixture
async def env():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Package confirmation requires TEST_REDIS_URL")
    runtime = RedisRuntime(url)
    await runtime.ping()
    tenant = "packages-" + uuid4().hex
    application = FastAPI()
    application.include_router(router)
    application.include_router(skills_router)
    application.state.redis = runtime
    application.state.settings = Settings(
        skill_v2_enabled=True,
        skill_package_import_enabled=True,
        tenant_requests_per_minute=10_000,
        api_keys_json=json.dumps(
            {
                "admin": {"subject": "admin", "role": "admin", "tenant_id": tenant},
                "operator": {"subject": "operator", "role": "operator", "tenant_id": tenant},
                "foreign": {"subject": "admin", "role": "admin", "tenant_id": tenant + "-other"},
            }
        ),
    )

    class Detector:
        blocked = False

        async def ascan(self, text, **kwargs):
            return ThreatScan(kwargs["event_type"], kwargs["location"], (), self.blocked)

    application.state.threat_detector = Detector()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://test",
        headers={"x-api-key": "admin"},
    ) as client:
        try:
            yield SimpleNamespace(runtime=runtime, client=client, tenant=tenant, app=application)
        finally:
            keys = [key async for key in runtime._client.scan_iter(f"tenant:{tenant}:*")]
            if keys:
                await runtime._client.delete(*keys)
            await runtime.close()


async def preview(env):
    response = await env.client.post(
        "/v1/skill-imports/upload", json={"archive_base64": base64.b64encode(archive()).decode()}
    )
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    return response.json()


async def test_preview_is_not_publication_and_concurrent_confirm_is_idempotent(env):
    candidate = await preview(env)
    assert await SkillStore(env.runtime).list(env.tenant) == []
    path = f"/v1/skill-imports/{candidate['import_id']}/confirm"
    body = {"expected_content_sha256": candidate["content_sha256"]}
    results = await asyncio.gather(*(env.client.post(path, json=body) for _ in range(4)))
    assert all(value.status_code == 200 for value in results)
    assert len({value.json()["skill_id"] for value in results}) == 1
    skill = results[0].json()
    assert (
        skill["origin"] == "package" and skill["provenance"] == candidate["content"]["provenance"]
    )
    assert len(await SkillStore(env.runtime).list(env.tenant)) == 1
    assert await AccessStore(env.runtime).can(
        Principal("admin", env.tenant, "operator", "api_key"), "skill", skill["skill_id"]
    )
    assert (await env.client.get(f"/v1/skill-imports/{candidate['import_id']}")).json()[
        "status"
    ] == "published"


async def test_preview_authorization_flags_expiry_hash_and_rescan(env):
    candidate = await preview(env)
    path = f"/v1/skill-imports/{candidate['import_id']}"
    assert (await env.client.get(path, headers={"x-api-key": "operator"})).status_code == 403
    assert (await env.client.get(path, headers={"x-api-key": "foreign"})).status_code == 404
    assert (
        await env.client.post(path + "/confirm", json={"expected_content_sha256": "f" * 64})
    ).status_code == 409
    env.app.state.threat_detector.blocked = True
    assert (
        await env.client.post(
            path + "/confirm", json={"expected_content_sha256": candidate["content_sha256"]}
        )
    ).status_code == 403
    assert await SkillStore(env.runtime).list(env.tenant) == []
    await env.runtime._client.delete(preview_key(env.tenant, candidate["import_id"]))
    assert (await env.client.get(path)).status_code == 404
    env.app.state.settings.skill_package_import_enabled = False
    assert (
        await env.client.post("/v1/skill-imports/upload", json={"archive_base64": "eA=="})
    ).status_code == 409


def test_descriptive_standard_metadata_cannot_grant_tools_or_execute():
    manifest = SKILL.replace(
        "---\nUse",
        "allowed-tools: Bash(git:*)\nlicense: MIT\nmetadata:\n  author: example\n---\nUse",
    )
    body = parse_package(archive({"SKILL.md": manifest}))
    assert body["required_tools"] == []
    meta = json.loads(body["resources"][0]["content"])
    assert meta["allowed-tools"] == "Bash(git:*)" and meta["metadata"]["author"] == "example"


@pytest.mark.parametrize("destination", ["skills", "audit"])
async def test_bad_publication_destination_cannot_create_half_published_skill(env, destination):
    candidate = await preview(env)
    key = f"tenant:{env.tenant}:{destination}"
    await env.runtime._client.delete(key)
    await env.runtime._client.set(key, "WRONGTYPE")
    response = await env.client.post(
        f"/v1/skill-imports/{candidate['import_id']}/confirm",
        json={"expected_content_sha256": candidate["content_sha256"]},
    )
    assert response.status_code == 503 and "WRONGTYPE" not in response.text
    assert not [
        key async for key in env.runtime._client.scan_iter(f"tenant:{env.tenant}:skills:record:*")
    ]
    stored = json.loads(
        await env.runtime._client.get(preview_key(env.tenant, candidate["import_id"]))
    )
    assert stored["status"] == "previewed"


@pytest.mark.parametrize("change", ["expire", "replace"])
async def test_preview_expiry_or_replacement_during_confirmation_scan_is_fenced(env, change):
    candidate = await preview(env)
    key = preview_key(env.tenant, candidate["import_id"])

    class Detector:
        async def ascan(self, text, **kwargs):
            if change == "expire":
                await env.runtime._client.delete(key)
            else:
                updated = dict(candidate)
                updated["content"] = {**candidate["content"], "instructions": "replacement"}
                updated["content_sha256"] = snapshot_content_hash(updated["content"])
                await env.runtime._client.set(key, json.dumps(updated))
            return ThreatScan(kwargs["event_type"], kwargs["location"], (), False)

    env.app.state.threat_detector = Detector()
    response = await env.client.post(
        f"/v1/skill-imports/{candidate['import_id']}/confirm",
        json={"expected_content_sha256": candidate["content_sha256"]},
    )
    assert response.status_code == (404 if change == "expire" else 409)
    assert await SkillStore(env.runtime).list(env.tenant) == []


async def test_github_http_preview_preserves_server_verified_source_not_body_claims(
    env, monkeypatch
):
    import app.skill_imports as imports

    calls = []

    async def fetch(repository, commit):
        calls.append((repository, commit))
        return archive(prefix="repo-snapshot/safe-skill/")

    monkeypatch.setattr(imports, "fetch_github", fetch)
    payload = {"repository": "acme/repo", "commit": "a" * 40, "subdirectory": "safe-skill"}
    response = await env.client.post("/v1/skill-imports/github", json=payload)
    assert response.status_code == 200
    source = response.json()["content"]["provenance"]
    assert source["repository"] == payload["repository"] and source["commit"] == payload["commit"]
    assert source["source"] == "github" and source["subdirectory"] == "safe-skill"
    for body in (payload | {"commit": "main"}, payload | {"url": "http://127.0.0.1/private"}):
        assert (await env.client.post("/v1/skill-imports/github", json=body)).status_code == 422
    assert len(calls) == 1


async def test_package_provenance_survives_explicit_rollback_without_allowing_forgery(env):
    candidate = await preview(env)
    imported = (
        await env.client.post(
            f"/v1/skill-imports/{candidate['import_id']}/confirm",
            json={"expected_content_sha256": candidate["content_sha256"]},
        )
    ).json()
    sid = imported["skill_id"]
    manual = candidate["content"] | {
        "provenance": None,
        "instructions": "Manual revision",
        "expected_version": 1,
    }
    assert (await env.client.put(f"/v1/skills/{sid}", json=manual)).status_code == 200
    rolled = await env.client.post(
        f"/v1/skills/{sid}/rollback", json={"revision": 1, "expected_version": 2}
    )
    assert rolled.status_code == 200
    assert rolled.json()["provenance"] == imported["provenance"]
    assert rolled.json()["content_sha256"] == imported["content_sha256"]


def test_crlf_manifest_is_supported_without_executing_any_resource():
    body = parse_package(archive({"SKILL.md": SKILL.replace("\n", "\r\n")}))
    assert body["instructions"] == "Use supplied evidence only."


def test_quarantine_and_indexes_are_never_projected_as_public_history():
    from app.core.durable_types import classify_key

    assert classify_key("tenant:acme:skills:import:one") is None
    assert classify_key("tenant:acme:skills:import-previews") is None
    assert classify_key("tenant:acme:skills:record:one")[0] == "skill"


async def test_detect_only_import_findings_cannot_trigger_internal_automation_work(env):
    class Detector:
        async def ascan(self, text, **kwargs):
            return ThreatScan(
                kwargs["event_type"],
                kwargs["location"],
                (
                    {
                        "finding_id": str(uuid4()),
                        "rule_id": "TEST-IMPORT",
                        "severity": "high",
                        "title": "Synthetic finding",
                        "confidence": "high",
                        "enforcement_action": "detected",
                        "block_candidate": True,
                    },
                ),
                False,
            )

    env.app.state.threat_detector = Detector()

    async def own_events(stream):
        return [
            (identity, fields)
            for identity, fields in await env.runtime._client.xrange(stream)
            if fields.get("tenant_id") == env.tenant
        ]

    before = await own_events("automation:internal-events")
    queue_before = await own_events("agent:goal:queue")
    candidate = await preview(env)
    response = await env.client.post(
        f"/v1/skill-imports/{candidate['import_id']}/confirm",
        json={"expected_content_sha256": candidate["content_sha256"]},
    )
    assert response.status_code == 200
    assert await own_events("automation:internal-events") == before
    assert await own_events("agent:goal:queue") == queue_before
