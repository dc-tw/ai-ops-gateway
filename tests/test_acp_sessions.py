import json
import os
from uuid import uuid4

import httpx
import pytest

from app.core.config import Settings
from app.core.redis_runtime import RedisRuntime
from app.main import create_app


@pytest.fixture
async def runtime():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Real Redis integration requires TEST_REDIS_URL")
    value = RedisRuntime(url)
    await value.ping()
    try:
        yield value
    finally:
        await value.close()


async def test_acp_session_api_is_tenant_scoped_versioned_and_mode_has_no_execution(runtime):
    tenant = f"acp-{uuid4().hex}"
    keys = {
        "human": {"subject": "alice", "tenant_id": tenant, "role": "operator"},
        "other": {"subject": "mallory", "tenant_id": tenant, "role": "operator"},
    }
    settings = Settings(
        _env_file=None,
        app_env="production",
        redis_url=os.environ["TEST_REDIS_URL"],
        api_keys_json=json.dumps(keys),
        resource_acl_enabled=True,
    )
    app = create_app(settings=settings, redis_runtime=runtime)
    # ASGITransport deliberately skips the application lifespan so this test
    # exercises only the ACP API/Redis path, not checkpointer bootstrap.
    app.state.settings = settings
    app.state.redis = runtime
    app.state.postgres = None
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        created = await client.post(
            "/v1/acp/sessions",
            json={"cwd_sha256": "a" * 64, "client_name": "reference-client"},
            headers={"x-api-key": "human"},
        )
        assert created.status_code == 200, created.text
        body = created.json()
        session_id = body["session"]["session_id"]
        assert body["execution_started"] is False
        assert "cwd" not in body["session"]
        loaded = await client.get(f"/v1/acp/sessions/{session_id}", headers={"x-api-key": "human"})
        assert loaded.status_code == 200, loaded.text
        metadata_only = await client.get(
            f"/v1/acp/sessions/{session_id}?include_history=false",
            headers={"x-api-key": "human"},
        )
        assert metadata_only.status_code == 200, metadata_only.text
        assert metadata_only.json()["history"] == []
        denied = await client.get(f"/v1/acp/sessions/{session_id}", headers={"x-api-key": "other"})
        assert denied.status_code == 404
        changed = await client.put(
            f"/v1/acp/sessions/{session_id}/mode",
            json={"expected_version": 1, "mode": "execute"},
            headers={"x-api-key": "human"},
        )
        assert changed.status_code == 200, changed.text
        assert changed.json()["execution_started"] is False
        assert changed.json()["approval_granted"] is False
        conflict = await client.put(
            f"/v1/acp/sessions/{session_id}/mode",
            json={"expected_version": 1, "mode": "plan"},
            headers={"x-api-key": "human"},
        )
        assert conflict.status_code == 409
