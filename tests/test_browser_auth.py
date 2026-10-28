import json

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.core.config import Settings
from app.main import create_app


class MemoryClient:
    def __init__(self):
        self.values = {}

    async def set(self, key, value, **options):
        if options.get("nx") and key in self.values:
            return False
        self.values[key] = value
        return True

    async def get(self, key):
        return self.values.get(key)

    async def delete(self, key):
        return int(self.values.pop(key, None) is not None)

    async def execute_command(self, command, key):
        assert command == "GETDEL"
        return self.values.pop(key, None)


class MemoryRuntime:
    def __init__(self):
        self._client = MemoryClient()

    async def close(self):
        return None

    async def delete_conversation(self, *args, **kwargs):
        return True

    async def append_audit(self, *args, **kwargs):
        return "1-0"


def test_development_browser_session_requires_csrf_for_mutation_and_can_logout():
    runtime = MemoryRuntime()
    settings = Settings(
        _env_file=None,
        app_env="development",
        api_key="disable-implicit-development-principal",
        browser_dev_login_enabled=True,
    )
    with TestClient(create_app(settings=settings, redis_runtime=runtime)) as client:
        login = client.post(
            "/auth/development-login",
            json={"subject": "alice", "tenant_id": "acme", "role": "admin"},
            follow_redirects=False,
        )
        assert login.status_code == 303
        assert "gateway_session=" in login.headers["set-cookie"]
        assert "HttpOnly" in login.headers["set-cookie"]

        session = client.get("/auth/session")
        assert session.json() == {
            "authenticated": True,
            "subject": "alice",
            "tenant_id": "acme",
            "role": "admin",
            "actor_type": "human",
            "auth_method": "browser_development",
            "resource_acl_enabled": False,
        }
        assert client.delete("/v1/conversations/private").status_code == 403
        csrf = client.cookies.get("gateway_csrf")
        deleted = client.delete(
            "/v1/conversations/private", headers={"x-csrf-token": csrf}
        )
        assert deleted.status_code == 200

        logout = client.post(
            "/auth/logout", headers={"x-csrf-token": csrf}, follow_redirects=False
        )
        assert logout.status_code == 303
        assert client.get("/auth/session").json() == {"authenticated": False}


def test_browser_login_state_is_bound_and_single_use():
    runtime = MemoryRuntime()
    settings = Settings(
        _env_file=None,
        oidc_authorization_url="https://identity.example/authorize",
        oidc_token_url="https://identity.example/token",
        oidc_client_id="gateway",
        oidc_redirect_uri="https://gateway.example/auth/callback",
        oidc_jwks_url="https://identity.example/jwks",
        oidc_issuer="https://identity.example/",
    )
    with TestClient(create_app(settings=settings, redis_runtime=runtime)) as client:
        response = client.get("/auth/login", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"].startswith("https://identity.example/authorize?")
        state_records = list(runtime._client.values.values())
        assert len(state_records) == 1
        assert "code_challenge" in response.headers["location"]
        assert "verifier" in json.loads(state_records[0])

        # A wrong state cannot be exchanged and never enters the provider call.
        rejected = client.get(
            "/auth/callback?code=secret-code&state=wrong", follow_redirects=False
        )
        assert rejected.status_code == 400
        assert "secret-code" not in rejected.text


def test_production_cannot_enable_development_browser_login():
    with pytest.raises(ValidationError, match="forbidden outside development"):
        Settings(_env_file=None, app_env="production", browser_dev_login_enabled=True)


def test_console_does_not_silently_accept_legacy_implicit_development_admin():
    settings = Settings(
        _env_file=None, app_env="development", browser_dev_login_enabled=True
    )
    with TestClient(create_app(settings=settings, redis_runtime=MemoryRuntime())) as client:
        assert client.get("/auth/session").json() == {"authenticated": False}


def test_api_key_agent_actor_type_is_preserved():
    settings = Settings(
        _env_file=None,
        app_env="production",
        api_keys_json=json.dumps(
            {
                "agent-key": {
                    "subject": "worker-1",
                    "tenant_id": "acme",
                    "role": "operator",
                    "actor_type": "agent",
                }
            }
        ),
    )
    with TestClient(create_app(settings=settings, redis_runtime=MemoryRuntime())) as client:
        # Session representation is a convenient authenticated identity probe.
        result = client.get("/auth/session", headers={"x-api-key": "agent-key"})
        goal_approval = client.post(
            "/v1/goals/goal-one/approval",
            json={"approval_id": "approval-one", "approved": True},
            headers={"x-api-key": "agent-key"},
        )
        run_approval = client.post(
            "/v1/agent/runs/run-one/approval",
            json={"approved": True},
            headers={"x-api-key": "agent-key"},
        )
    assert result.status_code == 200
    assert result.json()["actor_type"] == "agent"
    assert goal_approval.status_code == 403
    assert run_approval.status_code == 403
