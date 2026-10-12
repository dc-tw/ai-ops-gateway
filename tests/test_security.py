from fastapi.testclient import TestClient

from app.core.config import Settings
from app.core.security import redact_pii
from app.main import create_app


class NeverGraph:
    async def astream(self, *args, **kwargs):
        raise AssertionError("blocked requests must not execute the graph")
        yield {}


class MinimalRedis:
    async def close(self) -> None:
        return None

    async def delete_conversation(self, conversation_id: str, tenant_id: str = "default") -> bool:
        return True


def test_pii_redaction_covers_email_phone_and_identifier() -> None:
    value = redact_pii("mail me@example.com or +1 (415) 555-1212, id 123-45-6789")
    assert "me@example.com" not in value
    assert "555-1212" not in value
    assert "123-45-6789" not in value


def test_api_key_rbac_rejects_viewer_admin_action() -> None:
    settings = Settings(
        app_env="production",
        api_keys_json=(
            '{"viewer-key":{"subject":"reader","tenant_id":"acme","role":"viewer"}}'
        ),
    )
    app = create_app(settings=settings, redis_runtime=MinimalRedis())
    with TestClient(app) as client:
        listed = client.get("/v1/models", headers={"x-api-key": "viewer-key"})
        deleted = client.delete(
            "/v1/conversations/private", headers={"x-api-key": "viewer-key"}
        )
        unauthenticated = client.get("/v1/models")

    assert listed.status_code == 200
    assert deleted.status_code == 403
    assert unauthenticated.status_code == 401


def test_content_policy_and_request_budget_reject_before_execution() -> None:
    settings = Settings(
        content_blocked_patterns=["forbidden-operation"],
        request_token_budget=256,
    )
    app = create_app(graph=NeverGraph(), settings=settings, redis_runtime=MinimalRedis())
    with TestClient(app) as client:
        blocked = client.post(
            "/v1/agent/stream", json={"query": "perform forbidden-operation"}
        )
        oversized = client.post("/v1/agent/stream", json={"query": "x" * 1_200})

    assert blocked.status_code == 403
    assert oversized.status_code == 413
