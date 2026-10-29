from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from fastapi import HTTPException, Request, status

if TYPE_CHECKING:
    from app.core.config import Settings


ROLE_LEVEL = {"viewer": 1, "operator": 2, "admin": 3}
TENANT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


@dataclass(frozen=True, slots=True)
class Principal:
    subject: str
    tenant_id: str
    role: str
    auth_method: str
    actor_type: str = "human"
    credential_id: str = ""


def redact_pii(text: str) -> str:
    patterns = [
        (r"(?<![\w.+-])[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}(?![\w.-])", "[EMAIL_REDACTED]"),
        (r"(?<!\d)(?:\+?\d[\d .()-]{7,}\d)(?!\d)", "[PHONE_REDACTED]"),
        (r"(?<!\d)\d{3}-\d{2}-\d{4}(?!\d)", "[ID_REDACTED]"),
    ]
    for pattern, replacement in patterns:
        text = re.sub(pattern, replacement, text)
    return text


def _validate_principal(
    subject: str,
    tenant_id: str,
    role: str,
    method: str,
    actor_type: str = "human",
) -> Principal:
    if not subject or len(subject) > 256 or not subject.strip():
        raise HTTPException(status_code=401, detail="Invalid subject claim")
    if not TENANT_PATTERN.fullmatch(tenant_id):
        raise HTTPException(status_code=401, detail="Invalid tenant claim")
    if role not in ROLE_LEVEL:
        raise HTTPException(status_code=403, detail="Invalid role claim")
    if actor_type not in {"human", "agent"}:
        raise HTTPException(status_code=401, detail="Invalid actor type")
    return Principal(
        subject=subject,
        tenant_id=tenant_id,
        role=role,
        auth_method=method,
        actor_type=actor_type,
    )


async def authenticate(request: Request) -> Principal:
    settings: Settings = request.app.state.settings
    api_key = request.headers.get("x-api-key")
    agent_key = request.headers.get("x-agent-key")
    if agent_key:
        if api_key or request.headers.get("authorization"):
            raise HTTPException(status_code=400, detail="Ambiguous authentication credentials")
        runtime = getattr(request.app.state, "redis", None)
        if runtime is None or not hasattr(runtime, "_client"):
            raise HTTPException(status_code=503, detail="Agent authentication requires Redis")
        from app.core.agent_credentials import AgentCredentialStore

        record = await AgentCredentialStore(runtime).resolve(agent_key)
        if record is None:
            raise HTTPException(status_code=401, detail="Invalid Agent credential")
        return Principal(
            subject=str(record["agent_id"]),
            tenant_id=str(record["tenant_id"]),
            role="operator",
            auth_method="agent_credential",
            actor_type="agent",
            credential_id=str(record["credential_id"]),
        )
    if api_key:
        if settings.api_keys_json.strip():
            configured = json.loads(settings.api_keys_json)
            record = configured.get(api_key) if isinstance(configured, dict) else None
            if isinstance(record, dict):
                return _validate_principal(
                    str(record.get("subject", "api-key")),
                    str(record.get("tenant_id", "default")),
                    str(record.get("role", "operator")),
                    "api_key",
                    str(record.get("actor_type", "human")),
                )
        if settings.api_key and api_key == settings.api_key:
            return Principal("legacy-api-key", "default", "admin", "api_key")

    authorization = request.headers.get("authorization", "")
    if authorization.lower().startswith("bearer ") and settings.oidc_jwks_url:
        token = authorization.split(None, 1)[1]

        def decode() -> dict[str, object]:
            import jwt

            jwks = jwt.PyJWKClient(settings.oidc_jwks_url)
            signing_key = jwks.get_signing_key_from_jwt(token)
            return jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256", "ES256"],
                audience=settings.oidc_audience or None,
                issuer=settings.oidc_issuer or None,
                options={
                    "verify_aud": bool(settings.oidc_audience),
                    "verify_iss": bool(settings.oidc_issuer),
                },
            )

        try:
            claims = await asyncio.to_thread(decode)
        except Exception as exc:
            raise HTTPException(status_code=401, detail="Invalid bearer token") from exc
        return _validate_principal(
            str(claims.get("sub", "oidc-user")),
            str(claims.get(settings.oidc_tenant_claim, "default")),
            str(claims.get(settings.oidc_role_claim, "viewer")),
            "oidc",
        )

    session_id = request.cookies.get("gateway_session", "")
    if session_id:
        from app.core.browser_auth import browser_store

        resolved = await browser_store(request).get(session_id)
        if resolved is not None:
            principal, csrf_sha256 = resolved
            request.state.browser_session_id = session_id
            request.state.browser_csrf_sha256 = csrf_sha256
            return principal

    if settings.app_env == "development" and not (
        settings.api_key or settings.api_keys_json or settings.oidc_jwks_url
    ):
        return Principal("development", "default", "admin", "development")
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")


async def authorize(request: Request, minimum_role: str = "viewer") -> Principal:
    principal = await authenticate(request)
    if principal.auth_method in {"browser_oidc", "browser_development"} and request.method not in {
        "GET",
        "HEAD",
        "OPTIONS",
    }:
        from app.core.browser_auth import csrf_valid

        if not csrf_valid(request):
            raise HTTPException(status_code=403, detail="Valid CSRF token required")
    if ROLE_LEVEL[principal.role] < ROLE_LEVEL[minimum_role]:
        raise HTTPException(status_code=403, detail=f"{minimum_role} role required")
    if principal.auth_method == "agent_credential":
        own = f"/v1/agents/{principal.subject}"
        goal_path = request.url.path.startswith("/v1/goals/")
        goal_control = request.url.path.endswith(("/cancel", "/pause", "/resume"))
        allowed = (
            request.method == "GET" and request.url.path == own
        ) or (
            request.method == "POST" and request.url.path == own + "/goals"
        ) or (
            request.method == "GET" and request.url.path == "/auth/session"
        ) or (
            request.method == "GET"
            and (request.url.path == "/v1/goals" or goal_path)
        ) or (
            request.method == "POST" and goal_path and goal_control
        )
        if not allowed:
            raise HTTPException(status_code=403, detail="Agent credential route is not allowed")
    from app.core.resource_access import authorize_resource_route

    await authorize_resource_route(request, principal)
    return principal
