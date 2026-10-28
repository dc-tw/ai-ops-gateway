"""Server-side browser login state and sessions; provider tokens are never persisted."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode

from fastapi import HTTPException, Request

from app.core.security import Principal, _validate_principal

SESSION_COOKIE = "gateway_session"
CSRF_COOKIE = "gateway_csrf"
LOGIN_COOKIE = "gateway_login"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _token(size: int = 32) -> str:
    return secrets.token_urlsafe(size)


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


class BrowserSessionStore:
    def __init__(self, runtime: Any, settings: Any) -> None:
        if runtime is None or not hasattr(runtime, "_client"):
            raise HTTPException(status_code=503, detail="Browser login requires Redis")
        self.client = runtime._client
        self.settings = settings

    @staticmethod
    def state_key(state: str) -> str:
        return f"gateway:browser-auth:state:{_digest(state)}"

    @staticmethod
    def session_key(session_id: str) -> str:
        return f"gateway:browser-auth:session:{_digest(session_id)}"

    async def begin(self) -> tuple[str, str, str]:
        state, binding, verifier, nonce = _token(), _token(), _token(48), _token()
        challenge = _b64url(hashlib.sha256(verifier.encode("ascii")).digest())
        record = {
            "schema_version": 1,
            "binding_sha256": _digest(binding),
            "verifier": verifier,
            "nonce": nonce,
            "created_at": datetime.now(UTC).isoformat(),
        }
        created = await self.client.set(
            self.state_key(state),
            json.dumps(record, separators=(",", ":")),
            ex=self.settings.ui_login_state_ttl_seconds,
            nx=True,
        )
        if not created:
            raise HTTPException(status_code=503, detail="Browser login state unavailable")
        query = urlencode(
            {
                "response_type": "code",
                "client_id": self.settings.oidc_client_id,
                "redirect_uri": self.settings.oidc_redirect_uri,
                "scope": self.settings.oidc_scopes,
                "state": state,
                "nonce": nonce,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
            }
        )
        return f"{self.settings.oidc_authorization_url}?{query}", binding, state

    async def consume_state(self, state: str, binding: str) -> dict[str, Any]:
        if not state or not binding or len(state) > 256 or len(binding) > 256:
            raise HTTPException(status_code=400, detail="Invalid login response")
        raw = await self.client.execute_command("GETDEL", self.state_key(state))
        try:
            record = json.loads(raw) if raw else None
        except (TypeError, ValueError):
            record = None
        if (
            not isinstance(record, dict)
            or record.get("schema_version") != 1
            or not hmac.compare_digest(str(record.get("binding_sha256", "")), _digest(binding))
            or not isinstance(record.get("verifier"), str)
            or not isinstance(record.get("nonce"), str)
        ):
            raise HTTPException(status_code=400, detail="Invalid or expired login response")
        return record

    async def create(self, principal: Principal) -> tuple[str, str]:
        session_id, csrf = _token(48), _token()
        record = {
            "schema_version": 1,
            "subject": principal.subject,
            "tenant_id": principal.tenant_id,
            "role": principal.role,
            "actor_type": principal.actor_type,
            "auth_method": principal.auth_method,
            "csrf_sha256": _digest(csrf),
            "created_at": datetime.now(UTC).isoformat(),
        }
        created = await self.client.set(
            self.session_key(session_id),
            json.dumps(record, separators=(",", ":")),
            ex=self.settings.ui_session_ttl_seconds,
            nx=True,
        )
        if not created:
            raise HTTPException(status_code=503, detail="Browser session unavailable")
        return session_id, csrf

    async def get(self, session_id: str) -> tuple[Principal, str] | None:
        if not session_id or len(session_id) > 512:
            return None
        raw = await self.client.get(self.session_key(session_id))
        try:
            record = json.loads(raw) if raw else None
        except (TypeError, ValueError):
            return None
        if (
            not isinstance(record, dict)
            or record.get("schema_version") != 1
            or record.get("actor_type") != "human"
            or not isinstance(record.get("csrf_sha256"), str)
        ):
            return None
        principal = _validate_principal(
            str(record.get("subject", "")),
            str(record.get("tenant_id", "")),
            str(record.get("role", "")),
            str(record.get("auth_method", "browser_oidc")),
        )
        return principal, record["csrf_sha256"]

    async def delete(self, session_id: str) -> None:
        if session_id and len(session_id) <= 512:
            await self.client.delete(self.session_key(session_id))


def browser_store(request: Request) -> BrowserSessionStore:
    return BrowserSessionStore(request.app.state.redis, request.app.state.settings)


def csrf_valid(request: Request) -> bool:
    expected = getattr(request.state, "browser_csrf_sha256", "")
    header = request.headers.get("x-csrf-token", "")
    cookie = request.cookies.get(CSRF_COOKIE, "")
    return bool(
        expected
        and header
        and cookie
        and len(header) <= 512
        and hmac.compare_digest(header, cookie)
        and hmac.compare_digest(_digest(header), expected)
    )
