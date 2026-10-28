"""Same-origin browser authentication using OIDC authorization-code + PKCE."""

from __future__ import annotations

import asyncio
import hmac
from typing import Literal

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, ConfigDict, Field

from app.core.browser_auth import (
    CSRF_COOKIE,
    LOGIN_COOKIE,
    SESSION_COOKIE,
    browser_store,
)
from app.core.security import Principal, authenticate, authorize

router = APIRouter(prefix="/auth", tags=["browser-auth"])


class DevLogin(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    subject: str = Field(default="development", min_length=1, max_length=256)
    tenant_id: str = Field(default="default", min_length=1, max_length=64)
    role: Literal["viewer", "operator", "admin"] = "admin"


def _secure(request: Request) -> bool:
    return request.app.state.settings.app_env == "production"


def _set_session(response: RedirectResponse, request: Request, session_id: str, csrf: str) -> None:
    ttl = request.app.state.settings.ui_session_ttl_seconds
    response.set_cookie(
        SESSION_COOKIE,
        session_id,
        max_age=ttl,
        httponly=True,
        secure=_secure(request),
        samesite="lax",
        path="/",
    )
    response.set_cookie(
        CSRF_COOKIE,
        csrf,
        max_age=ttl,
        httponly=False,
        secure=_secure(request),
        samesite="strict",
        path="/",
    )


def _clear(response: RedirectResponse, request: Request) -> None:
    for name in (SESSION_COOKIE, CSRF_COOKIE, LOGIN_COOKIE):
        response.delete_cookie(name, path="/", secure=_secure(request), samesite="lax")


def decode_id_token(token: str, request: Request) -> dict[str, object]:
    settings = request.app.state.settings
    import jwt

    jwks = jwt.PyJWKClient(settings.oidc_jwks_url)
    key = jwks.get_signing_key_from_jwt(token)
    return jwt.decode(
        token,
        key.key,
        algorithms=["RS256", "ES256"],
        audience=settings.oidc_client_id,
        issuer=settings.oidc_issuer,
    )


@router.get("/config")
async def auth_config(request: Request) -> dict[str, bool]:
    settings = request.app.state.settings
    return {
        "oidc_enabled": bool(settings.oidc_authorization_url),
        "development_login_enabled": bool(
            settings.app_env == "development" and settings.browser_dev_login_enabled
        ),
    }


@router.get("/login")
async def login(request: Request) -> RedirectResponse:
    settings = request.app.state.settings
    if not settings.oidc_authorization_url:
        raise HTTPException(status_code=404, detail="Browser OIDC login is not configured")
    location, binding, _ = await browser_store(request).begin()
    response = RedirectResponse(location, status_code=303)
    response.set_cookie(
        LOGIN_COOKIE,
        binding,
        max_age=settings.ui_login_state_ttl_seconds,
        httponly=True,
        secure=_secure(request),
        samesite="lax",
        path="/auth/callback",
    )
    return response


@router.get("/callback")
async def callback(
    request: Request,
    code: str = Query(min_length=1, max_length=4_096),
    state: str = Query(min_length=1, max_length=256),
) -> RedirectResponse:
    settings = request.app.state.settings
    if not settings.oidc_token_url:
        raise HTTPException(status_code=404, detail="Browser OIDC login is not configured")
    login_state = await browser_store(request).consume_state(
        state, request.cookies.get(LOGIN_COOKIE, "")
    )
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": settings.oidc_redirect_uri,
        "client_id": settings.oidc_client_id,
        "code_verifier": login_state["verifier"],
    }
    if settings.oidc_client_secret:
        form["client_secret"] = settings.oidc_client_secret
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
            provider = await client.post(settings.oidc_token_url, data=form)
            provider.raise_for_status()
            token_data = provider.json()
        id_token = token_data.get("id_token") if isinstance(token_data, dict) else None
        if not isinstance(id_token, str) or len(id_token) > 32_768:
            raise ValueError("missing ID token")
        claims = await asyncio.to_thread(decode_id_token, id_token, request)
    except Exception as exc:
        raise HTTPException(status_code=401, detail="OIDC login failed") from exc
    if not hmac.compare_digest(str(claims.get("nonce", "")), login_state["nonce"]):
        raise HTTPException(status_code=401, detail="OIDC login failed")
    principal = Principal(
        subject=str(claims.get("sub", "")),
        tenant_id=str(claims.get(settings.oidc_tenant_claim, "")),
        role=str(claims.get(settings.oidc_role_claim, "viewer")),
        auth_method="browser_oidc",
    )
    # Reuse claim validation without accepting provider-controlled actor types.
    from app.core.security import _validate_principal

    principal = _validate_principal(
        principal.subject, principal.tenant_id, principal.role, "browser_oidc"
    )
    session_id, csrf = await browser_store(request).create(principal)
    response = RedirectResponse("/console/", status_code=303)
    response.delete_cookie(LOGIN_COOKIE, path="/auth/callback")
    _set_session(response, request, session_id, csrf)
    return response


@router.post("/development-login")
async def development_login(payload: DevLogin, request: Request) -> RedirectResponse:
    settings = request.app.state.settings
    if settings.app_env != "development" or not settings.browser_dev_login_enabled:
        raise HTTPException(status_code=404, detail="Development login is disabled")
    from app.core.security import _validate_principal

    principal = _validate_principal(
        payload.subject, payload.tenant_id, payload.role, "browser_development"
    )
    session_id, csrf = await browser_store(request).create(principal)
    response = RedirectResponse("/console/", status_code=303)
    _set_session(response, request, session_id, csrf)
    return response


@router.get("/session")
async def session(request: Request) -> dict[str, object]:
    try:
        principal = await authorize(request, "viewer")
    except HTTPException as exc:
        if exc.status_code == 401:
            return {"authenticated": False}
        raise
    if principal.auth_method == "development":
        # Keep legacy API convenience, but require the console's explicit,
        # opt-in development session instead of silently becoming an admin UI.
        return {"authenticated": False}
    return {
        "authenticated": True,
        "subject": principal.subject,
        "tenant_id": principal.tenant_id,
        "role": principal.role,
        "actor_type": principal.actor_type,
        "auth_method": principal.auth_method,
        "resource_acl_enabled": request.app.state.settings.resource_acl_enabled,
    }


@router.post("/logout")
async def logout(request: Request) -> RedirectResponse:
    await authenticate(request)
    # authenticate() sets this only for a live server-side browser session.
    session_id = getattr(request.state, "browser_session_id", "")
    if not session_id:
        raise HTTPException(status_code=400, detail="Browser session required")
    from app.core.browser_auth import csrf_valid

    if not csrf_valid(request):
        raise HTTPException(status_code=403, detail="Valid CSRF token required")
    await browser_store(request).delete(session_id)
    response = RedirectResponse("/console/", status_code=303)
    _clear(response, request)
    return response
