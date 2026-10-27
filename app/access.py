"""Explicit ownership/grant management; no automatic legacy adoption."""

from fastapi import APIRouter, HTTPException, Path, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from app.core.resource_access import (
    AccessUpdate,
    ResourceKind,
    access_store,
    enforcement,
    resource_keys,
)
from app.core.security import authorize


class AccessRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def handle(request):
            try:
                return await original(request)
            except RequestValidationError:
                return JSONResponse(status_code=422, content={"detail": "Invalid access request"})
            except HTTPException:
                raise
            except Exception:
                return JSONResponse(
                    status_code=503, content={"detail": "Resource authorization unavailable"}
                )

        return handle


router = APIRouter(prefix="/v1/access", tags=["resource-access"], route_class=AccessRoute)


def _store(request):
    if not enforcement(request):
        raise HTTPException(status_code=409, detail="Resource ACL enforcement is disabled")
    return access_store(request)


@router.get("/legacy-preview/{kind}/{resource_id}")
async def preview_legacy(
    kind: ResourceKind,
    request: Request,
    resource_id: str = Path(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"),
):
    principal = await authorize(request, "admin")
    store = _store(request)
    current = await store.get(principal.tenant_id, kind, resource_id)
    exists = bool(await store.client.exists(*resource_keys(principal.tenant_id, kind, resource_id)))
    return {
        "kind": kind,
        "resource_id": resource_id,
        "exists": exists,
        "has_acl": current is not None,
        "can_adopt": exists and current is None,
        "writes": False,
        "requires_explicit_owner": True,
    }


@router.get("/{kind}/{resource_id}")
async def get_access(
    kind: ResourceKind,
    request: Request,
    resource_id: str = Path(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"),
):
    principal = await authorize(request, "viewer")
    store = _store(request)
    value = await store.get(principal.tenant_id, kind, resource_id)
    owner = {"subject": principal.subject, "actor_type": principal.actor_type}
    if not value or (principal.role != "admin" and value["owner"] != owner):
        raise HTTPException(status_code=404, detail="Resource not found")
    return value


@router.put("/{kind}/{resource_id}")
async def update_access(
    kind: ResourceKind,
    body: AccessUpdate,
    request: Request,
    resource_id: str = Path(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"),
):
    principal = await authorize(request, "operator")
    return await _store(request).update(principal, kind, resource_id, body)
