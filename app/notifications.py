from __future__ import annotations

from fastapi import APIRouter, HTTPException, Path, Query, Request

from app.core.outbound_notifications import NotificationDelivery
from app.core.security import authorize

router = APIRouter(prefix="/v1/notifications", tags=["notifications"])
STREAM_ID = r"^\d+-\d+$"


def _delivery(request: Request) -> NotificationDelivery:
    if request.app.state.redis is None or not hasattr(request.app.state.redis, "_client"):
        raise HTTPException(status_code=503, detail="Notification queue requires Redis")
    return NotificationDelivery(request.app.state.redis, request.app.state.settings)


@router.get("/failures")
async def notification_failures(
    request: Request, limit: int = Query(default=100, ge=1, le=100)
) -> dict[str, object]:
    principal = await authorize(request, "admin")
    return {
        "failures": await _delivery(request).failures(principal.tenant_id, limit),
        "contains_credentials": False,
    }


@router.post("/failures/{failure_id}/redrive")
async def redrive_notification(
    request: Request, failure_id: str = Path(pattern=STREAM_ID)
) -> dict[str, object]:
    principal = await authorize(request, "admin")
    event = await _delivery(request).redrive(
        principal.tenant_id, failure_id, principal.subject
    )
    if event is None:
        raise HTTPException(status_code=404, detail="Notification failure not found")
    return {"redriven": True, "event_id": event["event_id"]}
