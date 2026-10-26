"""Tenant-scoped archival queries, deliberately separate from live controls."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, HTTPException, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field, StrictStr

from app.core.durable_types import EventKind, RecordKind
from app.core.security import authorize

HistoryKind = Literal[
    "agent",
    "skill",
    "skill_revision",
    "planning_session",
    "planning_revision",
    "goal",
    "run",
    "finding",
    "automation",
    "tool_receipt",
    "conversation",
    "planning",
    "audit",
]


class HistoryRoute(APIRoute):
    """Never reflect private search text or validation input in public errors."""

    def get_route_handler(self):
        original = super().get_route_handler()

        async def handle(request: Request):
            try:
                return await original(request)
            except RequestValidationError:
                return JSONResponse(status_code=422, content={"detail": "Invalid history request"})

        return handle


router = APIRouter(prefix="/v1/history", tags=["history"], route_class=HistoryRoute)


class PurgeHistory(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    kind: HistoryKind
    before_cursor: StrictStr = Field(pattern=r"^[0-9]+-[0-9]+$", max_length=64)


def _query(request: Request, allowed: set[str]) -> None:
    """Reject unsupported tenant/SQL knobs and ambiguous repeated parameters."""
    seen: set[str] = set()
    for name, value in request.query_params.multi_items():
        if name not in allowed or name in seen:
            raise HTTPException(status_code=422, detail="Invalid history request")
        seen.add(name)
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            raise HTTPException(status_code=422, detail="Invalid history request") from None


def _store(request: Request):
    store = getattr(request.app.state, "postgres", None)
    if store is None:
        raise HTTPException(status_code=503, detail="History storage unavailable")
    return store


async def _call(store: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    try:
        return await getattr(store, method)(*args, **kwargs)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid history request") from None
    except Exception:
        # DSNs, SQL fragments and archived payloads must not reach HTTP errors.
        raise HTTPException(status_code=503, detail="History storage unavailable") from None


async def _read_store(request: Request):
    store = _store(request)
    if getattr(request.app.state.settings, "postgres_history_mode", "shadow") != "postgres":
        raise HTTPException(status_code=409, detail="History read cutover is not enabled")
    migration = await _call(store, "latest_migration")
    if not isinstance(migration, dict) or migration.get("status") != "verified":
        raise HTTPException(status_code=409, detail="History migration is not verified")
    return store


def _response(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        **payload,
        "source_cursor": payload.get("source_cursor"),
        "history_only": True,
        "consistency": "eventual",
        "execution_authority": False,
    }


def _page(name: str, rows: list[dict[str, Any]], limit: int) -> dict[str, Any]:
    return _response(
        {
            name: rows,
            "next_cursor": rows[-1].get("cursor") if len(rows) == limit else None,
            "source_cursor": rows[-1].get("source_cursor") if rows else None,
        }
    )


def _status(status: dict[str, Any]) -> dict[str, Any]:
    """Keep future internal diagnostic fields out of public status responses."""
    names = {
        "source_id",
        "schema_version",
        "records",
        "events",
        "applied_operations",
        "tombstones",
        "latest_cursor",
        "source_cursor",
    }
    public = {name: value for name, value in status.items() if name in names}
    migration = status.get("migration")
    if isinstance(migration, dict):
        public["migration"] = {
            key: migration[key] for key in ("report_id", "saved_at", "status") if key in migration
        }
    return public


@router.get("/records")
async def list_history_records(
    request: Request,
    kind: RecordKind | None = None,
    after: Annotated[str, Query(max_length=512)] = "",
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    _query(request, {"kind", "after", "limit"})
    store = await _read_store(request)
    rows = await _call(
        store, "list_records", principal.tenant_id, kind=kind or "", after=after, limit=limit
    )
    return _page("records", rows, limit)


@router.get("/records/{kind}/{resource_id:path}")
async def get_history_record(
    kind: RecordKind,
    resource_id: Annotated[str, Path(min_length=1, max_length=512)],
    request: Request,
) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    _query(request, set())
    try:
        resource_id.encode("utf-8")
    except UnicodeEncodeError:
        raise HTTPException(status_code=422, detail="Invalid history request") from None
    store = await _read_store(request)
    record = await _call(store, "get_record", principal.tenant_id, kind, resource_id)
    if record is None:
        raise HTTPException(status_code=404, detail="History record not found")
    return _response({"record": record, "source_cursor": record.get("source_cursor")})


@router.get("/events")
async def history_events(
    request: Request,
    kind: EventKind | None = None,
    resource_id: Annotated[str, Query(max_length=512)] = "",
    after: Annotated[int, Query(ge=0, le=9_223_372_036_854_775_807)] = 0,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    _query(request, {"kind", "resource_id", "after", "limit"})
    store = await _read_store(request)
    rows = await _call(
        store,
        "events",
        principal.tenant_id,
        kind=kind or "",
        resource_id=resource_id,
        after=after,
        limit=limit,
    )
    return _page("events", rows, limit)


@router.get("/search")
async def search_history(
    request: Request,
    q: Annotated[str, Query(min_length=1, max_length=200)],
    kind: RecordKind | None = None,
    after: Annotated[str, Query(max_length=512)] = "",
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    _query(request, {"q", "kind", "after", "limit"})
    if not q.strip():
        raise HTTPException(status_code=422, detail="Invalid history request")
    store = await _read_store(request)
    rows = await _call(
        store, "search", principal.tenant_id, q, kind=kind or "", after=after, limit=limit
    )
    return _page("results", rows, limit)


@router.get("/status")
async def history_status(request: Request) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    _query(request, set())
    status = _status(await _call(_store(request), "status", principal.tenant_id))
    return _response(
        {
            "status": status,
            "source_cursor": status.get("source_cursor", status.get("latest_cursor")),
        }
    )


@router.post("/purge")
async def purge_history(payload: PurgeHistory, request: Request) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    _query(request, set())
    result = await _call(
        _store(request),
        "purge",
        principal.tenant_id,
        kind=payload.kind,
        before_cursor=payload.before_cursor,
    )
    return _response({"purge": result, "source_cursor": result.get("source_cursor")})
