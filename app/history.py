"""Tenant-scoped archival queries, deliberately separate from live controls."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, HTTPException, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr, model_validator

from app.core.durable_types import EventKind, RecordKind
from app.core.postgres_schema import SCHEMA_VERSION
from app.core.postgres_store import PostgresConflict
from app.core.resource_access import filter_history, history_permissions, require_access
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
    execute: StrictBool = False
    expected_preview_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def explicit_execution(self):
        if self.execute != (self.expected_preview_sha256 is not None):
            raise ValueError("Execution requires exactly one matching preview hash")
        return self


class PoisonRepair(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    expected_payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    replacement_envelope: dict[str, Any]


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
    except PostgresConflict:
        raise HTTPException(status_code=409, detail="History state changed; retry") from None
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
    if (
        not isinstance(migration, dict)
        or migration.get("status") != "verified"
        or migration.get("schema_version") != SCHEMA_VERSION
    ):
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
        "retention_operations",
        "latest_cursor",
        "source_cursor",
    }
    public = {name: value for name, value in status.items() if name in names}
    migration = status.get("migration")
    if isinstance(migration, dict):
        public["migration"] = {
            key: migration[key]
            for key in ("report_id", "saved_at", "status", "schema_version")
            if key in migration
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
        store,
        "list_records",
        principal.tenant_id,
        kind=kind or "",
        after=after,
        limit=limit,
        **(await history_permissions(request, principal)),
    )
    rows = await filter_history(request, principal, rows)
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
    await require_access(request, principal, kind, resource_id)
    record = await _call(store, "get_record", principal.tenant_id, kind, resource_id)
    if record is None:
        raise HTTPException(status_code=404, detail="History record not found")
    await require_access(request, principal, kind, resource_id)
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
        **(await history_permissions(request, principal)),
    )
    rows = await filter_history(request, principal, rows)
    return _page("events", rows, limit)


@router.get("/search")
async def search_history(
    request: Request,
    q: Annotated[str, Query(min_length=1, max_length=200)],
    kind: HistoryKind | None = None,
    scope: Literal["all", "records", "events"] = "all",
    include_summary: bool = False,
    expand: Literal["none", "relations"] = "none",
    after: Annotated[str, Query(max_length=512)] = "",
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> dict[str, Any]:
    principal = await authorize(request, "viewer")
    _query(
        request,
        {"q", "kind", "scope", "include_summary", "expand", "after", "limit"},
    )
    if not q.strip():
        raise HTTPException(status_code=422, detail="Invalid history request")
    store = await _read_store(request)
    rows = await _call(
        store,
        "search_documents",
        principal.tenant_id,
        q,
        kind=kind or "",
        scope=scope,
        after=after,
        limit=limit,
        include_relations=expand == "relations",
        **(await history_permissions(request, principal)),
    )
    rows = await filter_history(request, principal, rows)
    for row in rows:
        candidates = row.pop("_relation_candidates", [])
        if expand != "relations":
            continue
        relations = []
        for candidate in candidates:
            try:
                await require_access(
                    request,
                    principal,
                    candidate["kind"],
                    candidate["resource_id"],
                )
            except HTTPException as exc:
                if exc.status_code == 404:
                    continue
                raise
            relations.append(candidate)
        row["relations"] = relations
    page = _page("results", rows, limit)
    if include_summary:
        page["summary"] = {
            "returned": len(rows),
            "by_document_type": {
                name: sum(row.get("document_type") == name for row in rows)
                for name in ("record", "event")
            },
            "by_kind": {
                name: sum(row.get("kind") == name for row in rows)
                for name in sorted({row.get("kind") for row in rows if row.get("kind")})
            },
            "acl_rechecked": True,
        }
    return page


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
    method = "purge" if payload.execute else "preview_purge"
    kwargs = {"kind": payload.kind, "before_cursor": payload.before_cursor}
    if payload.execute:
        kwargs["expected_preview_sha256"] = payload.expected_preview_sha256
        kwargs["subject"] = principal.subject
    result = await _call(_store(request), method, principal.tenant_id, **kwargs)
    return _response({"purge": result, "source_cursor": result.get("source_cursor")})


@router.get("/poison")
async def list_poison(
    request: Request,
    after: Annotated[str, Query(pattern=r"^[0-9]+-[0-9]+$", max_length=64)] = "0-0",
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    _query(request, {"after", "limit"})
    runtime = request.app.state.redis
    if runtime is None or not hasattr(runtime, "_client"):
        raise HTTPException(status_code=503, detail="History projection diagnostics unavailable")
    try:
        from app.postgres_worker import diagnose_poison

        result = await diagnose_poison(
            runtime,
            request.app.state.settings.postgres_source_id,
            after=after,
            limit=limit,
            tenant_id=principal.tenant_id,
        )
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid history request") from None
    except Exception:
        raise HTTPException(
            status_code=503, detail="History projection diagnostics unavailable"
        ) from None
    return _response({**result, "source_cursor": result.get("next_after")})


@router.post("/poison/{event_id}/repair")
async def repair_poison_entry(
    payload: PoisonRepair,
    request: Request,
    event_id: str = Path(pattern=r"^[0-9]+-[0-9]+$", max_length=64),
) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    _query(request, set())
    runtime = request.app.state.redis
    if runtime is None or not hasattr(runtime, "_client"):
        raise HTTPException(status_code=503, detail="History projection repair unavailable")
    try:
        from app.postgres_worker import repair_poison

        result = await repair_poison(
            runtime,
            _store(request),
            event_id,
            payload.expected_payload_sha256,
            payload.replacement_envelope,
            principal.subject,
            tenant_id=principal.tenant_id,
        )
    except PostgresConflict:
        raise HTTPException(status_code=409, detail="History state changed; retry") from None
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid history repair") from None
    except Exception:
        raise HTTPException(
            status_code=503, detail="History projection repair unavailable"
        ) from None
    return _response({"repair": result, "source_cursor": event_id})
