"""Administrator-only quarantine previews and atomic confirmation of read-only Skills."""

import asyncio
import base64
import binascii
import json
import time
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Path, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from redis.exceptions import WatchError

from app.core.security import authorize
from app.core.skill_packages import COMMIT, REPOSITORY, fetch_github, parse_package, subdirectory
from app.core.skill_store import (
    SkillConflict,
    SkillContent,
    SkillNotFound,
    SkillStore,
    snapshot_content_hash,
)
from app.skills import SkillRoute, _check_content


class ImportRoute(SkillRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def handle(request):
            try:
                response = await original(request)
                response.headers["Cache-Control"] = "no-store"
                return response
            except HTTPException:
                raise
            except Exception:
                return JSONResponse(status_code=503, content={"detail": "Skill import unavailable"})

        return handle


router = APIRouter(prefix="/v1/skill-imports", tags=["skill-imports"], route_class=ImportRoute)


class ImportInput(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)


class UploadImport(ImportInput):
    archive_base64: str = Field(min_length=1, max_length=350_000)
    subdirectory: str = Field(default="", max_length=128)


class GithubImport(ImportInput):
    repository: str = Field(pattern=REPOSITORY.pattern, max_length=201)
    commit: str = Field(pattern=COMMIT.pattern)
    subdirectory: str = Field(default="", max_length=128)


class ConfirmImport(ImportInput):
    expected_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


def _runtime(request):
    settings = request.app.state.settings
    if not settings.skill_v2_enabled or not settings.skill_package_import_enabled:
        raise HTTPException(status_code=409, detail="Skill package import is disabled")
    runtime = request.app.state.redis
    if runtime is None or not hasattr(runtime, "_client"):
        raise HTTPException(status_code=503, detail="Skill import requires Redis")
    return runtime


def preview_key(tenant, import_id):
    return f"tenant:{tenant}:skills:import:{import_id}"


async def _admit(request, principal):
    runtime = _runtime(request)
    try:
        await runtime.enforce_rate_limit(
            principal.tenant_id, request.app.state.settings.tenant_requests_per_minute
        )
    except RuntimeError:
        raise HTTPException(status_code=429, detail="Import request rate exceeded") from None


async def _load(runtime, tenant, import_id):
    raw = await runtime._client.get(preview_key(tenant, import_id))
    if not raw:
        raise HTTPException(status_code=404, detail="Import preview unavailable or expired")
    try:
        value = json.loads(raw)
        SkillContent.model_validate(value["content"])
        if (
            type(value.get("schema_version")) is not int
            or value["schema_version"] != 1
            or value.get("executes_code") is not False
            or value["tenant_id"] != tenant
            or value["import_id"] != import_id
            or value["content_sha256"] != snapshot_content_hash(value["content"])
            or value["status"] not in {"previewed", "published"}
        ):
            raise ValueError
        return value
    except Exception:
        raise HTTPException(
            status_code=503, detail="Import preview integrity check failed"
        ) from None


async def _preview(request, principal, raw, **source):
    runtime = _runtime(request)
    try:
        body = await asyncio.to_thread(parse_package, raw, **source)
    except ValueError:
        raise HTTPException(
            status_code=422, detail="Invalid or unsafe read-only Skill package"
        ) from None
    await _check_content(request, principal.tenant_id, body, imported=True)
    import_id = str(uuid4())
    now = time.time()
    record = {
        "schema_version": 1,
        "import_id": import_id,
        "tenant_id": principal.tenant_id,
        "created_by": principal.subject,
        "created_at": now,
        "expires_at": now + 3600,
        "status": "previewed",
        "content": body,
        "content_sha256": snapshot_content_hash(body),
        "executes_code": False,
    }
    key = preview_key(principal.tenant_id, import_id)
    index = f"tenant:{principal.tenant_id}:skills:import-previews"
    audit = f"tenant:{principal.tenant_id}:audit"
    while True:
        async with runtime._client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(key, index, audit)
                if await pipe.type(index) not in {"none", "zset"} or await pipe.type(audit) not in {
                    "none",
                    "stream",
                }:
                    raise HTTPException(status_code=503, detail="Import storage unavailable")
                if await pipe.exists(key):
                    raise HTTPException(status_code=409, detail="Import identity conflict")
                if await pipe.zcount(index, now, "+inf") >= 32:
                    raise HTTPException(status_code=429, detail="Import preview limit exceeded")
                pipe.multi()
                pipe.set(key, json.dumps(record, ensure_ascii=True), ex=3600)
                pipe.zremrangebyscore(index, "-inf", now)
                pipe.zadd(index, {import_id: now + 3600})
                pipe.expire(index, 3600)
                pipe.xadd(
                    audit,
                    {
                        "payload": json.dumps(
                            {
                                "action": "skill.import_previewed",
                                "import_id": import_id,
                                "subject": principal.subject,
                                "source": source.get("source", "upload"),
                                "time": now,
                            }
                        )
                    },
                )
                await pipe.execute()
                return record
            except WatchError:
                continue


@router.post("/upload")
async def preview_upload(payload: UploadImport, request: Request):
    principal = await authorize(request, "admin")
    await _admit(request, principal)
    try:
        raw = base64.b64decode(payload.archive_base64, validate=True)
        subdirectory(payload.subdirectory)
    except (ValueError, binascii.Error):
        raise HTTPException(status_code=422, detail="Invalid Skill package upload") from None
    return await _preview(request, principal, raw, directory=payload.subdirectory)


@router.post("/github")
async def preview_github(payload: GithubImport, request: Request):
    principal = await authorize(request, "admin")
    await _admit(request, principal)
    try:
        subdirectory(payload.subdirectory)
        raw = await fetch_github(payload.repository, payload.commit)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid GitHub import source") from None
    except Exception:
        raise HTTPException(status_code=503, detail="GitHub archive unavailable") from None
    return await _preview(
        request,
        principal,
        raw,
        source="github",
        repository=payload.repository,
        commit=payload.commit,
        directory=payload.subdirectory,
    )


@router.get("/{import_id}")
async def get_preview(request: Request, import_id: str = Path(pattern=r"^[0-9a-f-]{36}$")):
    principal = await authorize(request, "admin")
    return await _load(_runtime(request), principal.tenant_id, import_id)


@router.post("/{import_id}/confirm")
async def confirm_import(
    payload: ConfirmImport, request: Request, import_id: str = Path(pattern=r"^[0-9a-f-]{36}$")
):
    principal = await authorize(request, "admin")
    runtime = _runtime(request)
    await _admit(request, principal)
    candidate = await _load(runtime, principal.tenant_id, import_id)
    if candidate["content_sha256"] != payload.expected_content_sha256:
        raise HTTPException(status_code=409, detail="Import preview version conflict")
    await _check_content(request, principal.tenant_id, candidate["content"], imported=True)
    try:
        return await SkillStore(runtime).create(
            principal.tenant_id,
            candidate["content"],
            principal.subject,
            import_confirmation=(
                preview_key(principal.tenant_id, import_id),
                payload.expected_content_sha256,
            ),
        )
    except SkillNotFound:
        raise HTTPException(status_code=404, detail="Import preview expired") from None
    except SkillConflict:
        raise HTTPException(status_code=409, detail="Skill import conflict") from None
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid Skill import confirmation") from None
