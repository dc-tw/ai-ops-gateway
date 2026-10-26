"""Tenant planning snapshots and mode transitions; never execute or enqueue work."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from redis.exceptions import WatchError

SCHEMA_VERSION = 1
MAX_PLAN_BYTES = 64 * 1_024
MAX_SNAPSHOT_BYTES = 512 * 1_024
MAX_REVISIONS = 1_000
MAX_SESSIONS_PER_TENANT = 1_000
TERMINAL_GOAL_STATUSES = {"completed", "failed", "cancelled"}
SnapshotBuilder = Callable[[Any], Awaitable[dict[str, Any]]]
PlanValidator = Callable[[dict[str, Any], dict[str, Any]], bool]


class PlanningConflict(ValueError):
    pass


class PlanningNotFound(ValueError):
    pass


class PlanningIntegrityError(ValueError):
    pass


def _json(value: Any) -> str:
    try:
        encoded = json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )
        encoded.encode("utf-8")
        return encoded
    except (TypeError, ValueError, RecursionError, UnicodeError):
        raise PlanningIntegrityError("Planning data is not valid JSON") from None


def canonical_hash(value: Any) -> str:
    """Hash canonical JSON without changing any existing Skill hash contract."""
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _digest(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _positive(value: Any, *, zero: bool = False) -> bool:
    return type(value) is int and value >= (0 if zero else 1)


def _identifier(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9-]{1,128}", value) is not None


def _object(value: Any, limit: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PlanningIntegrityError("Planning data must be an object")
    encoded = _json(value)
    if len(encoded.encode("utf-8")) > limit:
        raise ValueError("Planning data exceeds its byte limit")
    return json.loads(encoded)


def _decode(raw: Any) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except (TypeError, ValueError, RecursionError):
        raise PlanningIntegrityError("Stored planning data is invalid") from None
    if not isinstance(value, dict):
        raise PlanningIntegrityError("Stored planning data is invalid")
    return value


def _plan_hash(plan: dict[str, Any], complete: bool, snapshot: dict[str, Any]) -> str:
    return canonical_hash(
        {"schema_version": SCHEMA_VERSION, "plan": plan, "complete": complete, "snapshot": snapshot}
    )


class PlanningStore:
    def __init__(self, runtime: Any, *, max_watch_retries: int = 8) -> None:
        if type(max_watch_retries) is not int or not 1 <= max_watch_retries <= 50:
            raise ValueError("Planning retries must be between one and fifty")
        self.runtime = runtime
        self.client = runtime._client
        self.max_watch_retries = max_watch_retries

    def session_key(self, tenant: str, session_id: str) -> str:
        return self.runtime._tenant_key(tenant, f"planning:session:{session_id}")

    def revision_key(self, tenant: str, session_id: str, revision: int) -> str:
        return self.runtime._tenant_key(tenant, f"planning:revision:{session_id}:{revision}")

    def events_key(self, tenant: str, session_id: str) -> str:
        return self.runtime._tenant_key(tenant, f"planning:events:{session_id}")

    def _index_key(self, tenant: str) -> str:
        return self.runtime._tenant_key(tenant, "planning:sessions")

    @staticmethod
    def _snapshot(value: Any, tenant: str, agent_id: str) -> dict[str, Any]:
        snapshot = _object(value, MAX_SNAPSHOT_BYTES)
        if (
            type(snapshot.get("schema_version")) is not int
            or snapshot["schema_version"] != SCHEMA_VERSION
            or snapshot.get("tenant_id") != tenant
            or snapshot.get("agent_id") != agent_id
            or not _digest(snapshot.get("snapshot_fingerprint"))
        ):
            raise PlanningIntegrityError("Planning snapshot identity or schema is invalid")
        return snapshot

    def _session(self, raw: Any, tenant: str, session_id: str) -> dict[str, Any]:
        if raw is None:
            raise PlanningNotFound("Planning session not found")
        value = _decode(raw)
        if value.get("tenant_id") != tenant:
            raise PlanningNotFound("Planning session not found")
        if (
            type(value.get("schema_version")) is not int
            or value["schema_version"] != SCHEMA_VERSION
            or value.get("session_id") != session_id
            or not _identifier(value.get("agent_id"))
            or value.get("mode") not in {"plan", "execute"}
            or not _positive(value.get("version"))
            or not _positive(value.get("plan_revision"), zero=True)
            or value["plan_revision"] > MAX_REVISIONS
            or value.get("active_goal_id") is not None
            and not _identifier(value["active_goal_id"])
        ):
            raise PlanningIntegrityError("Planning session identity or schema is invalid")
        snapshot = self._snapshot(value.get("snapshot"), tenant, value["agent_id"])
        if value.get("snapshot_sha256") != canonical_hash(snapshot):
            raise PlanningIntegrityError("Planning snapshot hash does not match")
        if (value["plan_revision"] == 0 and value.get("plan_sha256") is not None) or (
            value["plan_revision"] > 0 and not _digest(value.get("plan_sha256"))
        ):
            raise PlanningIntegrityError("Planning revision reference is invalid")
        if value["mode"] == "plan":
            accepted_valid = (
                value.get("accepted_revision") is None and value.get("accepted_hash") is None
            )
        else:
            accepted_valid = (
                value["plan_revision"] > 0
                and _positive(value.get("accepted_revision"))
                and value.get("accepted_revision") == value["plan_revision"]
                and value.get("accepted_hash") == value["plan_sha256"]
            )
        if not accepted_valid:
            raise PlanningIntegrityError("Planning acceptance reference is invalid")
        return value

    def _revision(
        self, raw: Any, tenant: str, session_id: str, revision: int, agent_id: str
    ) -> dict[str, Any]:
        if raw is None:
            raise PlanningIntegrityError("Stored plan revision is missing")
        value = _decode(raw)
        if (
            type(value.get("schema_version")) is not int
            or value["schema_version"] != SCHEMA_VERSION
            or value.get("tenant_id") != tenant
            or value.get("session_id") != session_id
            or value.get("agent_id") != agent_id
            or type(value.get("revision")) is not int
            or value["revision"] != revision
            or type(value.get("complete")) is not bool
        ):
            raise PlanningIntegrityError("Plan revision identity or schema is invalid")
        plan = _object(value.get("plan"), MAX_PLAN_BYTES)
        snapshot = self._snapshot(value.get("snapshot"), tenant, agent_id)
        if value.get("snapshot_sha256") != canonical_hash(snapshot):
            raise PlanningIntegrityError("Plan snapshot hash does not match")
        if value.get("content_sha256") != _plan_hash(plan, value["complete"], snapshot):
            raise PlanningIntegrityError("Plan content hash does not match")
        return value

    async def _current_revision(
        self, reader: Any, session: dict[str, Any], *, watch: bool = False
    ) -> dict[str, Any] | None:
        revision = session["plan_revision"]
        if revision == 0:
            return None
        key = self.revision_key(session["tenant_id"], session["session_id"], revision)
        if watch:
            await reader.watch(key)
        value = self._revision(
            await reader.get(key),
            session["tenant_id"],
            session["session_id"],
            revision,
            session["agent_id"],
        )
        if (
            value["content_sha256"] != session["plan_sha256"]
            or value["snapshot_sha256"] != session["snapshot_sha256"]
            or session["mode"] == "execute"
            and not value["complete"]
        ):
            raise PlanningIntegrityError("Current plan does not match its session")
        return value

    async def get(self, tenant: str, session_id: str) -> dict[str, Any] | None:
        try:
            session = self._session(
                await self.client.get(self.session_key(tenant, session_id)), tenant, session_id
            )
        except PlanningNotFound:
            return None
        await self._current_revision(self.client, session)
        return session

    async def get_revision(
        self, tenant: str, session_id: str, revision: int
    ) -> dict[str, Any] | None:
        if not _positive(revision):
            raise ValueError("Plan revision must be a positive integer")
        session = await self.get(tenant, session_id)
        if session is None or revision > session["plan_revision"]:
            return None
        return self._revision(
            await self.client.get(self.revision_key(tenant, session_id, revision)),
            tenant,
            session_id,
            revision,
            session["agent_id"],
        )

    async def revisions(
        self, tenant: str, session_id: str, *, after: int = 0, limit: int = 50
    ) -> tuple[list[dict[str, Any]], int | None]:
        if not _positive(after, zero=True) or not _positive(limit) or limit > 100:
            raise ValueError("Invalid plan revision pagination")
        session = await self.get(tenant, session_id)
        if session is None:
            raise PlanningNotFound("Planning session not found")
        end = min(session["plan_revision"], after + limit)
        numbers = range(after + 1, end + 1)
        keys = [self.revision_key(tenant, session_id, number) for number in numbers]
        rows = await self.client.mget(keys) if keys else []
        values = [
            self._revision(raw, tenant, session_id, number, session["agent_id"])
            for number, raw in zip(numbers, rows, strict=True)
        ]
        return values, end if end < session["plan_revision"] else None

    @staticmethod
    def _expected(session: dict[str, Any], expected_version: int) -> None:
        if not _positive(expected_version):
            raise ValueError("Expected version must be a positive integer")
        if session["version"] != expected_version:
            raise PlanningConflict("Planning session version conflict")

    def _record_event(self, pipe: Any, session: dict[str, Any], action: str, subject: str) -> None:
        event = {
            "type": action,
            "session_id": session["session_id"],
            "agent_id": session["agent_id"],
            "version": session["version"],
            "mode": session["mode"],
            "plan_revision": session["plan_revision"],
            "plan_sha256": session["plan_sha256"],
            "snapshot_sha256": session["snapshot_sha256"],
            "accepted_revision": session["accepted_revision"],
            "accepted_hash": session["accepted_hash"],
            "subject": subject,
            "time": session["updated_at"],
        }
        tenant = session["tenant_id"]
        pipe.xadd(self.events_key(tenant, session["session_id"]), {"payload": _json(event)})
        pipe.xadd(
            self.runtime._tenant_key(tenant, "audit"),
            {"payload": _json({"action": action} | event)},
        )

    async def create(
        self, tenant: str, agent_id: str, subject: str, snapshot_builder: SnapshotBuilder
    ) -> dict[str, Any]:
        if not _identifier(agent_id):
            raise ValueError("Invalid Agent identity")
        session_id = str(uuid4())
        key, index = self.session_key(tenant, session_id), self._index_key(tenant)
        for _ in range(self.max_watch_retries):
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key, index)
                    if await pipe.exists(key):
                        raise PlanningConflict("Planning session already exists")
                    if await pipe.scard(index) >= MAX_SESSIONS_PER_TENANT:
                        raise PlanningConflict("Planning session limit reached")
                    snapshot = self._snapshot(await snapshot_builder(pipe), tenant, agent_id)
                    now = datetime.now(UTC).isoformat()
                    session = {
                        "schema_version": SCHEMA_VERSION,
                        "tenant_id": tenant,
                        "session_id": session_id,
                        "agent_id": agent_id,
                        "mode": "plan",
                        "version": 1,
                        "plan_revision": 0,
                        "plan_sha256": None,
                        "snapshot": snapshot,
                        "snapshot_sha256": canonical_hash(snapshot),
                        "accepted_revision": None,
                        "accepted_hash": None,
                        "active_goal_id": None,
                        "created_at": now,
                        "updated_at": now,
                        "created_by": subject,
                        "updated_by": subject,
                    }
                    pipe.multi()
                    pipe.set(key, _json(session))
                    pipe.sadd(index, session_id)
                    self._record_event(pipe, session, "planning.session_created", subject)
                    await pipe.execute()
                    return session
                except WatchError:
                    await asyncio.sleep(0)
        raise PlanningConflict("Planning configuration changed too often; retry the request")

    async def _write_plan(
        self,
        tenant: str,
        session_id: str,
        expected_version: int,
        subject: str,
        snapshot_builder: SnapshotBuilder,
        *,
        plan: dict[str, Any] | None = None,
        complete: bool | None = None,
        plan_validator: PlanValidator | None = None,
        refresh: bool = False,
    ) -> dict[str, Any]:
        if not refresh:
            plan = _object(plan, MAX_PLAN_BYTES)
            if type(complete) is not bool:
                raise ValueError("Plan completeness must be a boolean")
        key = self.session_key(tenant, session_id)
        for _ in range(self.max_watch_retries):
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    session = self._session(await pipe.get(key), tenant, session_id)
                    self._expected(session, expected_version)
                    if session["mode"] != "plan":
                        raise PlanningConflict("Planning session is not in plan mode")
                    current = await self._current_revision(pipe, session, watch=True)
                    snapshot = self._snapshot(
                        await snapshot_builder(pipe), tenant, session["agent_id"]
                    )
                    snapshot_hash = canonical_hash(snapshot)
                    if not refresh and snapshot_hash != session["snapshot_sha256"]:
                        raise PlanningConflict(
                            "Planning configuration is stale; refresh the session"
                        )
                    if refresh:
                        plan = current["plan"] if current else None
                        if current:
                            if plan_validator is None:
                                raise ValueError("Refresh requires a trusted plan validator")
                            try:
                                complete = plan_validator(snapshot, plan)
                            except ValueError:
                                raise PlanningConflict(
                                    "Saved plan is incompatible; "
                                    "replace the plan or create a session"
                                ) from None
                            if type(complete) is not bool:
                                raise ValueError("Plan validator must return a boolean")
                    revision = None
                    if plan is not None:
                        number = session["plan_revision"] + 1
                        if number > MAX_REVISIONS:
                            raise PlanningConflict("Planning revision limit reached")
                        revision_key = self.revision_key(tenant, session_id, number)
                        await pipe.watch(revision_key)
                        if await pipe.exists(revision_key):
                            # A competing CAS may have committed between reading
                            # the session and adding the new revision to WATCH.
                            self._expected(
                                self._session(await pipe.get(key), tenant, session_id),
                                expected_version,
                            )
                            raise PlanningIntegrityError("Immutable plan revision already exists")
                        revision = {
                            "schema_version": SCHEMA_VERSION,
                            "tenant_id": tenant,
                            "session_id": session_id,
                            "agent_id": session["agent_id"],
                            "revision": number,
                            "plan": plan,
                            "complete": complete,
                            "snapshot": snapshot,
                            "snapshot_sha256": snapshot_hash,
                            "content_sha256": _plan_hash(plan, complete, snapshot),
                            "created_at": datetime.now(UTC).isoformat(),
                            "created_by": subject,
                        }
                        session.update(plan_revision=number, plan_sha256=revision["content_sha256"])
                    session.update(
                        snapshot=snapshot,
                        snapshot_sha256=snapshot_hash,
                        version=session["version"] + 1,
                        updated_at=datetime.now(UTC).isoformat(),
                        updated_by=subject,
                    )
                    pipe.multi()
                    pipe.set(key, _json(session))
                    if revision is not None:
                        pipe.set(revision_key, _json(revision))
                    self._record_event(
                        pipe, session, "planning.refreshed" if refresh else "plan.revised", subject
                    )
                    await pipe.execute()
                    return session
                except WatchError:
                    await asyncio.sleep(0)
        raise PlanningConflict("Planning configuration changed too often; retry the request")

    async def save_plan(
        self,
        tenant: str,
        session_id: str,
        plan: dict[str, Any],
        expected_version: int,
        subject: str,
        snapshot_builder: SnapshotBuilder,
        *,
        complete: bool,
    ) -> dict[str, Any]:
        return await self._write_plan(
            tenant,
            session_id,
            expected_version,
            subject,
            snapshot_builder,
            plan=plan,
            complete=complete,
        )

    async def refresh(
        self,
        tenant: str,
        session_id: str,
        expected_version: int,
        subject: str,
        snapshot_builder: SnapshotBuilder,
        *,
        plan_validator: PlanValidator,
    ) -> dict[str, Any]:
        return await self._write_plan(
            tenant,
            session_id,
            expected_version,
            subject,
            snapshot_builder,
            refresh=True,
            plan_validator=plan_validator,
        )

    async def set_mode(
        self,
        tenant: str,
        session_id: str,
        mode: str,
        expected_version: int,
        subject: str,
        snapshot_builder: SnapshotBuilder,
        *,
        plan_revision: int | None = None,
        plan_sha256: str | None = None,
    ) -> dict[str, Any]:
        if mode not in {"plan", "execute"}:
            raise ValueError("Unknown planning mode")
        key = self.session_key(tenant, session_id)
        for _ in range(self.max_watch_retries):
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    session = self._session(await pipe.get(key), tenant, session_id)
                    self._expected(session, expected_version)
                    current = await self._current_revision(pipe, session, watch=True)
                    if mode == "execute":
                        if (
                            not _positive(plan_revision)
                            or current is None
                            or plan_revision != current["revision"]
                            or not current["complete"]
                        ):
                            raise PlanningConflict("Accept the current complete plan revision")
                        if plan_sha256 is not None and plan_sha256 != current["content_sha256"]:
                            raise PlanningConflict("Accepted plan hash does not match")
                        snapshot = self._snapshot(
                            await snapshot_builder(pipe), tenant, session["agent_id"]
                        )
                        if canonical_hash(snapshot) != session["snapshot_sha256"]:
                            raise PlanningConflict(
                                "Planning configuration is stale; refresh the session"
                            )
                    elif session.get("active_goal_id"):
                        goal_id = session["active_goal_id"]
                        goal_key = self.runtime._tenant_key(tenant, f"agent:goal:{goal_id}")
                        await pipe.watch(goal_key)
                        raw = await pipe.get(goal_key)
                        goal = _decode(raw) if raw else None
                        if (
                            not goal
                            or goal.get("tenant_id") != tenant
                            or goal.get("goal_id") != goal_id
                            or goal.get("status") not in TERMINAL_GOAL_STATUSES
                        ):
                            raise PlanningConflict(
                                "End the active Goal before returning to plan mode"
                            )
                    if session["mode"] == mode:
                        # Even a no-op must validate its watched snapshot before
                        # returning it as the current accepted session state.
                        pipe.multi()
                        await pipe.execute()
                        return session
                    session.update(
                        mode=mode,
                        version=session["version"] + 1,
                        accepted_revision=current["revision"] if mode == "execute" else None,
                        accepted_hash=current["content_sha256"] if mode == "execute" else None,
                        updated_at=datetime.now(UTC).isoformat(),
                        updated_by=subject,
                    )
                    pipe.multi()
                    pipe.set(key, _json(session))
                    self._record_event(pipe, session, "planning.mode_changed", subject)
                    await pipe.execute()
                    return session
                except WatchError:
                    await asyncio.sleep(0)
        raise PlanningConflict("Planning configuration changed too often; retry the request")
