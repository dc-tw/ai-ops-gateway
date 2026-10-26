"""Durable schedule and internal-event delivery into the existing Goal worker."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi import HTTPException
from redis.exceptions import ResponseError

from app.automations import INTERNAL_STREAM, dispatch, event_matches
from app.core.automation_scheduling import coalesce_due
from app.core.automation_store import AutomationStore
from app.core.config import get_settings
from app.core.redis_runtime import RedisRuntime
from app.core.threat_detection import ThreatDetector
from app.llm.client import ModelRouter

logger = logging.getLogger(__name__)
INTERNAL_GROUP = "automation-schedulers"


async def _rejected(store: Any, job: dict[str, Any], exc: HTTPException) -> None:
    await store.append_event(
        job["tenant_id"],
        job["automation_id"],
        {
            "type": "automation_rejected",
            "status": "rejected",
            "http_status": exc.status_code,
        },
    )
    await store.runtime.append_audit(
        job["tenant_id"],
        {
            "action": "automation.rejected",
            "automation_id": job["automation_id"],
            "agent_id": job["agent_id"],
            "status": "rejected",
            "http_status": exc.status_code,
        },
    )


async def _internal_events(
    runtime: Any,
    settings: Any,
    model_router: Any,
    detector: Any,
    store: AutomationStore,
    now: datetime,
    consumer: str,
) -> int:
    client = runtime._client
    try:
        await client.xgroup_create(INTERNAL_STREAM, INTERNAL_GROUP, id="0-0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
    claimed = await client.xautoclaim(
        INTERNAL_STREAM,
        INTERNAL_GROUP,
        consumer,
        min_idle_time=30_000,
        start_id="0-0",
        count=100,
    )
    entries = list(claimed[1])
    if not entries:
        batches = await client.xreadgroup(
            INTERNAL_GROUP, consumer, {INTERNAL_STREAM: ">"}, count=100, block=1
        )
        entries = [entry for _, batch in batches for entry in batch]
    processed = 0
    for stream_id, fields in entries:
        tenant_id = fields["tenant_id"]
        event = json.loads(fields["payload"])
        # Only the server emits on this stream. External webhook fields never
        # influence tenant, service identity, or automation provenance.
        for job in await store.list(tenant_id):
            if job["trigger"]["kind"] != "internal" or not job["enabled"]:
                continue
            if not event_matches(job, event):
                continue
            try:
                await dispatch(
                    runtime,
                    settings,
                    model_router,
                    detector,
                    store,
                    job,
                    f"internal:{event['event_id']}",
                    event,
                    now=now,
                    metadata={
                        "trigger_type": "internal",
                        "event_id": event["event_id"],
                        "event_type": event["type"],
                        "causation_id": stream_id,
                    },
                )
            except HTTPException as exc:
                if exc.status_code == 409:
                    # Leave this delivery pending so a configuration change
                    # cannot silently consume an otherwise matching event.
                    raise
                await _rejected(store, job, exc)
        await client.xack(INTERNAL_STREAM, INTERNAL_GROUP, stream_id)
        processed += 1
    return processed


async def tick(
    runtime: Any,
    settings: Any,
    model_router: Any,
    detector: Any,
    *,
    store: AutomationStore | None = None,
    now: datetime | None = None,
    consumer: str = "scheduler-test",
) -> dict[str, int]:
    """One idempotent tick; safe to invoke simultaneously from multiple replicas."""
    now, store = now or datetime.now(UTC), store or AutomationStore(runtime)
    scheduled = 0
    for tenant_id, automation_id in await store.due(now.timestamp()):
        job = await store.get(tenant_id, automation_id)
        if not job or not job["enabled"] or job["trigger"]["kind"] != "schedule":
            continue
        expected = job.get("next_run_at")
        if expected is None or expected > now.timestamp():
            continue
        scheduled_at = datetime.fromtimestamp(expected, UTC)
        count, next_at = await asyncio.to_thread(coalesce_due, job["trigger"], scheduled_at, now)
        try:
            await dispatch(
                runtime,
                settings,
                model_router,
                detector,
                store,
                job,
                f"schedule:{expected}",
                now=now,
                coalesced_count=count,
                metadata={
                    "trigger_type": "schedule",
                    "scheduled_at": scheduled_at.isoformat(),
                    "schedule_due_at": expected,
                    "schedule_next_at": next_at.timestamp(),
                },
            )
        except HTTPException as exc:
            if exc.status_code == 409:
                continue
            await _rejected(store, job, exc)
        # Accepted schedule submissions already move the cursor atomically with
        # the durable Goal/pending slot. This CAS only advances rejected input;
        # for accepted work it is a no-op, including concurrent replica retries.
        await store.advance_schedule(
            tenant_id,
            automation_id,
            expected,
            next_at.timestamp(),
            expected_version=job["version"],
        )
        scheduled += 1
    flushed = 0
    for tenant_id in await store.tenants():
        for job in await store.list(tenant_id):
            result = await store.flush_pending(tenant_id, job["automation_id"], now.timestamp())
            flushed += int(result.get("status") == "queued")
    internal = await _internal_events(
        runtime, settings, model_router, detector, store, now, consumer
    )
    from app.core.goal_execution import GoalExecutionStore

    execution = GoalExecutionStore(runtime)
    await execution.expire_approvals(now.timestamp())
    await execution.recover_expired(now.timestamp())
    from app.core.delegation import DelegationStore

    await DelegationStore(runtime).reconcile_parents()
    return {"scheduled": scheduled, "flushed": flushed, "internal_events": internal}


async def run_scheduler() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    runtime = RedisRuntime(settings.redis_url, settings.redis_stream_maxlen)
    model_router, detector = ModelRouter(settings), ThreatDetector.from_settings(settings)
    consumer = f"scheduler-{uuid4()}"
    try:
        await runtime.ping()
        while True:
            try:
                await tick(runtime, settings, model_router, detector, consumer=consumer)
            except Exception:
                logger.exception("Automation scheduler tick failed")
            await asyncio.sleep(getattr(settings, "automation_poll_seconds", 2.0))
    finally:
        await runtime.close()


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(run_scheduler())
