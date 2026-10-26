"""Timezone-aware schedules and exact missed-occurrence counts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from croniter import croniter


def _next_cron(iterator: croniter, expression: str, after: datetime) -> datetime:
    while True:
        candidate = iterator.get_next(datetime)
        instant = candidate.astimezone(UTC)
        # croniter may shift a nonexistent spring-forward wall time to the gap
        # boundary. Skip that synthetic time. Repeated fall-back wall times each
        # represent a separate valid UTC occurrence and are both retained.
        if instant > after and croniter.match(expression, candidate.replace(tzinfo=None)):
            return instant


def utc_datetime(value: str | float | datetime) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, (float, int)):
        result = datetime.fromtimestamp(value, UTC)
    else:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("An explicit timezone is required")
    return result.astimezone(UTC)


def next_run(trigger: dict[str, Any], after: datetime) -> datetime | None:
    if trigger["kind"] != "schedule":
        return None
    after = utc_datetime(after)
    if trigger.get("interval_seconds"):
        return after + timedelta(seconds=int(trigger["interval_seconds"]))
    local = after.astimezone(ZoneInfo(trigger.get("timezone", "Asia/Taipei")))
    return _next_cron(
        croniter(trigger["cron"], local, max_years_between_matches=8), trigger["cron"], after
    )


def coalesce_due(trigger: dict[str, Any], due: datetime, now: datetime) -> tuple[int, datetime]:
    """Return occurrence count and future due time, preserving interval phase.

    Cron recovery counts calendar matches (at most one per minute); the scheduler
    offloads that enumeration from its event loop. Eight years is the cron search
    horizon between matches, not a limit on outage duration. Recovery creates one
    run containing the count instead of issuing one run for each missed tick.
    """
    due, now = utc_datetime(due), utc_datetime(now)
    if due > now:
        return 0, due
    interval = trigger.get("interval_seconds")
    if interval:
        count = int((now - due).total_seconds() // int(interval)) + 1
        return count, due + timedelta(seconds=count * int(interval))
    zone = ZoneInfo(trigger.get("timezone", "Asia/Taipei"))
    iterator = croniter(trigger["cron"], due.astimezone(zone), max_years_between_matches=8)
    count = 1
    following = _next_cron(iterator, trigger["cron"], due)
    while following <= now:
        count += 1
        following = _next_cron(iterator, trigger["cron"], following)
    return count, following
