"""Outbound notification worker entrypoint."""

from __future__ import annotations

import asyncio
import json
import os
import socket

from app.core.config import get_settings
from app.core.outbound_notifications import NotificationDelivery, destinations
from app.core.redis_runtime import RedisRuntime


async def run() -> None:
    settings = get_settings()
    # Fail startup on malformed destinations instead of silently dropping events.
    configured = json.loads(settings.notification_webhooks_json or "{}")
    if not isinstance(configured, dict):
        raise ValueError("Notification webhook configuration must be a tenant map")
    for tenant in configured:
        destinations(settings, tenant)
    runtime = RedisRuntime(
        settings.redis_url,
        settings.redis_stream_maxlen,
        durable_source_id=settings.postgres_source_id if settings.postgres_dsn else None,
    )
    consumer = f"{socket.gethostname()}-{os.getpid()}"
    delivery = NotificationDelivery(runtime, settings)
    try:
        while True:
            processed = await delivery.process_once(consumer)
            if not processed:
                await asyncio.sleep(min(5, settings.notification_retry_seconds))
    finally:
        await runtime.close()


if __name__ == "__main__":
    asyncio.run(run())
