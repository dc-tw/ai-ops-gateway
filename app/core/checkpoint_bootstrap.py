"""Serialize checkpointer index creation across Gateway and worker replicas."""

import asyncio
from contextlib import suppress
from uuid import uuid4

BOOTSTRAP_KEY = "gateway:checkpoint:bootstrap"


async def open_checkpointer(runtime, settings, *, factory=None, setup_seconds=30, wait_seconds=65):
    if factory is None:
        from langgraph.checkpoint.redis.aio import AsyncRedisSaver

        factory = AsyncRedisSaver
    client, token = runtime._client, uuid4().hex
    saver = None
    acquired = False
    try:
        # Setup's enforced deadline is shorter than the lease. A dead owner
        # leaves only an expiring bootstrap lock, not an execution reservation.
        async with asyncio.timeout(wait_seconds):
            for _ in range(max(1, int(wait_seconds / 0.05) + 1)):
                acquired = bool(await client.set(BOOTSTRAP_KEY, token, nx=True, ex=60))
                if acquired:
                    break
                await asyncio.sleep(0.05)
            else:
                raise TimeoutError("Checkpoint bootstrap contention")
        try:
            async with asyncio.timeout(min(setup_seconds, 30)):
                saver = factory(
                    redis_url=settings.redis_url,
                    ttl={"default_ttl": settings.checkpoint_ttl_minutes, "refresh_on_read": True},
                )
                await saver.__aenter__()
                if await client.get(BOOTSTRAP_KEY) != token:
                    raise RuntimeError("Checkpoint bootstrap ownership lost")
        finally:
            if acquired:
                released = await client.eval(
                    "if redis.call('GET',KEYS[1]) == ARGV[1] then "
                    "return redis.call('DEL',KEYS[1]) end return 0",
                    1,
                    BOOTSTRAP_KEY,
                    token,
                )
                if released != 1:
                    raise RuntimeError("Checkpoint bootstrap ownership lost")
        return saver
    except BaseException as exc:
        if saver is not None:
            with suppress(Exception):
                async with asyncio.timeout(5):
                    await saver.__aexit__(None, None, None)
        if not isinstance(exc, Exception):
            raise
        raise RuntimeError("Checkpoint initialization unavailable") from None
