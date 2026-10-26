"""Replica bootstrap lock, bounded setup, cancellation and ownership cleanup."""

import asyncio
import os

import pytest

from app.core.checkpoint_bootstrap import BOOTSTRAP_KEY, open_checkpointer
from app.core.config import Settings
from app.core.redis_runtime import RedisRuntime


@pytest.fixture
async def runtime():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("Set TEST_REDIS_URL to a dedicated Redis test server")
    value = RedisRuntime(url)
    await value.ping()
    try:
        yield value
    finally:
        await value.close()


async def test_parallel_bootstrap_is_serialized(runtime):
    active, peak = 0, 0

    class Saver:
        def __init__(self, **kwargs):
            self.closed = False

        async def __aenter__(self):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.05)
            active -= 1

        async def __aexit__(self, *args):
            self.closed = True

    savers = await asyncio.gather(
        *[open_checkpointer(runtime, Settings(_env_file=None), factory=Saver) for _ in range(4)]
    )
    assert peak == 1 and len(savers) == 4
    assert not await runtime._client.exists(BOOTSTRAP_KEY)
    await asyncio.gather(*(saver.__aexit__(None, None, None) for saver in savers))


@pytest.mark.parametrize("cancel", [False, True])
async def test_timeout_or_cancel_closes_saver_and_releases_lock(runtime, cancel):
    entered, closed = asyncio.Event(), asyncio.Event()

    class Saver:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            entered.set()
            await asyncio.Event().wait()

        async def __aexit__(self, *args):
            closed.set()

    task = asyncio.create_task(
        open_checkpointer(runtime, Settings(_env_file=None), factory=Saver, setup_seconds=0.1)
    )
    await entered.wait()
    if cancel:
        task.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else RuntimeError):
        await task
    assert closed.is_set()
    assert not await runtime._client.exists(BOOTSTRAP_KEY)


async def test_wait_timeout_cannot_release_someone_elses_lock(runtime):
    assert await runtime._client.set(BOOTSTRAP_KEY, "other-test-owner", ex=10, nx=True)
    try:
        with pytest.raises(RuntimeError, match="initialization unavailable"):
            await open_checkpointer(runtime, Settings(_env_file=None), wait_seconds=0.1)
        assert await runtime._client.get(BOOTSTRAP_KEY) == "other-test-owner"
    finally:
        await runtime._client.delete(BOOTSTRAP_KEY)
