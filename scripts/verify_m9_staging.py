#!/usr/bin/env python3
"""M9 private-cluster load and recovery verifier using the synthetic provider."""

from __future__ import annotations

import asyncio
import json
import math
import os
import socket
import statistics
import time
from collections.abc import Awaitable, Callable
from uuid import uuid4

import httpx
from redis.asyncio import Redis

OUTBOX = "gateway:durable:postgres-e2e:outbox"
GROUP = "postgres-history-v1"


def percentile(values: list[float], ratio: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * ratio) - 1))
    return round(ordered[index], 2)


def gateway_addresses() -> list[str]:
    rows = socket.getaddrinfo("gateway", 8000, type=socket.SOCK_STREAM)
    return sorted({row[4][0] for row in rows})


async def stream_once(client: httpx.AsyncClient, address: str, marker: str) -> float:
    started = time.perf_counter()
    completed = False
    async with client.stream(
        "POST",
        f"http://{address}:8000/v1/agent/stream",
        headers={"x-api-key": "m9-admin"},
        json={
            "conversation_id": f"m9-{marker}-{uuid4().hex}",
            "query": "M9 synthetic staging load verification",
            "max_retries": 0,
        },
    ) as response:
        if not response.is_success:
            detail = (await response.aread()).decode("utf-8", errors="replace")[:500]
            raise AssertionError(
                {"status": response.status_code, "address": address, "detail": detail}
            )
        async for line in response.aiter_lines():
            if line.startswith("data: "):
                event = json.loads(line[6:])
                completed = completed or event.get("type") == "completed"
    if not completed:
        raise AssertionError("SSE request ended without completed event")
    return (time.perf_counter() - started) * 1_000


async def wait_until(
    check: Callable[[], Awaitable[bool]], max_wait_seconds: float = 30.0
) -> float:
    started = time.perf_counter()
    while time.perf_counter() - started < max_wait_seconds:
        if await check():
            return (time.perf_counter() - started) * 1_000
        await asyncio.sleep(0.1)
    raise AssertionError("recovery condition did not converge")


async def projector_state(redis: Redis) -> dict[str, int]:
    groups = await redis.xinfo_groups(OUTBOX)
    group = next((row for row in groups if row.get("name") == GROUP), None)
    return {
        "lag": int(group.get("lag") or 0) if group else int(await redis.xlen(OUTBOX)),
        "pending": int(group.get("pending") or 0) if group else 0,
        "outbox_length": int(await redis.xlen(OUTBOX)),
    }


async def ready(address: str, expected: int) -> bool:
    try:
        async with httpx.AsyncClient(timeout=2) as client:
            response = await client.get(f"http://{address}:8000/readyz")
        return response.status_code == expected
    except httpx.HTTPError:
        return False


async def main() -> None:
    phase = os.environ.get("M9_PHASE", "load")
    addresses = gateway_addresses()
    minimum = int(os.environ.get("M9_MIN_REPLICAS", "2"))
    if len(addresses) < minimum:
        raise AssertionError(("gateway replicas", addresses, minimum))

    if phase in {"redis-down", "redis-recovered"}:
        expected = 503 if phase == "redis-down" else 200

        async def all_ready() -> bool:
            return all([await ready(address, expected) for address in addresses])

        elapsed = await wait_until(all_ready)
        print(json.dumps({"phase": phase, "replicas": len(addresses), "recovery_ms": elapsed}))
        return

    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    try:
        if phase == "projector-recovered":

            async def caught_up() -> bool:
                state = await projector_state(redis)
                return state["lag"] == 0 and state["pending"] == 0

            elapsed = await wait_until(caught_up, 60)
            print(
                json.dumps(
                    {
                        "phase": phase,
                        "projector_recovery_ms": round(elapsed, 2),
                        "projector": await projector_state(redis),
                    }
                )
            )
            return

        concurrencies = (
            [int(value) for value in os.environ.get("M9_CONCURRENCIES", "1,4,16").split(",")]
            if phase == "load"
            else [4]
        )
        results = []
        limits = httpx.Limits(max_connections=32, max_keepalive_connections=0)
        async with httpx.AsyncClient(timeout=30, limits=limits) as client:
            for concurrency in concurrencies:
                samples = max(8, concurrency * 2)
                semaphore = asyncio.Semaphore(concurrency)

                async def one(
                    index: int,
                    gate: asyncio.Semaphore = semaphore,
                    level: int = concurrency,
                ) -> float:
                    async with gate:
                        return await stream_once(
                            client, addresses[index % len(addresses)], f"{phase}-{level}"
                        )

                started = time.perf_counter()
                rows = await asyncio.gather(*(one(index) for index in range(samples)))
                results.append(
                    {
                        "concurrency": concurrency,
                        "samples": samples,
                        "errors": 0,
                        "p50_ms": round(statistics.median(rows), 2),
                        "p95_ms": percentile(rows, 0.95),
                        "max_ms": round(max(rows), 2),
                        "wall_ms": round((time.perf_counter() - started) * 1_000, 2),
                    }
                )
        state = await projector_state(redis)
        print(
            json.dumps(
                {
                    "phase": phase,
                    "provider": "synthetic",
                    "replica_addresses": addresses,
                    "loads": results,
                    "projector": state,
                },
                ensure_ascii=False,
            )
        )
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
