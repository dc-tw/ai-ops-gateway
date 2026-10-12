#!/usr/bin/env python3
"""Measure SSE time-to-first-token and total response latency."""

import argparse
import asyncio
import json
import os
import statistics
import time
from dataclasses import asdict, dataclass

import httpx


@dataclass(slots=True)
class Sample:
    status_code: int
    ttft_ms: float
    total_ms: float
    token_events: int
    completed: bool


async def measure(client: httpx.AsyncClient, endpoint: str, payload: dict[str, object]) -> Sample:
    started = time.perf_counter()
    first_token_at: float | None = None
    token_events = 0
    completed = False
    current_event = ""

    async with client.stream("POST", endpoint, json=payload) as response:
        response.raise_for_status()
        async for line in response.aiter_lines():
            if line.startswith("event: "):
                current_event = line.removeprefix("event: ")
            elif line.startswith("data: "):
                data = json.loads(line.removeprefix("data: "))
                event_type = str(data.get("type", current_event))
                if event_type in {"draft_token", "token"}:
                    token_events += 1
                    first_token_at = first_token_at or time.perf_counter()
                elif event_type == "completed":
                    completed = True

        finished = time.perf_counter()
        if first_token_at is None:
            raise AssertionError("SSE stream completed without a token event")
        return Sample(
            status_code=response.status_code,
            ttft_ms=(first_token_at - started) * 1_000,
            total_ms=(finished - started) * 1_000,
            token_events=token_events,
            completed=completed,
        )


async def run(args: argparse.Namespace) -> int:
    headers = {}
    api_key = args.api_key or os.getenv("API_KEY")
    if api_key:
        headers["x-api-key"] = api_key
    payload: dict[str, object] = {"query": args.query, "max_retries": args.max_retries}
    if args.target_url:
        payload["target_url"] = args.target_url

    timeout = httpx.Timeout(args.timeout, connect=10.0)
    limits = httpx.Limits(max_connections=max(args.concurrency, args.samples))
    async with httpx.AsyncClient(headers=headers, timeout=timeout, limits=limits) as client:
        semaphore = asyncio.Semaphore(args.concurrency)

        async def bounded_sample() -> Sample:
            async with semaphore:
                return await measure(client, args.endpoint, payload)

        samples = await asyncio.gather(*(bounded_sample() for _ in range(args.samples)))

    ttfts = [sample.ttft_ms for sample in samples]
    totals = [sample.total_ms for sample in samples]
    summary = {
        "samples": [asdict(sample) for sample in samples],
        "ttft_ms": {
            "min": round(min(ttfts), 1),
            "p50": round(statistics.median(ttfts), 1),
            "p95": round(sorted(ttfts)[max(0, int(len(ttfts) * 0.95) - 1)], 1),
            "max": round(max(ttfts), 1),
        },
        "total_ms": {
            "p50": round(statistics.median(totals), 1),
            "max": round(max(totals), 1),
        },
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if not all(sample.completed for sample in samples):
        raise AssertionError("At least one request did not emit a completed event")
    if args.max_ttft_ms is not None and max(ttfts) > args.max_ttft_ms:
        raise AssertionError(f"TTFT SLO exceeded: {max(ttfts):.1f}ms > {args.max_ttft_ms}ms")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://localhost:8000/v1/agent/stream")
    parser.add_argument("--query", default="請用一句話說明這個服務的狀態")
    parser.add_argument("--target-url")
    parser.add_argument("--api-key")
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--max-ttft-ms", type=float)
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
