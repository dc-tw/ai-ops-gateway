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
    draft_ttft_ms: float | None
    final_ttft_ms: float | None
    total_ms: float
    draft_token_events: int
    final_token_events: int
    token_events: int
    completed: bool


async def measure(client: httpx.AsyncClient, endpoint: str, payload: dict[str, object]) -> Sample:
    started = time.perf_counter()
    first_token_at: float | None = None
    first_draft_at: float | None = None
    first_final_at: float | None = None
    draft_token_events = 0
    final_token_events = 0
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
                if event_type == "draft_token":
                    draft_token_events += 1
                    first_draft_at = first_draft_at or time.perf_counter()
                    token_events += 1
                    first_token_at = first_token_at or first_draft_at
                elif event_type == "token":
                    final_token_events += 1
                    first_final_at = first_final_at or time.perf_counter()
                    token_events += 1
                    first_token_at = first_token_at or first_final_at
                elif event_type == "completed":
                    completed = True

        finished = time.perf_counter()
        if first_token_at is None:
            raise AssertionError("SSE stream completed without a token event")
        return Sample(
            status_code=response.status_code,
            ttft_ms=(first_token_at - started) * 1_000,
            draft_ttft_ms=(first_draft_at - started) * 1_000 if first_draft_at else None,
            final_ttft_ms=(first_final_at - started) * 1_000 if first_final_at else None,
            total_ms=(finished - started) * 1_000,
            draft_token_events=draft_token_events,
            final_token_events=final_token_events,
            token_events=token_events,
            completed=completed,
        )


def summarize(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    return {
        "min": round(ordered[0], 1),
        "p50": round(statistics.median(ordered), 1),
        "p95": round(ordered[max(0, int(len(ordered) * 0.95) - 1)], 1),
        "max": round(ordered[-1], 1),
    }


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
    draft_ttfts = [sample.draft_ttft_ms for sample in samples if sample.draft_ttft_ms is not None]
    final_ttfts = [sample.final_ttft_ms for sample in samples if sample.final_ttft_ms is not None]
    totals = [sample.total_ms for sample in samples]
    summary = {
        "label": args.label,
        "ttft_ms": summarize(ttfts),
        "draft_ttft_ms": summarize(draft_ttfts) if draft_ttfts else None,
        "final_ttft_ms": summarize(final_ttfts) if final_ttfts else None,
        "total_ms": summarize(totals),
    }
    if not args.summary_only:
        summary["samples"] = [asdict(sample) for sample in samples]
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if not all(sample.completed for sample in samples):
        raise AssertionError("At least one request did not emit a completed event")
    if args.max_ttft_ms is not None and max(ttfts) > args.max_ttft_ms:
        raise AssertionError(f"TTFT SLO exceeded: {max(ttfts):.1f}ms > {args.max_ttft_ms}ms")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://localhost:8000/v1/agent/stream")
    parser.add_argument(
        "--label",
        default="unspecified",
        help="寫入報告的模式標籤，例如 atr-off、detection-only 或 blocking",
    )
    parser.add_argument("--query", default="請用一句話說明這個服務的狀態")
    parser.add_argument("--target-url")
    parser.add_argument("--api-key")
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--max-ttft-ms", type=float)
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="只輸出彙總統計，不輸出每筆 sample",
    )
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
