#!/usr/bin/env python3
"""Inject an unreachable URL and assert retries plus a non-success fallback."""

import argparse
import asyncio
import json
import os

import httpx


async def run(args: argparse.Namespace) -> int:
    headers = {"x-api-key": args.api_key} if args.api_key else {}
    retry_events = 0
    tool_failures = 0
    completed: dict[str, object] | None = None

    async with httpx.AsyncClient(headers=headers, timeout=args.timeout) as client:
        async with client.stream(
            "POST",
            args.endpoint,
            json={
                "query": "讀取這個網址並摘要；若無法存取，請明確說明失敗",
                "target_url": args.invalid_url,
                "max_retries": args.max_retries,
            },
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.startswith("data: "):
                    continue
                event = json.loads(line.removeprefix("data: "))
                if event.get("type") == "retry":
                    retry_events += 1
                if event.get("type") == "tool_status" and event.get("status") == "failed":
                    tool_failures += 1
                if event.get("type") == "completed":
                    completed = event

    if retry_events != args.max_retries:
        raise AssertionError(f"Expected {args.max_retries} retries, received {retry_events}")
    if tool_failures != args.max_retries + 1:
        raise AssertionError(
            f"Expected {args.max_retries + 1} tool failures, received {tool_failures}"
        )
    if not completed or completed.get("evidence_validated") is not False:
        raise AssertionError(f"Expected an explicit evidence failure fallback: {completed}")
    print(
        json.dumps(
            {
                "status": "passed",
                "tool_failures": tool_failures,
                "retries": retry_events,
                "completed": completed,
            },
            ensure_ascii=False,
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://localhost:8000/v1/agent/stream")
    parser.add_argument("--invalid-url", default="https://does-not-exist.invalid/announcement")
    parser.add_argument("--api-key", default=os.getenv("API_KEY"))
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=180.0)
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
