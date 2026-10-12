#!/usr/bin/env python3
"""Assert the observable LangGraph phase sequence through the public SSE API."""

import argparse
import asyncio
import json
import os

import httpx

EXPECTED_PHASES = ["Planning", "Tool_Call", "Parsing", "Evidence Validation", "Final_Answer"]


async def run(args: argparse.Namespace) -> int:
    headers = {"x-api-key": args.api_key} if args.api_key else {}
    phases: list[str] = []
    evidence_validations: list[str] = []
    async with httpx.AsyncClient(headers=headers, timeout=args.timeout) as client:
        async with client.stream(
            "POST",
            args.endpoint,
            json={
                "query": args.query,
                "target_url": args.target_url,
                "max_retries": args.max_retries,
            },
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.startswith("data: "):
                    continue
                event = json.loads(line.removeprefix("data: "))
                if event.get("type") == "node_status" and event.get("status") == "started":
                    phase = event.get("phase")
                    if phase and (not phases or phases[-1] != phase):
                        phases.append(str(phase))
                if event.get("type") == "evidence_validation":
                    evidence_validations.append(str(event.get("status")))

    cursor = 0
    for phase in phases:
        if cursor < len(EXPECTED_PHASES) and phase == EXPECTED_PHASES[cursor]:
            cursor += 1
    if cursor != len(EXPECTED_PHASES):
        raise AssertionError(f"Unexpected phase sequence: {phases}")
    if "passed" not in evidence_validations:
        raise AssertionError(
            f"No successful evidence validation event: {evidence_validations}"
        )
    print(
        json.dumps(
            {
                "status": "passed",
                "phases": phases,
                "evidence_validation": evidence_validations,
            }
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://localhost:8000/v1/agent/stream")
    parser.add_argument("--query", default="請讀取網頁並摘要最新公告")
    parser.add_argument("--target-url", default="https://example.com")
    parser.add_argument("--api-key", default=os.getenv("API_KEY"))
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=180.0)
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
