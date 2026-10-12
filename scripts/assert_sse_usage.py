"""Validate provider-reported usage in an SSE capture without float string coupling."""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path


def main(path: str) -> None:
    completed: dict[str, object] | None = None
    event_name: str | None = None
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.startswith("event: "):
            event_name = line.removeprefix("event: ")
            continue
        if not line.startswith("data: "):
            continue
        payload = json.loads(line.removeprefix("data: "))
        if event_name == "completed":
            completed = payload

    assert completed is not None, "missing completed SSE event"
    usage = completed.get("usage")
    assert isinstance(usage, dict), "completed event has no usage object"
    assert usage.get("provider_reported") is True, usage
    assert usage.get("input_tokens") == 10, usage
    assert usage.get("output_tokens") == 8, usage
    assert math.isclose(float(usage["cost_usd"]), 0.000084, abs_tol=1e-12), usage


if __name__ == "__main__":
    main(sys.argv[1])
