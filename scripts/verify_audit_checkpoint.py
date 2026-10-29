#!/usr/bin/env python3
"""Offline verifier for an exported audit checkpoint JSON document."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from app.core.audit_chain import verify_checkpoint


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify an AI Ops Gateway audit checkpoint")
    parser.add_argument("checkpoint", type=Path)
    parser.add_argument("--secret-env", default="AUDIT_CHECKPOINT_SIGNING_KEY")
    args = parser.parse_args()

    import os

    secret = os.environ.get(args.secret_env, "")
    try:
        value = json.loads(args.checkpoint.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"invalid checkpoint document: {exc}", file=sys.stderr)
        return 2
    if not isinstance(value, dict) or not verify_checkpoint(value, secret):
        print("INVALID")
        return 1
    print("VALID")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
