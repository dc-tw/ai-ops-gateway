from __future__ import annotations

import asyncio
import contextlib
import math
import multiprocessing
import threading
import time
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from pyatr import AgentEvent, ATREngine

from app.core.atr_engine import GatewayATREngine

SEVERITY_RANK = {"informational": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}


@dataclass(frozen=True)
class ThreatScan:
    event_type: str
    location: str
    findings: tuple[dict[str, Any], ...]
    blocked: bool
    truncated: bool = False  # Legacy field: scans are no longer truncated.
    complete: bool = True
    reason: str = "complete"

    @property
    def needs_record(self) -> bool:
        return bool(self.findings) or not self.complete

    def to_event(self) -> dict[str, Any]:
        return {
            "type": "threat_detection",
            "status": "blocked" if self.blocked else "detected" if self.findings else "passed",
            "event_type": self.event_type,
            "location": self.location,
            "finding_count": len(self.findings),
            "findings": list(self.findings),
            "truncated": self.truncated,
            "scan_complete": self.complete,
            "scan_reason": self.reason,
        }


class ThreatBlockedError(RuntimeError):
    def __init__(self, scan: ThreatScan) -> None:
        self.scan = scan
        detail = (
            ", ".join(item["rule_id"] for item in scan.findings) if scan.complete else scan.reason
        )
        super().__init__(f"ATR blocked {scan.location}: {detail}")


def reject_incomplete_scan(scan: ThreatScan) -> None:
    """HTTP admission reports capacity failures separately from attack matches."""
    if scan.complete:
        return
    from fastapi import HTTPException

    code = (
        413 if scan.reason == "content_limit" else 422 if scan.reason == "invalid_content" else 503
    )
    raise HTTPException(
        status_code=code, detail={"error": "atr_scan_incomplete", "scan_reason": scan.reason}
    )


def _evaluate_process(engine, event, connection, timeout_seconds, memory_mb):
    """Only this disposable process evaluates regexes; errors never echo input."""
    try:
        import os
        import resource
        import signal
        import sys

        resource.setrlimit(resource.RLIMIT_CPU, (math.ceil(timeout_seconds) + 1,) * 2)
        if sys.platform.startswith("linux"):
            ceiling = memory_mb * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (ceiling, ceiling))
        # Independently bound wall time if the supervisor is abruptly killed.
        signal.signal(signal.SIGALRM, lambda *_: os._exit(124))
        signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
        matches = engine.evaluate(event)
        connection.send(
            (
                True,
                [
                    {
                        key: getattr(match, key)
                        for key in ("rule_id", "title", "severity", "confidence")
                    }
                    for match in matches
                ],
            )
        )
    except BaseException:
        with contextlib.suppress(Exception):
            connection.send((False, []))
    finally:
        connection.close()


class ThreatDetector:
    """Full, bounded, privacy-safe ATR checks in killable subprocesses."""

    def __init__(
        self,
        *,
        enabled: bool = True,
        blocking: bool = False,
        min_severity: str = "medium",
        block_severity: str = "high",
        max_content_chars: int = 100_000,
        rules_dir: str = "",
        blocking_rule_ids: list[str] | None = None,
        blocking_excluded_rule_ids: list[str] | None = None,
        engine: ATREngine | None = None,
        scan_timeout_seconds: float = 5.0,
        scan_max_workers: int = 4,
        scan_memory_mb: int = 512,
    ) -> None:
        if (
            max_content_chars < 1
            or not math.isfinite(scan_timeout_seconds)
            or scan_timeout_seconds <= 0
        ):
            raise ValueError("Invalid ATR scan limits")
        if scan_max_workers < 1 or scan_memory_mb < 64:
            raise ValueError("Invalid ATR worker limits")
        self.enabled, self.blocking = enabled, blocking
        self.min_severity, self.block_severity = min_severity, block_severity
        self.max_content_chars = max_content_chars
        self.scan_timeout_seconds = scan_timeout_seconds
        self.scan_memory_mb = scan_memory_mb
        self._slots = threading.BoundedSemaphore(scan_max_workers)
        self.blocking_rule_ids = frozenset(blocking_rule_ids or [])
        self.blocking_excluded_rule_ids = frozenset(blocking_excluded_rule_ids or [])
        self.engine = engine if engine is not None else GatewayATREngine()
        self.ruleset_fingerprint = "disabled"
        if not enabled:
            self.rule_count = 0
            return
        if engine is not None:
            self.rule_count = len(engine.rules)
        elif rules_dir:
            self.rule_count = self.engine.load_rules_from_directory(rules_dir)
        else:
            self.rule_count = self.engine.load_default_rules()
        if self.rule_count == 0:
            raise RuntimeError("ATR is enabled but no threat rules were loaded")
        if isinstance(self.engine, GatewayATREngine):
            self.ruleset_fingerprint = self.engine.verify()
        elif isinstance(self.engine, ATREngine):
            active = {
                rule.id for rule in self.engine.rules if rule.status not in {"draft", "deprecated"}
            }
            if any(rule in active for rule, _ in self.engine._failed_regexes):
                raise RuntimeError("ATR rules failed integrity validation")
            self.ruleset_fingerprint = "injected-engine"
        else:
            self.ruleset_fingerprint = "test-engine"

    @classmethod
    def from_settings(cls, settings: Any) -> ThreatDetector:
        return cls(
            enabled=settings.atr_enabled,
            blocking=settings.atr_blocking,
            min_severity=settings.atr_min_severity,
            block_severity=settings.atr_block_severity,
            max_content_chars=settings.atr_max_content_chars,
            rules_dir=settings.atr_rules_dir,
            blocking_rule_ids=settings.atr_blocking_rule_ids,
            blocking_excluded_rule_ids=settings.atr_blocking_excluded_rule_ids,
            scan_timeout_seconds=settings.atr_scan_timeout_seconds,
            scan_max_workers=settings.atr_scan_max_workers,
            scan_memory_mb=settings.atr_scan_memory_mb,
        )

    def scan(
        self,
        content: str,
        *,
        event_type: str,
        location: str,
        fields: dict[str, object] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> ThreatScan:
        return self._scan(content, event_type, location, fields, metadata, threading.Event())

    def _scan(self, content, event_type, location, fields, metadata, cancelled):
        if not self.enabled:
            return ThreatScan(event_type, location, (), False, reason="disabled")

        def incomplete(reason):
            # Failure to inspect is not an attack finding, but always denies release.
            return ThreatScan(event_type, location, (), True, complete=False, reason=reason)

        try:
            text = str(content)
            field_text = {str(key): str(value) for key, value in (fields or {}).items()}
            meta_text = {str(key): str(value) for key, value in (metadata or {}).items()}
            values = {text, *field_text.values(), *meta_text.values()}
            if (
                len(field_text) + len(meta_text) > 64
                or any(len(key) > 128 for key in (*field_text, *meta_text))
                or sum(len(value) for value in values) > self.max_content_chars
            ):
                return incomplete("content_limit")
            for value in (*values, *field_text, *meta_text):
                value.encode("utf-8")
        except (UnicodeError, ValueError, TypeError):
            return incomplete("invalid_content")
        started = time.monotonic()
        while not self._slots.acquire(blocking=False):
            if cancelled.wait(0.02):
                return incomplete("cancelled")
            if time.monotonic() - started >= self.scan_timeout_seconds:
                return incomplete("capacity_timeout")
        process = None
        receiver = sender = None
        try:
            context = multiprocessing.get_context("spawn")
            receiver, sender = context.Pipe(duplex=False)
            process = context.Process(
                target=_evaluate_process,
                args=(
                    self.engine,
                    AgentEvent(
                        content=text, event_type=event_type, fields=field_text, metadata=meta_text
                    ),
                    sender,
                    self.scan_timeout_seconds,
                    self.scan_memory_mb,
                ),
                daemon=True,
            )
            process.start()
            sender.close()
            while not receiver.poll(0.02):
                if cancelled.is_set():
                    return incomplete("cancelled")
                if time.monotonic() - started >= self.scan_timeout_seconds:
                    return incomplete("timeout")
                if not process.is_alive():
                    return incomplete("engine_error")
            ok, matches = receiver.recv()
            if not ok:
                return incomplete("engine_error")
            findings = []
            for match in matches:
                if SEVERITY_RANK.get(match["severity"], 0) < SEVERITY_RANK.get(
                    self.min_severity, 2
                ):
                    continue
                candidate = (
                    SEVERITY_RANK.get(match["severity"], 0)
                    >= SEVERITY_RANK.get(self.block_severity, 3)
                    and (not self.blocking_rule_ids or match["rule_id"] in self.blocking_rule_ids)
                    and match["rule_id"] not in self.blocking_excluded_rule_ids
                )
                findings.append(
                    {
                        **match,
                        "finding_id": str(uuid4()),
                        "block_candidate": candidate,
                        "enforcement_action": "blocked"
                        if self.blocking and candidate
                        else "detected",
                    }
                )
            return ThreatScan(
                event_type,
                location,
                tuple(findings),
                any(item["enforcement_action"] == "blocked" for item in findings),
            )
        except Exception:
            return incomplete("engine_error")
        finally:
            if process is not None and process.pid is not None:
                if process.is_alive():
                    process.terminate()
                process.join(0.2)
                if process.is_alive():
                    process.kill()
                    process.join(0.2)
                if not process.is_alive():
                    process.close()
            for connection in (receiver, sender):
                if connection is not None:
                    connection.close()
            self._slots.release()

    async def ascan(
        self,
        content: str,
        *,
        event_type: str,
        location: str,
        fields: dict[str, object] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> ThreatScan:
        cancelled = threading.Event()
        try:
            return await asyncio.to_thread(
                self._scan, content, event_type, location, fields, metadata, cancelled
            )
        except asyncio.CancelledError:
            cancelled.set()
            raise
