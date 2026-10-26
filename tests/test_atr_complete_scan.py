"""Complete inspection, subprocess lifecycle, and pinned rule health gates."""

import asyncio
import multiprocessing
import re
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from pyatr import ATREngine

from app.core.atr_engine import FIXED_REPEAT_PATTERN, GatewayATREngine
from app.core.config import Settings
from app.core.threat_detection import ThreatDetector
from app.graph.workflow import build_agent_graph, initial_state
from app.main import create_app


class SleepEngine:
    rules = [object()]

    def evaluate(self, event):
        time.sleep(30)
        return []


class RaisingEngine:
    rules = [object()]

    def evaluate(self, event):
        raise RuntimeError("private-content-must-not-escape")


class RegexEngine:
    rules = [object()]

    def evaluate(self, event):
        re.search(r"(a+)+$", event.content)
        return []


class CompleteEngine:
    rules = [object()]

    def evaluate(self, event):
        if "ATTACK-MIDDLE" in event.content:
            return [
                SimpleNamespace(
                    rule_id="TEST-MIDDLE", title="test", severity="high", confidence="high"
                )
            ]
        return []


def test_full_middle_content_and_exact_size_boundary():
    detector = ThreatDetector(engine=CompleteEngine(), blocking=True, max_content_chars=2000)
    text = "a" * 900 + "ATTACK-MIDDLE" + "z" * 900
    scan = detector.scan(
        text, event_type="llm_input", location="middle", fields={"user_input": text}
    )
    assert scan.complete and scan.blocked and not scan.truncated
    assert detector.scan("x" * 2000, event_type="llm_input", location="boundary").complete


@pytest.mark.parametrize("blocking", [False, True])
def test_oversize_and_fields_fail_closed_without_fabricated_attack(blocking):
    detector = ThreatDetector(engine=CompleteEngine(), blocking=blocking, max_content_chars=1000)
    for text, fields, metadata in [
        ("x" * 1001, {}, {}),
        ("ok", {"tool_response": "x" * 1001}, {}),
        ("ok", {}, {"details": "x" * 1001}),
    ]:
        scan = detector.scan(
            text, fields=fields, metadata=metadata, event_type="tool_response", location="limit"
        )
        assert scan.blocked and not scan.complete and not scan.findings
        assert scan.reason == "content_limit" and scan.needs_record
        assert "x" * 30 not in str(scan.to_event())


@pytest.mark.parametrize("engine", [SleepEngine, RegexEngine])
async def test_timed_out_regex_or_sleep_is_terminated(engine):
    before = {child.pid for child in multiprocessing.active_children()}
    detector = ThreatDetector(engine=engine(), scan_timeout_seconds=0.8)
    started = time.monotonic()
    scan = await detector.ascan("a" * 100 + "!", event_type="llm_input", location="timeout")
    assert scan.blocked and not scan.complete
    assert scan.reason in {"timeout", "engine_error"}
    assert time.monotonic() - started < 3
    assert not ({child.pid for child in multiprocessing.active_children()} - before)


async def test_cancelled_scan_releases_child_and_capacity():
    before = {child.pid for child in multiprocessing.active_children()}
    detector = ThreatDetector(engine=SleepEngine(), scan_timeout_seconds=5, scan_max_workers=1)
    pending = asyncio.create_task(detector.ascan("ok", event_type="llm_input", location="cancel"))
    await asyncio.sleep(0.3)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    async with asyncio.timeout(2):
        for _ in range(100):
            if not ({child.pid for child in multiprocessing.active_children()} - before):
                break
            await asyncio.sleep(0.02)
        else:
            pytest.fail("Cancelled ATR scan left a child process")
    assert detector._slots.acquire(blocking=False)
    detector._slots.release()


def test_engine_errors_and_invalid_unicode_never_echo_private_input():
    detector = ThreatDetector(engine=RaisingEngine())
    for value in ["private-content-must-not-escape", "bad\ud800"]:
        scan = detector.scan(value, event_type="llm_input", location="invalid")
        assert not scan.complete and scan.blocked
        assert value not in str(scan.to_event())


def test_all_active_bundled_regex_conditions_compile_and_local_fix_matches():
    engine = GatewayATREngine()
    assert engine.load_default_rules() == 833
    assert not engine._failed_regexes
    assert len(engine.verify()) == 64
    rule = next(rule for rule in engine.rules if rule.id == "ATR-2026-00290")
    assert rule.conditions[3].value == FIXED_REPEAT_PATTERN
    pattern = next(pattern for index, pattern in engine._compiled[rule.id] if index == 3)
    assert pattern.search("company " * 160)
    assert not pattern.search("one two three four " * 3)


def test_failed_necessary_regex_is_not_silently_accepted():
    base = GatewayATREngine()
    base.load_default_rules()
    rule = next(rule for rule in base.rules if rule.status == "stable")
    invalid = replace(rule.conditions[0], value="(missing-close")
    engine = ATREngine()
    engine.load_rule(replace(rule, conditions=(invalid,)))
    with pytest.raises(RuntimeError, match="integrity"):
        ThreatDetector(engine=engine)


def test_partial_rule_directory_is_rejected_instead_of_skipping_bad_files(tmp_path):
    (tmp_path / "valid.yaml").write_text(
        "id: TEST-ONE\ntitle: test\nseverity: high\nstatus: stable\ndetection:\n"
        "  conditions:\n    - field: content\n      operator: contains\n      value: attack\n"
    )
    (tmp_path / "broken.yaml").write_text("id: TEST-BROKEN\ndetection: invalid\n")
    with pytest.raises(RuntimeError, match="validation"):
        ThreatDetector(rules_dir=str(tmp_path))


def test_oversize_admission_returns_413_without_model_or_private_error_echo():
    settings = Settings(
        _env_file=None,
        redis_url="redis://127.0.0.1:1/0",
        langgraph_checkpointing=False,
        atr_max_content_chars=1000,
        pii_redaction=False,
    )
    with TestClient(create_app(settings=settings)) as client:
        response = client.post("/v1/agent/stream", json={"query": "private-sensitive-data " * 80})
    assert response.status_code == 413
    assert response.json()["detail"] == {
        "error": "atr_scan_incomplete",
        "scan_reason": "content_limit",
    }
    assert "private-sensitive-data" not in response.text


class OversizeModel:
    async def complete(self, system, prompt):
        return ""

    async def stream(self, system, prompt):
        yield "PRIVATE-UNSCANNED-" * 100


async def test_detect_only_output_is_buffered_and_oversize_never_streams():
    detector = ThreatDetector(engine=CompleteEngine(), max_content_chars=1000, blocking=False)
    graph = build_agent_graph(OversizeModel(), object(), threat_detector=detector)
    events = [
        event
        async for event in graph.astream(
            initial_state("oversize", "summarize", None, 0), stream_mode="custom"
        )
    ]
    assert "PRIVATE-UNSCANNED" not in str(events)
    assert not any(event["type"] == "draft_token" for event in events)
    failure = next(event for event in events if event["type"] == "threat_detection")
    assert failure["scan_complete"] is False and failure["scan_reason"] == "content_limit"
