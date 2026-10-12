import hashlib
import json
from collections.abc import AsyncIterator

import pytest

from app.graph.workflow import (
    _format_history,
    _inspect_evidence,
    _merge_usage,
    build_agent_graph,
    initial_state,
)


def test_usage_is_exact_only_when_every_model_call_reports_tokens() -> None:
    exact = _merge_usage(
        {},
        {
            "input_tokens": 10,
            "output_tokens": 5,
            "total_tokens": 15,
            "cost_usd": 0.01,
            "provider_reported": True,
        },
    )
    partial = _merge_usage(exact, {})

    assert exact["provider_reported"] is True
    assert partial["provider_reported"] is False
    assert partial["model_calls"] == 2
    assert partial["provider_reported_calls"] == 1


class FakeModel:
    def __init__(self, candidates: list[str]) -> None:
        self._candidates = iter(candidates)

    async def complete(self, system: str, prompt: str) -> str:
        return next(self._candidates)

    async def stream(self, system: str, prompt: str) -> AsyncIterator[str]:
        candidate = next(self._candidates)
        midpoint = max(1, len(candidate) // 2)
        yield candidate[:midpoint]
        yield candidate[midpoint:]


class FakeScraper:
    async def scrape(self, url: str) -> dict[str, object]:
        text = "Useful page content"
        return {
            "url": url,
            "title": "Example",
            "text": text,
            "characters": len(text),
            "retrieved_at": "2026-01-01T00:00:00+00:00",
            "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
        }


class FailingScraper:
    def __init__(self) -> None:
        self.calls = 0

    async def scrape(self, url: str) -> dict[str, object]:
        self.calls += 1
        raise ConnectionError("simulated browser failure")


@pytest.mark.asyncio
async def test_quality_failure_retries_then_completes() -> None:
    graph = build_agent_graph(
        FakeModel(["bad", "A sufficiently detailed verified response"]), FakeScraper()
    )
    events = [
        event
        async for event in graph.astream(
            initial_state("request-1", "summarize", None, 2), stream_mode="custom"
        )
    ]

    retries = [event for event in events if event.get("type") == "retry"]
    completed = [event for event in events if event.get("type") == "completed"]
    assert len(retries) == 1
    assert completed[0]["answer"] == "A sufficiently detailed verified response"
    assert completed[0]["attempts"] == 2
    assert completed[0]["evidence_validated"] is True
    assert "".join(
        str(event["content"])
        for event in events
        if event.get("type") == "draft_token"
        and event.get("attempt") == completed[0]["attempts"]
    ) == completed[0]["answer"]


@pytest.mark.asyncio
async def test_browser_tool_is_used_when_url_is_present() -> None:
    graph = build_agent_graph(
        FakeModel(
            ["Useful page content provides a sufficiently detailed response [source:1]."]
        ),
        FakeScraper(),
    )
    events = [
        event
        async for event in graph.astream(
            initial_state("request-2", "summarize", "https://example.com", 0),
            stream_mode="custom",
        )
    ]
    tool_events = [event for event in events if event.get("type") == "tool_status"]
    assert [event["status"] for event in tool_events] == ["started", "completed"]

    started_phases = [
        event["phase"]
        for event in events
        if event.get("type") == "node_status" and event.get("status") == "started"
    ]
    assert started_phases == [
        "Planning",
        "Tool_Call",
        "Parsing",
        "Evidence Validation",
        "Final_Answer",
    ]


@pytest.mark.asyncio
async def test_tool_failure_retries_then_returns_transparent_fallback() -> None:
    scraper = FailingScraper()
    graph = build_agent_graph(
        FakeModel(
            [
                "Candidate generated without required web data one",
                "Candidate generated without required web data two",
                "Candidate generated without required web data three",
            ]
        ),
        scraper,
    )
    events = [
        event
        async for event in graph.astream(
            initial_state("request-3", "summarize", "https://invalid.example", 2),
            stream_mode="custom",
        )
    ]

    assert scraper.calls == 3
    assert len([event for event in events if event.get("type") == "retry"]) == 2
    completed = [event for event in events if event.get("type") == "completed"]
    assert completed[0]["evidence_validated"] is False
    assert completed[0]["attempts"] == 3


def test_evidence_validation_rejects_number_not_present_in_source() -> None:
    source_text = "Revenue increased by 12% this quarter."
    state = initial_state("request-4", "summarize", "https://example.com", 0)
    state["candidate"] = (
        "The source reports that revenue increased by 21% this quarter [source:1]."
    )
    state["tool_result"] = json.dumps(
        {
            "url": "https://example.com",
            "text": source_text,
            "characters": len(source_text),
            "retrieved_at": "2026-01-01T00:00:00+00:00",
            "content_sha256": hashlib.sha256(source_text.encode()).hexdigest(),
        }
    )

    errors, report = _inspect_evidence(state)

    assert "unsupported_numeric_facts" in errors
    assert report["unsupported_numeric_facts"] == ["21%"]
    assert report["grounding_passed"] is False


def test_evidence_validation_accepts_supported_number_and_provenance() -> None:
    source_text = "Revenue increased by 12% this quarter."
    state = initial_state("request-5", "summarize", "https://example.com", 0)
    state["candidate"] = (
        "The source reports that revenue increased by 12% this quarter [source:1]."
    )
    state["tool_result"] = json.dumps(
        {
            "url": "https://example.com",
            "text": source_text,
            "characters": len(source_text),
            "retrieved_at": "2026-01-01T00:00:00+00:00",
            "content_sha256": hashlib.sha256(source_text.encode()).hexdigest(),
        }
    )

    errors, report = _inspect_evidence(state)

    assert errors == []
    assert report["provenance_passed"] is True
    assert report["grounding_passed"] is True
    assert report["claim_citations"][0]["source_urls"] == ["https://example.com"]


def test_evidence_validation_rejects_tampered_source_hash() -> None:
    source_text = "Revenue increased by 12% this quarter."
    state = initial_state("request-6", "summarize", "https://example.com", 0)
    state["candidate"] = "Revenue increased by 12% [source:1]."
    state["tool_result"] = json.dumps(
        {
            "url": "https://example.com",
            "text": source_text,
            "characters": len(source_text),
            "retrieved_at": "2026-01-01T00:00:00+00:00",
            "content_sha256": "tampered",
        }
    )

    errors, report = _inspect_evidence(state)

    assert "source_provenance_incomplete" in errors
    assert report["source_hash_verified"] is False


def test_evidence_validation_tracks_multiple_sources_and_conflicts() -> None:
    texts = ["Adoption reached 12% this year.", "Adoption reached 18% this year."]
    state = initial_state(
        "request-7",
        "compare sources",
        None,
        0,
        target_urls=["https://one.example", "https://two.example"],
    )
    state["candidate"] = (
        "One source reports 12% [source:1]. Another reports 18% [source:2]."
    )
    state["tool_calls"] = [
        {"name": "scrape_dynamic_page", "arguments": {"url": url}}
        for url in state["target_urls"]
    ]
    state["tool_result"] = json.dumps(
        [
            {
                "url": url,
                "text": text,
                "characters": len(text),
                "retrieved_at": "2026-01-01T00:00:00+00:00",
                "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
            }
            for url, text in zip(state["target_urls"], texts, strict=True)
        ]
    )

    errors, report = _inspect_evidence(state)

    assert errors == []
    assert len(report["evidence_sources"]) == 2
    assert report["potential_numeric_conflicts"] == [["12%", "18%"]]


def test_evidence_validation_rejects_claim_cited_to_wrong_source() -> None:
    texts = ["Adoption reached 12% this year.", "Adoption reached 18% this year."]
    state = initial_state(
        "request-8",
        "compare sources",
        None,
        0,
        target_urls=["https://one.example", "https://two.example"],
    )
    state["candidate"] = "Adoption reached 18% [source:1]."
    state["tool_result"] = json.dumps(
        [
            {
                "url": url,
                "text": text,
                "characters": len(text),
                "retrieved_at": "2026-01-01T00:00:00+00:00",
                "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
            }
            for url, text in zip(state["target_urls"], texts, strict=True)
        ]
    )

    errors, report = _inspect_evidence(state)

    assert "claim_not_supported_by_cited_source" in errors
    assert report["misgrounded_claims"] == [1]


def test_history_uses_token_budget_and_marks_untrusted_context() -> None:
    history = [
        {"role": "user", "content": "old " * 100},
        {"role": "assistant", "content": "recent answer"},
    ]
    formatted = _format_history(history, max_tokens=20)
    assert "Earlier messages omitted" in formatted
    assert "<CONVERSATION role=assistant>" in formatted
    assert "recent answer" in formatted
