import json
from pathlib import Path

from app.core.threat_detection import ThreatDetector

CORPUS_PATH = Path(__file__).parent / "fixtures" / "atr_evaluation_cases.json"
BENIGN_CORPUS_PATH = Path(__file__).parent / "fixtures" / "atr_benign_evaluation_cases.json"
CANDIDATE_RULE_CORPUS_PATH = (
    Path(__file__).parent / "fixtures" / "atr_00213_review_cases.json"
)


def test_atr_evaluation_corpus_has_unique_cases_for_every_boundary() -> None:
    cases = json.loads(CORPUS_PATH.read_text(encoding="utf-8"))

    case_ids = [case["case_id"] for case in cases]
    assert len(case_ids) == len(set(case_ids))

    for boundary in {"request_input", "tool_result", "model_output"}:
        boundary_cases = [case for case in cases if case["boundary"] == boundary]
        assert any(case["expected_detection"] for case in boundary_cases)
        assert any(not case["expected_detection"] for case in boundary_cases)


def test_atr_evaluation_corpus_declares_expected_enforcement_and_review() -> None:
    cases = json.loads(CORPUS_PATH.read_text(encoding="utf-8"))

    for case in cases:
        assert isinstance(case["content"], str) and case["content"]
        assert isinstance(case["expected_detection"], bool)
        assert isinstance(case["expected_block"], bool)
        assert case["review_verdict"] in {
            "true_positive",
            "false_positive_if_detected",
            "not_applicable",
        }
        if case["expected_block"]:
            assert case["expected_detection"] is True


def test_bundled_atr_rules_match_the_seed_corpus_expectations() -> None:
    cases = json.loads(CORPUS_PATH.read_text(encoding="utf-8"))
    detector = ThreatDetector(blocking=True)
    event_types = {
        "request_input": "llm_input",
        "tool_result": "tool_result",
        "model_output": "llm_output",
    }

    for case in cases:
        scan = detector.scan(
            case["content"],
            event_type=event_types[case["boundary"]],
            location=case["boundary"],
        )
        assert bool(scan.findings) is case["expected_detection"], case["case_id"]
        assert scan.blocked is case["expected_block"], case["case_id"]


def test_benign_evaluation_corpus_has_reviewed_boundary_coverage() -> None:
    cases = json.loads(BENIGN_CORPUS_PATH.read_text(encoding="utf-8"))

    assert len(cases) >= 60
    assert len({case["case_id"] for case in cases}) == len(cases)
    for boundary in {"request_input", "tool_result", "model_output"}:
        assert sum(case["boundary"] == boundary for case in cases) >= 20
    for case in cases:
        assert case["expected_detection"] is False
        assert case["expected_block"] is False
        assert case["review_verdict"] in {"false_positive_if_detected", "not_applicable"}


def test_candidate_rule_corpus_has_enough_reviewed_findings_and_near_matches() -> None:
    cases = json.loads(CANDIDATE_RULE_CORPUS_PATH.read_text(encoding="utf-8"))

    assert sum(case["expected_detection"] for case in cases) >= 20
    assert sum(not case["expected_detection"] for case in cases) >= 10
    assert all(case["boundary"] == "tool_result" for case in cases)
    assert len({case["case_id"] for case in cases}) == len(cases)
