#!/usr/bin/env python3
"""以實際 pyatr bundled rules 評估版本化的 Gateway 合成語料。"""

import argparse
import importlib.metadata
import json
from pathlib import Path

from app.core.threat_detection import ThreatDetector

EVENT_TYPES = {
    "request_input": "llm_input",
    "tool_result": "tool_result",
    "model_output": "llm_output",
}


def evaluate(corpus_path: Path, *, target_rule_id: str | None = None) -> dict[str, object]:
    cases = json.loads(corpus_path.read_text(encoding="utf-8"))
    detector = ThreatDetector(
        blocking=True,
        blocking_rule_ids=[target_rule_id] if target_rule_id else None,
    )
    results = []
    mismatches = 0
    confusion = {"true_positive": 0, "true_negative": 0, "false_positive": 0, "false_negative": 0}

    for case in cases:
        scan = detector.scan(
            case["content"],
            event_type=EVENT_TYPES[case["boundary"]],
            location=case["boundary"],
        )
        matched_rule_ids = [finding["rule_id"] for finding in scan.findings]
        actual_detection = (
            target_rule_id in matched_rule_ids if target_rule_id else bool(scan.findings)
        )
        actual_block = scan.blocked if not target_rule_id else actual_detection and scan.blocked
        matched = (
            actual_detection is case["expected_detection"]
            and actual_block is case["expected_block"]
        )
        mismatches += int(not matched)
        if case["expected_detection"]:
            outcome = "true_positive" if actual_detection else "false_negative"
        else:
            outcome = "false_positive" if actual_detection else "true_negative"
        confusion[outcome] += 1
        results.append(
            {
                "case_id": case["case_id"],
                "boundary": case["boundary"],
                "expected_detection": case["expected_detection"],
                "actual_detection": actual_detection,
                "expected_block": case["expected_block"],
                "actual_block": actual_block,
                "matched_rule_ids": matched_rule_ids,
                "expectation_matched": matched,
                "detection_outcome": outcome,
            }
        )

    benign_count = confusion["true_negative"] + confusion["false_positive"]
    reviewed_detection_count = confusion["true_positive"] + confusion["false_positive"]
    return {
        "pyatr_version": importlib.metadata.version("pyatr"),
        "loaded_rule_count": detector.rule_count,
        "corpus": str(corpus_path),
        "target_rule_id": target_rule_id,
        "case_count": len(results),
        "mismatch_count": mismatches,
        "confusion_matrix": confusion,
        "benign_case_count": benign_count,
        "benign_false_positive_rate": (
            confusion["false_positive"] / benign_count if benign_count else None
        ),
        "reviewed_detection_count": reviewed_detection_count,
        "reviewed_detection_false_positive_rate": (
            confusion["false_positive"] / reviewed_detection_count
            if reviewed_detection_count
            else None
        ),
        "cases": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--corpus",
        type=Path,
        default=Path("tests/fixtures/atr_evaluation_cases.json"),
    )
    parser.add_argument(
        "--rule-id",
        help="只以指定 rule ID 是否命中計算 confusion matrix 與 blocking 結果",
    )
    args = parser.parse_args()
    report = evaluate(args.corpus, target_rule_id=args.rule_id)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if report["mismatch_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
