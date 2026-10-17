from __future__ import annotations

from collections import defaultdict
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.core.security import authorize
from app.core.threat_detection import SEVERITY_RANK

router = APIRouter(prefix="/v1/security/threat-findings", tags=["security"])


class ThreatFindingReview(BaseModel):
    verdict: Literal["true_positive", "false_positive", "acceptable_risk"]
    reason: str = Field(min_length=1, max_length=500)


def _runtime(request: Request):
    runtime = request.app.state.redis
    if runtime is None or not hasattr(runtime, "list_threat_findings"):
        raise HTTPException(status_code=503, detail="Threat calibration requires Redis")
    return runtime


@router.get("")
async def list_threat_findings(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=1_000)] = 100,
    review_status: Literal[
        "", "unreviewed", "true_positive", "false_positive", "acceptable_risk"
    ] = "",
    rule_id: Annotated[str, Query(max_length=64)] = "",
) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    findings = await _runtime(request).list_threat_findings(
        principal.tenant_id,
        limit=limit,
        review_status=review_status,
        rule_id=rule_id,
    )
    return {"findings": findings, "count": len(findings)}


@router.post("/{finding_id}/review")
async def review_threat_finding(
    finding_id: UUID, payload: ThreatFindingReview, request: Request
) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    settings = request.app.state.settings
    finding = await _runtime(request).review_threat_finding(
        principal.tenant_id,
        str(finding_id),
        review_status=payload.verdict,
        reason=payload.reason,
        reviewer=principal.subject,
        ttl_seconds=settings.atr_findings_ttl_seconds,
    )
    if finding is None:
        raise HTTPException(status_code=404, detail="Threat finding not found")
    await request.app.state.redis.append_audit(
        principal.tenant_id,
        {
            "action": "threat_finding.review",
            "status": payload.verdict,
            "subject": principal.subject,
            "finding_id": str(finding_id),
            "rule_id": finding["rule_id"],
        },
    )
    return finding


@router.get("/stats/rules")
async def threat_rule_stats(request: Request) -> dict[str, Any]:
    principal = await authorize(request, "admin")
    settings = request.app.state.settings
    findings = await _runtime(request).list_threat_findings(
        principal.tenant_id,
        limit=settings.atr_findings_max_records,
    )
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "rule_id": "",
            "title": "",
            "severity": "unknown",
            "detections": 0,
            "blocked": 0,
            "unreviewed": 0,
            "true_positive": 0,
            "false_positive": 0,
            "acceptable_risk": 0,
        }
    )
    for finding in findings:
        rule_id = str(finding.get("rule_id", "unknown"))
        row = grouped[rule_id]
        row["rule_id"] = rule_id
        row["title"] = str(finding.get("title", ""))
        row["severity"] = str(finding.get("severity", "unknown"))
        row["detections"] += 1
        if finding.get("enforcement_status") == "blocked":
            row["blocked"] += 1
        review_status = str(finding.get("review_status", "unreviewed"))
        if review_status in row:
            row[review_status] += 1

    rules = []
    for row in grouped.values():
        reviewed = row["true_positive"] + row["false_positive"]
        false_positive_rate = (
            row["false_positive"] / reviewed if reviewed else None
        )
        row["reviewed_for_precision"] = reviewed
        row["false_positive_rate"] = false_positive_rate
        row["recommended_for_blocking"] = bool(
            reviewed >= settings.atr_calibration_min_reviews
            and false_positive_rate is not None
            and false_positive_rate <= settings.atr_calibration_max_false_positive_rate
            and SEVERITY_RANK.get(row["severity"], 0)
            >= SEVERITY_RANK.get(settings.atr_block_severity, 3)
        )
        rules.append(row)
    rules.sort(key=lambda row: (-SEVERITY_RANK.get(row["severity"], 0), row["rule_id"]))
    return {
        "mode": "blocking" if settings.atr_blocking else "detection_only",
        "calibration": {
            "minimum_reviews": settings.atr_calibration_min_reviews,
            "maximum_false_positive_rate": (
                settings.atr_calibration_max_false_positive_rate
            ),
        },
        "sample_size": len(findings),
        "rules": rules,
    }
