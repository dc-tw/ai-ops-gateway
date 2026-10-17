from __future__ import annotations

from typing import Any


async def record_threat_findings(
    runtime: Any,
    settings: Any,
    tenant_id: str,
    event: dict[str, Any],
    *,
    context_id: str,
    model: str,
) -> list[dict[str, Any]]:
    """Store ATR metadata when a Redis-compatible runtime supports calibration data."""
    if runtime is None or not event.get("findings"):
        return []
    save = getattr(runtime, "save_threat_findings", None)
    if save is None:
        return []
    return await save(
        tenant_id,
        event,
        context_id=context_id,
        model=model,
        ttl_seconds=settings.atr_findings_ttl_seconds,
        max_records=settings.atr_findings_max_records,
    )
