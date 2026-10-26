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
    source_automation_id: str | None = None,
) -> list[dict[str, Any]]:
    """Store ATR metadata when a Redis-compatible runtime supports calibration data."""
    if runtime is None or not event.get("findings"):
        return []
    save = getattr(runtime, "save_threat_findings", None)
    if save is None:
        return []
    # Real Redis commits the source finding and its queue event together.
    # Lightweight test/runtime adapters without Redis retain their existing API.
    origin = {"source_automation_id": source_automation_id} if hasattr(runtime, "_client") else {}
    return await save(
        tenant_id,
        event,
        context_id=context_id,
        model=model,
        ttl_seconds=settings.atr_findings_ttl_seconds,
        max_records=settings.atr_findings_max_records,
        **origin,
    )
