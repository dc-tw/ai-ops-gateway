from __future__ import annotations

import re
import threading
import time
from collections import defaultdict
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)

    @staticmethod
    def _key(name: str, labels: dict[str, str]) -> tuple[str, tuple[tuple[str, str], ...]]:
        return name, tuple(sorted(labels.items()))

    def inc(self, name: str, value: float = 1, **labels: str) -> None:
        with self._lock:
            self._counters[self._key(name, labels)] += value

    def gauge_add(self, name: str, value: float, **labels: str) -> None:
        with self._lock:
            self._gauges[self._key(name, labels)] += value

    def render(self) -> str:
        def line(key: tuple[str, tuple[tuple[str, str], ...]], value: float) -> str:
            name, labels = key
            encoded = ""
            if labels:
                pairs = ",".join(f'{k}="{v}"' for k, v in labels)
                encoded = "{" + pairs + "}"
            return f"{name}{encoded} {value}\n"

        with self._lock:
            items = sorted(self._counters.items()) + sorted(self._gauges.items())
            return "".join(line(key, value) for key, value in items)


metrics = Metrics()


def normalize_path(path: str) -> str:
    return re.sub(
        r"[0-9a-f]{8}-[0-9a-f-]{27,}", "{id}", path, flags=re.IGNORECASE
    )


def observe_agent_event(event: dict[str, Any], *, model: str) -> None:
    event_type = str(event.get("type", "unknown"))
    metrics.inc("ai_gateway_agent_events_total", event=event_type, model=model)
    if event_type in {"draft_token", "token"}:
        metrics.inc(
            "ai_gateway_output_characters_total",
            len(str(event.get("content", ""))),
            stage="draft" if event_type == "draft_token" else "validated",
            model=model,
        )
    elif event_type == "retry":
        metrics.inc("ai_gateway_retries_total", model=model)
    elif event_type == "evidence_validation":
        metrics.inc(
            "ai_gateway_evidence_validation_total",
            model=model,
            status=str(event.get("status", "unknown")),
        )
    elif event_type == "tool_status":
        metrics.inc(
            "ai_gateway_tool_calls_total",
            tool=str(event.get("tool", "unknown")),
            status=str(event.get("status", "unknown")),
        )
    elif event_type == "model_usage":
        usage = event.get("delta_usage", {})
        if isinstance(usage, dict):
            metrics.inc(
                "ai_gateway_model_input_tokens_total",
                float(usage.get("input_tokens", 0)),
                model=model,
            )
            metrics.inc(
                "ai_gateway_model_output_tokens_total",
                float(usage.get("output_tokens", 0)),
                model=model,
            )
            metrics.inc(
                "ai_gateway_model_cost_usd_total",
                float(usage.get("cost_usd", 0)),
                model=model,
            )


def install_observability(app: FastAPI, settings: Any) -> None:
    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next):
        started = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration = time.perf_counter() - started
            labels = {
                "method": request.method,
                "path": normalize_path(request.url.path),
                "status": str(status_code),
            }
            metrics.inc("ai_gateway_http_requests_total", **labels)
            metrics.inc("ai_gateway_http_request_duration_seconds_sum", duration, **labels)

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics() -> PlainTextResponse:
        return PlainTextResponse(metrics.render(), media_type="text/plain; version=0.0.4")

    if settings.otel_exporter_otlp_endpoint:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        provider = TracerProvider(
            resource=Resource.create({"service.name": settings.otel_service_name})
        )
        provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint)
            )
        )
        trace.set_tracer_provider(provider)
        FastAPIInstrumentor.instrument_app(app)
