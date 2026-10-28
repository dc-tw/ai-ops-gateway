"""CAS-published model identity and privacy-safe per-call telemetry.

Configured aliases are routing choices, not proof of deployed weights.  This
module lets an administrator publish the identity disclosed by the provider and
makes that publication visible to every Gateway replica through Redis.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime
from typing import Any

from redis.exceptions import WatchError

from app.core.planning_store import canonical_hash


class ModelRegistryConflict(ValueError):
    pass


class ModelRegistryIntegrityError(ValueError):
    pass


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _alias_key(alias: str) -> str:
    return hashlib.sha256(alias.encode("utf-8")).hexdigest()


def _decode(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except (TypeError, ValueError, RecursionError):
        raise ModelRegistryIntegrityError("Stored model publication is invalid") from None
    if not isinstance(value, dict):
        raise ModelRegistryIntegrityError("Stored model publication is invalid")
    return value


class ModelRegistry:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self.client = runtime._client

    def key(self, tenant: str, alias: str) -> str:
        return self.runtime._tenant_key(tenant, f"models:publication:{_alias_key(alias)}")

    def history_key(self, tenant: str, alias: str, revision: int) -> str:
        return self.runtime._tenant_key(
            tenant, f"models:publication:{_alias_key(alias)}:revision:{revision}"
        )

    def calibration_key(self, tenant: str, publication_sha256: str) -> str:
        return self.runtime._tenant_key(
            tenant, f"models:calibration:{publication_sha256.removeprefix('sha256:')}"
        )

    def calibration_history_key(self, tenant: str, publication_sha256: str, revision: int) -> str:
        return self.runtime._tenant_key(
            tenant,
            f"models:calibration:{publication_sha256.removeprefix('sha256:')}:revision:{revision}",
        )

    def telemetry_key(self, tenant: str) -> str:
        return self.runtime._tenant_key(tenant, "models:call-telemetry")

    async def get(
        self, tenant: str, alias: str, *, client: Any | None = None, watch: bool = False
    ) -> dict[str, Any] | None:
        reader = client or self.client
        key = self.key(tenant, alias)
        if watch:
            await reader.watch(key)
        value = _decode(await reader.get(key))
        if value is None:
            return None
        body = {key: item for key, item in value.items() if key != "publication_sha256"}
        if (
            value.get("schema_version") != 1
            or value.get("tenant_id") != tenant
            or value.get("alias") != alias
            or type(value.get("revision")) is not int
            or value["revision"] < 1
            or value.get("publication_sha256") != "sha256:" + canonical_hash(body)
        ):
            raise ModelRegistryIntegrityError("Stored model publication failed verification")
        return value

    async def publish(
        self,
        tenant: str,
        subject: str,
        route: dict[str, Any],
        disclosure: dict[str, Any],
        expected_revision: int,
    ) -> dict[str, Any]:
        alias = route["alias"]
        key = self.key(tenant, alias)
        for _ in range(8):
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    current = await self.get(tenant, alias, client=pipe)
                    revision = current["revision"] if current else 0
                    if revision != expected_revision:
                        raise ModelRegistryConflict("Model publication revision conflict")
                    now = datetime.now(UTC).isoformat()
                    body = {
                        "schema_version": 1,
                        "tenant_id": tenant,
                        "alias": alias,
                        "revision": revision + 1,
                        "route_fingerprint": route["route_fingerprint"],
                        "resolved_model": route["resolved_model"],
                        "actual_model_version": disclosure.get("actual_model_version"),
                        "deployment_id": disclosure.get("deployment_id"),
                        "tokenizer_version": disclosure.get("tokenizer_version"),
                        "runtime_version": disclosure.get("runtime_version"),
                        "published_at": now,
                        "published_by": subject,
                    }
                    value = body | {"publication_sha256": "sha256:" + canonical_hash(body)}
                    history = self.history_key(tenant, alias, body["revision"])
                    await pipe.watch(history)
                    if await pipe.exists(history):
                        raise ModelRegistryIntegrityError("Immutable publication already exists")
                    pipe.multi()
                    pipe.set(key, _json(value))
                    pipe.set(history, _json(value))
                    pipe.sadd(self.runtime._tenant_key(tenant, "models:publications"), alias)
                    pipe.xadd(
                        self.runtime._tenant_key(tenant, "audit"),
                        {
                            "payload": _json(
                                {
                                    "action": "model.publication_published",
                                    "subject": subject,
                                    "alias": alias,
                                    "revision": body["revision"],
                                    "publication_sha256": value["publication_sha256"],
                                    "time": now,
                                }
                            )
                        },
                    )
                    await pipe.execute()
                    return value
                except WatchError:
                    continue
        raise ModelRegistryConflict("Model publication changed too often; retry")

    async def append_telemetry(
        self, tenant: str, record: dict[str, Any], *, max_records: int
    ) -> str:
        call_id = record.get("call_id")
        if not isinstance(call_id, str) or not 1 <= len(call_id) <= 128:
            raise ValueError("Model telemetry requires a bounded call ID")
        safe = {
            key: record.get(key)
            for key in (
                "call_id",
                "goal_id",
                "node_id",
                "alias",
                "route_fingerprint",
                "publication_sha256",
                "publication_revision",
                "status",
                "started_at",
                "finished_at",
                "duration_ms",
                "input_bytes",
                "reserved_input_tokens",
                "output_token_cap",
                "actual_input_tokens",
                "actual_output_tokens",
                "actual_total_tokens",
                "provider_reported",
                "estimate_calibration_sha256",
                "estimated_input_tokens",
                "estimated_output_tokens",
                "estimated_total_tokens",
                "actual_minus_estimated_input_tokens",
                "actual_minus_estimated_output_tokens",
                "actual_minus_estimated_total_tokens",
                "error_type",
            )
        }
        marker = self.runtime._tenant_key(
            tenant, f"models:call-telemetry-id:{hashlib.sha256(call_id.encode()).hexdigest()}"
        )
        for _ in range(8):
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(marker)
                    if await pipe.exists(marker):
                        return "duplicate"
                    pipe.multi()
                    pipe.xadd(
                        self.telemetry_key(tenant),
                        {"payload": _json(safe)},
                        maxlen=max_records,
                        approximate=True,
                    )
                    pipe.set(marker, "recorded", ex=2_592_000)
                    result = await pipe.execute()
                    return str(result[0])
                except WatchError:
                    continue
        raise ModelRegistryConflict("Model telemetry changed too often; retry")

    async def telemetry(
        self, tenant: str, *, after: str = "0-0", limit: int = 100
    ) -> tuple[list[dict[str, Any]], str | None]:
        rows = await self.client.xrange(
            self.telemetry_key(tenant), min=f"({after}", count=limit + 1
        )
        values = []
        for event_id, fields in rows[:limit]:
            try:
                item = json.loads(fields["payload"])
            except (KeyError, TypeError, ValueError):
                continue
            if isinstance(item, dict):
                values.append({"event_id": event_id} | item)
        return values, rows[limit - 1][0] if len(rows) > limit else None

    async def calibration(
        self,
        tenant: str,
        publication_sha256: str,
        *,
        client: Any | None = None,
        watch: bool = False,
    ) -> dict[str, Any] | None:
        reader = client or self.client
        key = self.calibration_key(tenant, publication_sha256)
        if watch:
            await reader.watch(key)
        value = _decode(await reader.get(key))
        if value is None:
            return None
        body = {key: item for key, item in value.items() if key != "calibration_sha256"}
        if (
            value.get("schema_version") != 1
            or value.get("tenant_id") != tenant
            or value.get("publication_sha256") != publication_sha256
            or type(value.get("revision")) is not int
            or value["revision"] < 1
            or value.get("calibration_sha256") != "sha256:" + canonical_hash(body)
        ):
            raise ModelRegistryIntegrityError("Stored calibration failed verification")
        return value

    async def calibrate(
        self,
        tenant: str,
        subject: str,
        publication: dict[str, Any],
        minimum_samples: int,
        expected_revision: int,
    ) -> dict[str, Any]:
        """Explicitly derive a profile from existing calls; never probes a provider."""
        current_publication = await self.get(tenant, publication["alias"])
        if (
            current_publication is None
            or current_publication["publication_sha256"] != publication["publication_sha256"]
        ):
            raise ModelRegistryConflict("Model publication changed during calibration")
        prior_profile = await self.calibration(tenant, publication["publication_sha256"])
        if (prior_profile["revision"] if prior_profile else 0) != expected_revision:
            raise ModelRegistryConflict("Calibration revision conflict")
        rows = await self.client.xrevrange(self.telemetry_key(tenant), count=10_000)
        samples = []
        seen_calls: set[str] = set()
        for _event_id, fields in rows:
            try:
                item = json.loads(fields["payload"])
            except (KeyError, TypeError, ValueError):
                continue
            if (
                item.get("status") == "completed"
                and item.get("provider_reported") is True
                and item.get("publication_sha256") == publication["publication_sha256"]
                and type(item.get("actual_input_tokens")) is int
                and item["actual_input_tokens"] > 0
                and type(item.get("actual_output_tokens")) is int
                and type(item.get("input_bytes")) is int
                and item["input_bytes"] > 0
                and isinstance(item.get("duration_ms"), (int, float))
                and item["duration_ms"] >= 0
                and isinstance(item.get("call_id"), str)
                and item["call_id"] not in seen_calls
            ):
                samples.append(item)
                seen_calls.add(item["call_id"])
        if len(samples) < minimum_samples:
            return {
                "status": "insufficient_calibration",
                "sample_count": len(samples),
                "minimum_samples": minimum_samples,
                "provider_calls_started": 0,
            }

        def quantile(values: list[float], probability: float) -> float:
            ordered = sorted(values)
            return ordered[min(len(ordered) - 1, max(0, math.ceil(probability * len(ordered)) - 1))]

        ratios = [item["actual_input_tokens"] / item["input_bytes"] for item in samples]
        outputs = [float(item["actual_output_tokens"]) for item in samples]
        latencies = [float(item["duration_ms"]) / 1_000 for item in samples]
        key = self.calibration_key(tenant, publication["publication_sha256"])
        for _ in range(8):
            async with self.client.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key, self.key(tenant, publication["alias"]))
                    current = await self.get(tenant, publication["alias"], client=pipe)
                    if (
                        not current
                        or current["publication_sha256"] != publication["publication_sha256"]
                    ):
                        raise ModelRegistryConflict("Model publication changed during calibration")
                    prior = await self.calibration(
                        tenant, publication["publication_sha256"], client=pipe
                    )
                    revision = prior["revision"] if prior else 0
                    if revision != expected_revision:
                        raise ModelRegistryConflict("Calibration revision conflict")
                    body = {
                        "schema_version": 1,
                        "tenant_id": tenant,
                        "publication_sha256": publication["publication_sha256"],
                        "publication_revision": publication["revision"],
                        "revision": revision + 1,
                        "previous_calibration_sha256": (
                            prior["calibration_sha256"] if prior else None
                        ),
                        "sample_count": len(samples),
                        "input_tokens_per_byte": {
                            "low": quantile(ratios, 0.10),
                            "base": quantile(ratios, 0.50),
                            "high": quantile(ratios, 0.90),
                        },
                        "output_tokens_per_call": {
                            "low": int(quantile(outputs, 0.10)),
                            "base": int(quantile(outputs, 0.50)),
                            "high": int(quantile(outputs, 0.90)),
                        },
                        "latency_seconds_per_call": {
                            "low": quantile(latencies, 0.10),
                            "base": quantile(latencies, 0.50),
                            "high": quantile(latencies, 0.90),
                        },
                        "created_at": datetime.now(UTC).isoformat(),
                        "created_by": subject,
                    }
                    value = body | {"calibration_sha256": "sha256:" + canonical_hash(body)}
                    history = self.calibration_history_key(
                        tenant, publication["publication_sha256"], body["revision"]
                    )
                    await pipe.watch(history)
                    if await pipe.exists(history):
                        raise ModelRegistryIntegrityError(
                            "Immutable calibration revision already exists"
                        )
                    pipe.multi()
                    pipe.set(key, _json(value))
                    pipe.set(history, _json(value))
                    pipe.xadd(
                        self.runtime._tenant_key(tenant, "audit"),
                        {
                            "payload": _json(
                                {
                                    "action": "model.calibration_published",
                                    "subject": subject,
                                    "publication_sha256": publication["publication_sha256"],
                                    "sample_count": len(samples),
                                    "revision": body["revision"],
                                    "time": body["created_at"],
                                }
                            )
                        },
                    )
                    await pipe.execute()
                    return value | {"status": "calibrated", "provider_calls_started": 0}
                except WatchError:
                    continue
        raise ModelRegistryConflict("Calibration inputs changed too often; retry")
