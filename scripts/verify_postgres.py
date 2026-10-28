"""Authenticated HTTP/Redis/PostgreSQL checks for the disposable Docker stack.

The manifest stores only IDs and synthetic test markers in the test Redis. It is
not an application endpoint, archive source, migration report or production data.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from urllib.parse import quote
from uuid import uuid4

import httpx

from app.core.config import get_settings
from app.core.durable_outbox import outbox_key
from app.core.redis_runtime import RedisRuntime

MANIFEST = "postgres-e2e:test-manifest"
ADMIN = "postgres-admin"
VIEWER = "postgres-viewer"
FOREIGN = "postgres-foreign"


def events(text: str) -> list[dict]:
    return [json.loads(line[6:]) for line in text.splitlines() if line.startswith("data: ")]


async def main() -> None:
    settings = get_settings()
    runtime = RedisRuntime(settings.redis_url)
    phase = os.getenv("POSTGRES_TEST_PHASE", "verify")
    try:
        async with httpx.AsyncClient(
            base_url="http://127.0.0.1:8000", timeout=60, headers={"x-api-key": ADMIN}
        ) as client:

            async def call(method, path, body=None, *, key=ADMIN, params=None):
                response = await client.request(
                    method, path, json=body, headers={"x-api-key": key}, params=params
                )
                response.raise_for_status()
                return response.json()

            async def agent(name, *, key=ADMIN):
                return await call(
                    "POST",
                    "/v1/agents",
                    {
                        "name": name,
                        "persona": "只整理已提供的證據，保留不確定性。",
                        "model": "mock-model",
                        "policy": {"allowed_tools": []},
                    },
                    key=key,
                )

            if phase == "prepare":
                case = uuid4().hex
                marker = "pgarchiveseed" + case
                # Use an independent suffix so trigram similarity with the public
                # marker cannot turn this negative privacy assertion into a false hit.
                private = "pgprivatepersona" + uuid4().hex
                body_marker = "pgapprovedbody" + case
                skill = await call("POST", "/v1/skills/install", {"name": "evidence-review"})
                worker = await call(
                    "POST",
                    "/v1/agents",
                    {
                        "name": marker,
                        "persona": "只整理提供的證據。 " + private,
                        "model": "mock-model",
                        "policy": {"allowed_tools": []},
                        "skills": [{"skill_id": skill["skill_id"], "revision": 1}],
                    },
                )
                session = await call(
                    "POST", f"/v1/agents/{worker['agent_id']}/planning-sessions", {}
                )
                saved = await call(
                    "POST",
                    f"/v1/planning-sessions/{session['session_id']}/plans",
                    {
                        "expected_version": session["version"],
                        "plan": {
                            "objective": "驗證持久化的計畫快照，不從歷史啟動工作。",
                            "steps": [{"query": "保留已知資訊與未知事項。"}],
                        },
                    },
                )
                await call(
                    "POST",
                    f"/v1/planning-sessions/{session['session_id']}/mode",
                    {
                        "expected_version": saved["version"],
                        "mode": "execute",
                        "plan_revision": 1,
                    },
                )
                goal = await call(
                    "POST",
                    "/v1/goals",
                    {
                        "objective": "Postgres durable archive smoke " + body_marker,
                        "model": "mock-model",
                        "steps": [{"query": "Produce a durable archive verification result."}],
                        "max_retries": 0,
                    },
                )
                for _ in range(120):
                    goal = await call("GET", f"/v1/goals/{goal['goal_id']}")
                    if goal["status"] in {"completed", "failed", "cancelled"}:
                        break
                    await asyncio.sleep(0.25)
                assert goal["status"] == "completed", goal["status"]
                conversation_id = "pg-conversation-" + case
                response = await client.post(
                    "/v1/agent/stream",
                    json={
                        "conversation_id": conversation_id,
                        "model": "mock-model",
                        "query": "Summarize the supplied archival verification evidence.",
                        "max_retries": 0,
                    },
                )
                response.raise_for_status()
                assert any(event.get("type") == "completed" for event in events(response.text))
                run_id = response.headers["x-request-id"]
                # Keep the tenant-isolation negative canary dissimilar from the
                # source tenant's marker because search intentionally supports
                # trigram matches, not exact-match-only semantics.
                foreign = await agent("pgforeign" + uuid4().hex, key=FOREIGN)
                manifest = {
                    "marker": marker,
                    "private_marker": private,
                    "body_marker": body_marker,
                    "agent_id": worker["agent_id"],
                    "skill_id": skill["skill_id"],
                    "session_id": session["session_id"],
                    "goal_id": goal["goal_id"],
                    "conversation_id": conversation_id,
                    "run_id": run_id,
                    "foreign_agent_id": foreign["agent_id"],
                }
                await runtime._client.set(MANIFEST, json.dumps(manifest))
                shadow = await client.get("/v1/history/records")
                assert shadow.status_code == 409, shadow.status_code
                print("Postgres shadow source API preparation passed")
                return

            if phase == "redis-outage":
                history = await call("GET", "/v1/history/records", key=VIEWER)
                assert history["records"] and history["execution_authority"] is False
                for method, path, body in (
                    ("GET", "/v1/agents", None),
                    ("POST", "/v1/goals", {"objective": "Must not execute from archive"}),
                ):
                    response = await client.request(method, path, json=body)
                    assert response.status_code == 503, response.text
                    assert response.json() == {"detail": "Execution requires Redis"}
                print("Real Redis outage: Postgres history readable; execution returned 503")
                return

            raw = await runtime._client.get(MANIFEST)
            assert raw is not None, "Missing disposable-stack manifest"
            manifest = json.loads(raw)
            if phase == "outage-write":
                row = await agent("pgoutage" + uuid4().hex)
                manifest["outage_agent_id"] = row["agent_id"]
                await runtime._client.set(MANIFEST, json.dumps(manifest))
                assert (await call("GET", f"/v1/agents/{row['agent_id']}"))["agent_id"] == row[
                    "agent_id"
                ]
                print("Postgres outage: Redis-authoritative API write remained available")
                return
            if phase == "outage-read":
                response = await client.get("/v1/history/records")
                assert response.status_code == 503, response.status_code
                assert response.json() == {"detail": "History storage unavailable"}
                # Existing execution-authority reads are still Redis-backed.
                assert (await call("GET", f"/v1/agents/{manifest['agent_id']}"))["agent_id"] == (
                    manifest["agent_id"]
                )
                print(
                    "Postgres outage: history failed explicitly, live Redis reads stayed available"
                )
                return
            assert phase == "verify", phase

            async def archived(kind, resource_id, *, key=VIEWER):
                payload = await call(
                    "GET", f"/v1/history/records/{kind}/{quote(resource_id, safe='')}", key=key
                )
                assert payload["history_only"] is True
                assert payload["execution_authority"] is False
                assert payload["consistency"] == "eventual"
                record = payload["record"]
                assert record["resource_id"] == resource_id
                assert record["kind"] == kind
                return record

            wanted = [
                ("agent", manifest["agent_id"]),
                ("agent", manifest["outage_agent_id"]),
                ("skill", manifest["skill_id"]),
                ("skill_revision", manifest["skill_id"] + ":1"),
                ("planning_session", manifest["session_id"]),
                ("planning_revision", manifest["session_id"] + ":1"),
                ("goal", manifest["goal_id"]),
                ("run", manifest["run_id"]),
                ("conversation", manifest["conversation_id"]),
            ]
            for kind, resource_id in wanted:
                await archived(kind, resource_id)

            # Record pagination is not interchangeable with a Redis event cursor.
            rows, cursor = [], ""
            for _ in range(100):
                page = await call(
                    "GET", "/v1/history/records", params={"after": cursor, "limit": 2}
                )
                rows.extend(page["records"])
                next_cursor = page.get("next_cursor")
                if not next_cursor:
                    break
                assert next_cursor != cursor
                cursor = next_cursor
            else:
                raise AssertionError("History record pagination did not terminate")
            identities = [(row["kind"], row["resource_id"]) for row in rows]
            assert len(identities) == len(set(identities))
            assert set(wanted).issubset(identities)
            assert manifest["foreign_agent_id"] not in json.dumps(rows)

            hits = await call("GET", "/v1/history/search", params={"q": manifest["marker"]})
            assert any(row["resource_id"] == manifest["agent_id"] for row in hits["results"])
            controlled = await call(
                "GET",
                "/v1/history/search",
                params={
                    "q": manifest["body_marker"],
                    "kind": "goal",
                    "scope": "all",
                    "include_summary": "true",
                    "expand": "relations",
                },
            )
            assert any(
                row["resource_id"] == manifest["goal_id"] for row in controlled["results"]
            ), json.dumps(controlled, ensure_ascii=False)
            assert controlled["summary"]["acl_rechecked"] is True
            goal_hit = next(
                row
                for row in controlled["results"]
                if row["resource_id"] == manifest["goal_id"]
                and row["document_type"] == "record"
            )
            assert any(
                item["kind"] == "conversation" and item["resource_id"] == manifest["goal_id"]
                for item in goal_hit["relations"]
            )
            private = await call(
                "GET", "/v1/history/search", params={"q": manifest["private_marker"]}
            )
            assert private["results"] == [], "Private persona must not become searchable metadata"
            foreign_hits = await call(
                "GET", "/v1/history/search", params={"q": manifest["marker"]}, key=FOREIGN
            )
            assert foreign_hits["results"] == []
            response = await client.get(
                f"/v1/history/records/agent/{manifest['agent_id']}",
                headers={"x-api-key": FOREIGN},
            )
            assert response.status_code == 404
            await archived("agent", manifest["foreign_agent_id"], key=FOREIGN)
            for params in (
                {"tenant_id": "postgres-foreign"},
                {"kind": "checkpoint"},
                {"sql": "SELECT 1"},
            ):
                response = await client.get("/v1/history/records", params=params)
                assert response.status_code == 422, response.status_code
            response = await client.get("/v1/history/status", headers={"x-api-key": VIEWER})
            assert response.status_code == 403
            status = await call("GET", "/v1/history/status")
            assert status["execution_authority"] is False

            if not manifest.get("m6_lifecycle_verified"):
                stream = outbox_key(settings.postgres_source_id)
                private_poison = "PRIVATE_M6_POISON_" + uuid4().hex
                raw_poison = json.dumps(
                    {
                        "schema_version": 999,
                        "key": "tenant:postgres-e2e:agent:goal:m6-invalid",
                        "secret": private_poison,
                    },
                    ensure_ascii=False,
                )
                poison_id = await runtime._client.xadd(stream, {"payload": raw_poison})
                poison = None
                for _ in range(120):
                    diagnostic = await call("GET", "/v1/history/poison")
                    poison = next(
                        (
                            item
                            for item in diagnostic["records"]
                            if item["event_id"] == poison_id
                        ),
                        None,
                    )
                    if poison:
                        break
                    await asyncio.sleep(0.1)
                assert poison and poison["pending"] is True
                assert poison["payload_sha256"] == hashlib.sha256(raw_poison.encode()).hexdigest()
                assert private_poison not in json.dumps(poison)
                repaired_id = "m6-repaired-" + uuid4().hex
                repaired = await call(
                    "POST",
                    f"/v1/history/poison/{poison_id}/repair",
                    {
                        "expected_payload_sha256": poison["payload_sha256"],
                        "replacement_envelope": {
                            "schema_version": 1,
                            "command": "SET",
                            "key": f"tenant:postgres-e2e:agent:goal:{repaired_id}",
                            "value": json.dumps(
                                {
                                    "tenant_id": "postgres-e2e",
                                    "goal_id": repaired_id,
                                    "status": "completed",
                                    "history_search_approved": True,
                                    "objective": "M6 repaired projection evidence",
                                    "steps": [],
                                }
                            ),
                        },
                    },
                )
                assert repaired["repair"]["acknowledged_after_projection"] is True
                await archived("goal", repaired_id)

                finding_id = "m6-retention-" + uuid4().hex
                finding = {
                    "schema_version": 1,
                    "command": "SET",
                    "key": f"tenant:postgres-e2e:security:threat-finding:{finding_id}",
                    "value": json.dumps(
                        {
                            "tenant_id": "postgres-e2e",
                            "finding_id": finding_id,
                            "rule_id": "M6-RETENTION",
                            "severity": "low",
                            "review_status": "reviewed",
                        }
                    ),
                }
                finding_cursor = await runtime._client.xadd(
                    stream, {"payload": json.dumps(finding)}
                )
                for _ in range(120):
                    response = await client.get(
                        f"/v1/history/records/finding/{finding_id}",
                        headers={"x-api-key": ADMIN},
                    )
                    if response.status_code == 200:
                        break
                    await asyncio.sleep(0.1)
                assert response.status_code == 200
                preview = await call(
                    "POST",
                    "/v1/history/purge",
                    {"kind": "finding", "before_cursor": finding_cursor},
                )
                assert preview["purge"]["dry_run"] is True
                assert preview["purge"]["records"] == 1
                assert (
                    await client.get(
                        f"/v1/history/records/finding/{finding_id}",
                        headers={"x-api-key": ADMIN},
                    )
                ).status_code == 200
                executed = await call(
                    "POST",
                    "/v1/history/purge",
                    {
                        "kind": "finding",
                        "before_cursor": finding_cursor,
                        "execute": True,
                        "expected_preview_sha256": preview["purge"]["preview_sha256"],
                    },
                )
                assert executed["purge"]["executed"] is True
                before_replay = (await call("GET", "/v1/history/status"))["status"][
                    "applied_operations"
                ]
                replay = finding | {"backfill": True}
                await runtime._client.xadd(stream, {"payload": json.dumps(replay)})
                for _ in range(120):
                    replay_status = await call("GET", "/v1/history/status")
                    if replay_status["status"]["applied_operations"] > before_replay:
                        break
                    await asyncio.sleep(0.1)
                assert replay_status["status"]["applied_operations"] > before_replay
                assert (
                    await client.get(
                        f"/v1/history/records/finding/{finding_id}",
                        headers={"x-api-key": ADMIN},
                    )
                ).status_code == 404
                for _ in range(120):
                    audit_search = await call(
                        "GET",
                        "/v1/history/search",
                        params={
                            "q": "postgres.poison_repaired",
                            "kind": "audit",
                            "scope": "events",
                        },
                    )
                    if audit_search["results"]:
                        break
                    await asyncio.sleep(0.1)
                assert audit_search["results"]
                manifest["m6_lifecycle_verified"] = True
                manifest["repaired_goal_id"] = repaired_id
                await runtime._client.set(MANIFEST, json.dumps(manifest))
                status = await call("GET", "/v1/history/status")
                assert status["status"]["retention_operations"] >= 1

            for kind, resource_id in (
                ("run", manifest["run_id"]),
                ("goal", manifest["goal_id"]),
                ("planning", manifest["session_id"]),
                ("audit", "audit"),
            ):
                payload = await call(
                    "GET",
                    "/v1/history/events",
                    params={"kind": kind, "resource_id": resource_id},
                )
                assert payload["events"], f"Missing archived {kind} events"
                sequences = [row["cursor"] for row in payload["events"]]
                assert sequences == sorted(set(sequences))
            print(
                json.dumps(
                    {
                        "status": "passed",
                        "postgres_cutover": True,
                        "archived_resource_kinds": sorted({kind for kind, _ in wanted}),
                        "tenant_isolation": True,
                        "controlled_record_event_search": True,
                        "retention_dry_run_and_tombstone": True,
                        "poison_repair_after_projection": True,
                        "paginated_records": True,
                        "archival_events": True,
                        "outage_write_recovered": True,
                    }
                )
            )
    finally:
        await runtime.close()


asyncio.run(main())
