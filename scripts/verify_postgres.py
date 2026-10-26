"""Authenticated HTTP/Redis/PostgreSQL checks for the disposable Docker stack.

The manifest stores only IDs and synthetic test markers in the test Redis. It is
not an application endpoint, archive source, migration report or production data.
"""

from __future__ import annotations

import asyncio
import json
import os
from urllib.parse import quote
from uuid import uuid4

import httpx

from app.core.config import get_settings
from app.core.redis_runtime import RedisRuntime

MANIFEST = "postgres-e2e:test-manifest"
ADMIN = "postgres-admin"
VIEWER = "postgres-viewer"
FOREIGN = "postgres-foreign"


def events(text: str) -> list[dict]:
    return [json.loads(line[6:]) for line in text.splitlines() if line.startswith("data: ")]


async def main() -> None:
    runtime = RedisRuntime(get_settings().redis_url)
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
                marker, private = "pgarchiveseed" + case, "pgprivatepersona" + case
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
                        "objective": "Postgres durable archive smoke",
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
                foreign = await agent("pgforeign" + case, key=FOREIGN)
                manifest = {
                    "marker": marker,
                    "private_marker": private,
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
                        "metadata_only_fts": True,
                        "paginated_records": True,
                        "archival_events": True,
                        "outage_write_recovered": True,
                    }
                )
            )
    finally:
        await runtime.close()


asyncio.run(main())
