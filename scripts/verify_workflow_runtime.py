"""Isolated real HTTP/Redis/PG orchestration, using only synthetic model traffic."""

import asyncio
import json
import os
from uuid import uuid4

import httpx

from app.core.config import get_settings
from app.core.goal_protocol import WORKFLOW_QUEUE
from app.core.redis_runtime import RedisRuntime

MANIFEST = "workflow-e2e:manifest"
TENANT = "acl-e2e"


async def main():
    runtime = RedisRuntime(get_settings().redis_url)
    try:
        async with (
            httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=60) as api,
            httpx.AsyncClient(base_url="http://legacy-gateway:8000", timeout=60) as old,
            httpx.AsyncClient(base_url="http://mock-llm:8080", timeout=60) as model,
        ):

            async def call(method, path, body=None, *, who="bob", client=api, status=200):
                response = await client.request(
                    method, path, json=body, headers={"x-api-key": "acl-" + who}
                )
                assert response.status_code == status, (
                    method,
                    path,
                    response.status_code,
                    response.text[:200],
                )
                return response.json()

            async def wait_goal(goal_id, status="completed"):
                for _ in range(360):
                    value = await call("GET", f"/v1/goals/{goal_id}")
                    if value["status"] in {"completed", "failed", "cancelled"}:
                        if value.get("workflow", {}).get("phase") not in {
                            None,
                            "finished",
                            "quarantined",
                        }:
                            await asyncio.sleep(0.25)
                            continue
                        assert value["status"] == status, (
                            value["status"],
                            value.get("error"),
                            value.get("workflow", {}).get("states"),
                        )
                        return value
                    await asyncio.sleep(0.25)
                raise AssertionError("Workflow did not reach its expected terminal state")

            async def stats(case):
                return (await model.get(f"/test/workflow/{case}")).json()

            async def workflow(*, parallel=False, hold=False, dependent=False):
                case = uuid4().hex
                agents = []
                for index in range(2 if parallel else 1):
                    agent = await call(
                        "POST",
                        "/v1/agents",
                        {
                            "name": f"Workflow reviewer {index}",
                            "persona": "只根據已提供資訊回答，保留未知。",
                            "model": "mock-model",
                            "policy": {"allowed_tools": []},
                        },
                        who="admin",
                    )
                    await call(
                        "PUT",
                        f"/v1/access/agent/{agent['agent_id']}",
                        {
                            "expected_version": 1,
                            "grants": [{"subject": "acl-bob", "permission": "operate"}],
                        },
                        who="admin",
                    )
                    agents.append(agent)
                chief = agents[0]
                if parallel:
                    chief = await call(
                        "POST",
                        "/v1/agents",
                        {
                            "name": "Workflow Chief",
                            "persona": "僅彙整子任務證據，不重新委派。",
                            "role": "chief",
                            "delegate_agent_ids": [a["agent_id"] for a in agents],
                            "model": "mock-model",
                            "policy": {"allowed_tools": []},
                        },
                        who="admin",
                    )
                    await call(
                        "PUT",
                        f"/v1/access/agent/{chief['agent_id']}",
                        {
                            "expected_version": 1,
                            "grants": [{"subject": "acl-bob", "permission": "operate"}],
                        },
                        who="admin",
                    )
                session = await call(
                    "POST", f"/v1/agents/{chief['agent_id']}/planning-sessions", {}
                )
                sid = session["session_id"]
                nodes = [
                    {
                        "node_id": f"n{index}",
                        "agent_id": agent["agent_id"],
                        "query": "分類已提供的證據",
                        "token_budget": 6000 if parallel else 12000,
                        "output_schema": {
                            "type": "object",
                            "required": ["ready"],
                            "properties": {"ready": {"type": "boolean"}},
                        },
                    }
                    for index, agent in enumerate(agents)
                ]
                edges = []
                if parallel:
                    nodes.append(
                        {
                            "node_id": "summary",
                            "agent_id": chief["agent_id"],
                            "kind": "synthesis",
                            "query": "彙整已驗證的子任務結果",
                            "token_budget": 8000,
                        }
                    )
                    edges = [{"source": f"n{i}", "target": "summary"} for i in range(2)]
                elif dependent:
                    nodes.append(
                        {
                            "node_id": "later",
                            "agent_id": chief["agent_id"],
                            "query": "後續彙整",
                            "token_budget": 8000,
                        }
                    )
                    edges = [{"source": "n0", "target": "later"}]
                plan = {
                    "schema_version": 2,
                    "objective": f"workflow-e2e:{case}" + (" workflow-e2e-hold" if hold else ""),
                    "nodes": nodes,
                    "edges": edges,
                }
                saved = await call(
                    "POST",
                    f"/v1/planning-sessions/{sid}/plans",
                    {"expected_version": 1, "plan": plan},
                )
                compiled = await call("GET", f"/v1/planning-sessions/{sid}/compiled")
                assert (
                    compiled["execution_authority"] is False
                    and compiled["execution_started"] is False
                )
                accepted = await call(
                    "POST",
                    f"/v1/planning-sessions/{sid}/mode",
                    {"expected_version": saved["version"], "mode": "execute", "plan_revision": 1},
                )
                assert (await stats(case)).get("calls", 0) == 0
                payload = {
                    "expected_version": accepted["version"],
                    "accepted_hash": accepted["accepted_hash"],
                    "idempotency_key": case,
                }
                first, retry = await asyncio.gather(
                    *(
                        call("POST", f"/v1/planning-sessions/{sid}/execute", payload)
                        for _ in range(2)
                    )
                )
                assert first["goal"]["goal_id"] == retry["goal"]["goal_id"]
                root = first["goal"]
                assert root["workflow_protocol"] == 1 and root["workflow"]["acl_enforced"] is True
                return {
                    "case": case,
                    "root_id": root["goal_id"],
                    "agents": [a["agent_id"] for a in agents],
                }

            async def active_model(manifest):
                for _ in range(240):
                    if (await stats(manifest["case"])).get("active", 0):
                        return
                    await asyncio.sleep(0.1)
                raise AssertionError("Expected model request did not become active")

            phase = os.environ.get("WORKFLOW_TEST_PHASE", "prepare")
            manifest = json.loads(await runtime._client.get(MANIFEST) or "{}")
            if phase == "prepare":
                # Actual old worker traffic proves the legacy process is alive.
                legacy = await call(
                    "POST",
                    "/v1/goals",
                    {
                        "objective": "Produce a report from supplied context",
                        "model": "mock-model",
                        "steps": [{"query": "Review the supplied context and retain uncertainty"}],
                    },
                    client=old,
                    who="admin",
                )
                for _ in range(180):
                    legacy = await call(
                        "GET", f"/v1/goals/{legacy['goal_id']}", client=old, who="admin"
                    )
                    if legacy["status"] in {"completed", "failed", "cancelled"}:
                        break
                    await asyncio.sleep(0.25)
                assert legacy["status"] == "completed", legacy.get("error")
                manifest["parallel"] = await workflow(parallel=True)
                root_id = manifest["parallel"]["root_id"]
                for _ in range(80):
                    root = await call("GET", f"/v1/goals/{root_id}")
                    if len(root["workflow"]["states"]) >= 2:
                        break
                    await asyncio.sleep(0.25)
                assert root["status"] == "waiting_nodes"
                assert (await stats(manifest["parallel"]["case"])).get("calls", 0) == 0
                await call("GET", f"/v1/goals/{root_id}", client=old, status=404)
                await call("POST", f"/v1/goals/{root_id}/cancel", {}, client=old, status=404)
                for state in root["workflow"]["states"].values():
                    await call("GET", f"/v1/goals/{state['goal_id']}", client=old, status=404)
                assert not await runtime._client.get(f"tenant:{TENANT}:agent:goal:{root_id}")
                await runtime._client.set(MANIFEST, json.dumps(manifest))
                print(
                    "Old worker completed legacy traffic; "
                    "queued Workflow v1 remains undiscoverable/uncontrollable"
                )
            elif phase == "parallel":
                case = manifest["parallel"]
                value = await wait_goal(case["root_id"])
                assert (await stats(case["case"]))["max_active"] >= 2
                assert value["usage"]["policy_charged_tokens"] == 54
                assert value["token_reservation"]["reconciled"]
                assert len(value["workflow"]["states"]) == 3
                manifest["kill"] = await workflow(hold=True)
                await active_model(manifest["kill"])
                root = await call("GET", f"/v1/goals/{manifest['kill']['root_id']}")
                child_id = root["workflow"]["states"]["n0"]["goal_id"]
                manifest["kill"]["child_id"] = child_id
                # The mock provider is the authoritative signal that at least
                # one current worker is mid-call. Killing this disposable
                # stack's two current workers avoids a racy cross-system
                # attempt to map the Redis pending list back to one container.
                await runtime._client.set("workflow-e2e:kill-consumer", "all-current-workers")
                await runtime._client.set(MANIFEST, json.dumps(manifest))
                print("Parallel DAG/typed join passed; active current workers selected for SIGKILL")
            elif phase == "recover":
                case = manifest["kill"]
                value = await wait_goal(case["root_id"])
                child = await call("GET", f"/v1/goals/{case['child_id']}")
                assert child["execution_fence"] >= 2 and child["usage"]["model_calls"] >= 2
                assert 18 < value["usage"]["policy_charged_tokens"] <= value["token_budget"]
                before = (await stats(case["case"]))["calls"]
                duplicate = await runtime._client.xadd(
                    WORKFLOW_QUEUE, {"tenant_id": TENANT, "goal_id": case["child_id"]}
                )
                for _ in range(100):
                    groups = await runtime._client.xinfo_groups(WORKFLOW_QUEUE)
                    cursor = next(
                        g["last-delivered-id"] for g in groups if g["name"] == "goal-workers"
                    )
                    delivered = tuple(map(int, cursor.split("-"))) >= tuple(
                        map(int, duplicate.split("-"))
                    )
                    pending = await runtime._client.xpending_range(
                        WORKFLOW_QUEUE, "goal-workers", duplicate, duplicate, 1
                    )
                    if delivered and not pending:
                        break
                    await asyncio.sleep(0.2)
                assert delivered and not pending
                assert (await stats(case["case"]))["calls"] == before
                manifest["revoke"] = await workflow(hold=True, dependent=True)
                await active_model(manifest["revoke"])
                await call(
                    "PUT",
                    f"/v1/access/agent/{manifest['revoke']['agents'][0]}",
                    {"expected_version": 2, "grants": []},
                    who="admin",
                )
                failed = await wait_goal(manifest["revoke"]["root_id"], "failed")
                assert (
                    "later" not in failed["workflow"]["states"]
                    or failed["workflow"]["states"]["later"]["status"] != "queued"
                )
                assert (await stats(manifest["revoke"]["case"]))["calls"] == 1
                assert failed["token_reservation"]["reconciled"]
                await runtime._client.set(MANIFEST, json.dumps(manifest))
                print(
                    "SIGKILL/fenced recovery, conservative billing, duplicate delivery "
                    "and strict-posture revocation passed"
                )
            elif phase == "history":
                for case in manifest.values():
                    value = await call("GET", f"/v1/history/records/goal/{case['root_id']}")
                    assert value["execution_authority"] is False
                    assert value["record"]["data"]["workflow_protocol"] == 1
                print(
                    "Versioned Workflow Goals projected/reconciled into read-only Postgres history"
                )
            else:
                raise AssertionError("Unknown test phase")
    finally:
        await runtime.close()


asyncio.run(main())
