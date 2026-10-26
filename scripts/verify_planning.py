"""HTTP/Redis planning smoke test; used only by the disposable Docker test stack."""

import asyncio
import json

import httpx

from app.core.config import get_settings
from app.core.redis_runtime import RedisRuntime


async def main():
    settings = get_settings()
    runtime = RedisRuntime(settings.redis_url)
    try:
        async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=30) as client:

            async def call(method, path, body=None):
                response = await client.request(method, path, json=body)
                response.raise_for_status()
                return response.json()

            skill = await call("POST", "/v1/skills/install", {"name": "evidence-review"})
            worker = await call(
                "POST",
                "/v1/agents",
                {
                    "name": "規劃快照驗證員",
                    "persona": "只整理提供的證據，保留未知事項。",
                    "model": settings.llm_model,
                    "policy": {"allowed_tools": []},
                    "skills": [{"skill_id": skill["skill_id"], "revision": skill["revision"]}],
                },
            )
            chief = await call(
                "POST",
                "/v1/agents",
                {
                    "name": "規劃主任",
                    "persona": "使用明確分工，不開始執行。",
                    "role": "chief",
                    "model": settings.llm_model,
                    "delegate_agent_ids": [worker["agent_id"]],
                },
            )
            queue_before = await runtime._client.xlen("agent:goal:queue")
            events_before = await runtime._client.xlen("automation:internal-events")
            goals_before = await runtime.list_goal_ids("default")
            metrics_before = (await client.get("/metrics")).text
            model_metrics_before = [
                line for line in metrics_before.splitlines() if line.startswith("ai_gateway_model_")
            ]
            session = await call("POST", f"/v1/agents/{chief['agent_id']}/planning-sessions", {})
            path = f"/v1/planning-sessions/{session['session_id']}"
            draft = {
                "objective": "核對提供的證據，不執行外部操作。",
                "assignments": [
                    {
                        "agent_id": worker["agent_id"],
                        "objective": "整理已知與未知。",
                        "steps": [{"query": "保留來源與不確定性。"}],
                    }
                ],
            }
            saved = await call("POST", path + "/plans", {"expected_version": 1, "plan": draft})
            original = await call("GET", path + "/plans/1")
            assert original["complete"] is True
            assert original["snapshot"]["root_agent"]["skill_snapshots"] == []
            assert (
                original["snapshot"]["workers"][0]["skill_snapshots"][0]["skill_id"]
                == skill["skill_id"]
            )
            accepted = await call(
                "POST",
                path + "/mode",
                {"expected_version": saved["version"], "mode": "execute", "plan_revision": 1},
            )
            assert accepted["accepted_hash"] == original["content_sha256"]
            assert accepted["execution_started"] is False
            denied = await client.post(
                path + "/plans", json={"expected_version": accepted["version"], "plan": draft}
            )
            assert denied.status_code == 409
            editable = await call(
                "POST", path + "/mode", {"expected_version": accepted["version"], "mode": "plan"}
            )
            await call(
                "PUT",
                f"/v1/agents/{worker['agent_id']}",
                {
                    "expected_version": worker["version"],
                    "name": worker["name"],
                    "persona": "用新設定整理提供的證據。",
                    "model": settings.llm_model,
                    "policy": worker["policy"],
                    "skills": worker["skills"],
                },
            )
            assert (await call("GET", path))["configuration_status"] == "stale"
            refreshed = await call(
                "POST", path + "/refresh", {"expected_version": editable["version"]}
            )
            assert refreshed["configuration_status"] == "current"
            assert refreshed["plan_revision"] == 2
            assert await call("GET", path + "/plans/1") == original
            assert (await call("GET", path + "/plans/2"))["snapshot"] != original["snapshot"]
            assert await runtime._client.xlen("agent:goal:queue") == queue_before
            assert await runtime.list_goal_ids("default") == goals_before
            assert await runtime._client.xlen("automation:internal-events") == events_before
            model_metrics_after = [
                line
                for line in (await client.get("/metrics")).text.splitlines()
                if line.startswith("ai_gateway_model_")
            ]
            assert model_metrics_after == model_metrics_before
            audit = await runtime._client.xrange(runtime._tenant_key("default", "audit"))
            for _, fields in audit:
                entry = json.loads(fields["payload"])
                if entry.get("session_id") == session["session_id"]:
                    assert (
                        "persona" not in fields["payload"]
                        and "instructions" not in fields["payload"]
                    )
            print("Planning HTTP/Redis smoke passed: snapshots, modes, refresh, zero execution.")
    finally:
        await runtime.close()


asyncio.run(main())
