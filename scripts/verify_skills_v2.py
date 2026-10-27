"""Synthetic real HTTP/worker checks; no real model inference or external tools."""

import asyncio
import json

import httpx

from app.core.config import get_settings
from app.core.redis_runtime import RedisRuntime


async def main():
    runtime = RedisRuntime(get_settings().redis_url)
    try:
        async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=60) as client:

            async def call(method, path, body=None, user="admin", status=200):
                response = await client.request(
                    method, path, json=body, headers={"X-API-Key": "acl-" + user}
                )
                assert response.status_code == status, (method, path, response.status_code)
                return response.json()

            forecast = await call(
                "POST",
                "/v1/skills",
                {
                    "schema_version": 2,
                    "name": "plan-forecast",
                    "execution_modes": ["plan"],
                    "display_name": "規劃專用綁定驗證",
                    "description": "尚不提供 estimator",
                    "instructions": "只在規劃模式保留未知，不啟動計畫。",
                },
            )
            resource = await call(
                "POST",
                "/v1/skills",
                {
                    "schema_version": 2,
                    "name": "resource-review",
                    "display_name": "資源驗證",
                    "description": "唯讀內容進入實際模型 context",
                    "instructions": "只整理已提供證據。",
                    "resources": [
                        {"path": "references/evidence.md", "content": "READONLY-RESOURCE-CANARY"}
                    ],
                },
            )
            binding = {"skill_id": forecast["skill_id"], "revision": 1}
            config = {
                "name": "M3 驗證員",
                "persona": "skills-v2-resource-required: enabled",
                "model": "mock-model",
                "policy": {"allowed_tools": []},
                "planning_skills": [binding],
                "skill_selection_rules": [
                    {
                        "skill_id": resource["skill_id"],
                        "revision": 1,
                        "field": "objective",
                        "values": ["evidence"],
                        "priority": 10,
                    }
                ],
            }
            agent = await call("POST", "/v1/agents", config)
            aid = agent["agent_id"]
            await call("POST", "/v1/agents", config | {"skills": [binding]}, status=422)
            await call(
                "POST",
                "/v1/agents",
                config
                | {
                    "skill_selection_rules": [
                        binding | {"field": "objective", "values": ["forecast"]}
                    ]
                },
                status=422,
            )
            await call(
                "PUT",
                f"/v1/access/agent/{aid}",
                {
                    "expected_version": 1,
                    "grants": [{"subject": "acl-bob", "permission": "operate"}],
                },
            )
            before = await runtime._client.xlen("agent:goal:queue")
            metrics_before = (await client.get("/metrics")).text
            def model_metrics(text):
                return [line for line in text.splitlines() if line.startswith("ai_gateway_model_")]
            session = await call("POST", f"/v1/agents/{aid}/planning-sessions", {}, "bob")
            sid = session["session_id"]
            assert (
                session["snapshot"]["root_agent"]["planning_skill_snapshots"][0]["name"]
                == "plan-forecast"
            )
            await call(
                "POST",
                f"/v1/planning-sessions/{sid}/plans",
                {
                    "expected_version": 1,
                    "plan": {
                        "objective": "review evidence",
                        "steps": [{"query": "review evidence"}],
                    },
                },
                "bob",
            )
            revision = await call("GET", f"/v1/planning-sessions/{sid}/plans/1", user="bob")
            assert (
                revision["skill_selection"]["agents"][0]["bindings"][0]["skill_id"]
                == resource["skill_id"]
            )
            assert before == await runtime._client.xlen("agent:goal:queue")
            assert model_metrics(metrics_before) == model_metrics(
                (await client.get("/metrics")).text
            )
            goal = await call(
                "POST",
                f"/v1/agents/{aid}/goals",
                {
                    "objective": "review evidence",
                    "steps": [{"query": "Review supplied evidence only."}],
                },
                "bob",
            )
            gid = goal["goal_id"]
            assert goal["agent_config"]["skills"] == [
                {"skill_id": resource["skill_id"], "revision": 1}
            ]
            assert "planning_skills" not in goal["agent_config"]
            assert "plan-forecast" not in json.dumps(goal["agent_config"])
            assert goal["agent_config"]["skill_selection"]["selected"][0]["source"] == "rule"
            for _ in range(240):
                goal = await call("GET", f"/v1/goals/{gid}", user="bob")
                if goal["status"] in {"completed", "failed", "cancelled"}:
                    break
                await asyncio.sleep(0.25)
            assert goal["status"] == "completed", goal.get("error_type")
            assert goal["usage"]["model_calls"] > 0
            await call(
                "POST",
                f"/v1/skills/{resource['skill_id']}/enabled",
                {"expected_version": 1, "enabled": False},
            )
            await call(
                "POST", f"/v1/agents/{aid}/goals", {"objective": "review evidence"}, "bob", 409
            )
    finally:
        await runtime.close()


asyncio.run(main())
