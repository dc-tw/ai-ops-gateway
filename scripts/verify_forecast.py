"""M5 Docker E2E: real HTTP/Redis/worker, synthetic provider, no external writes."""

import asyncio

import httpx


async def main():
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=60) as client:

        async def call(method, path, body=None, user="admin", status=200):
            response = await client.request(
                method, path, json=body, headers={"X-API-Key": "acl-" + user}
            )
            assert response.status_code == status, (
                method,
                path,
                response.status_code,
                response.text,
            )
            return response.json()

        publication = await call(
            "POST",
            "/v1/models/publications",
            {
                "alias": "mock-model",
                "expected_revision": 0,
                "actual_model_version": "synthetic-weights-v1",
                "deployment_id": "docker-mock",
                "tokenizer_version": "synthetic-tokenizer-v1",
                "runtime_version": "synthetic-runtime-v1",
            },
        )
        await call(
            "POST",
            "/v1/models/publications",
            {"alias": "mock-model", "expected_revision": 0},
            status=409,
        )
        forecast_skill = await call(
            "POST",
            "/v1/skills",
            {
                "schema_version": 2,
                "name": "plan-forecast",
                "execution_modes": ["plan"],
                "display_name": "計畫耗時與 token 預測",
                "description": "只讀取固定計畫與既有校正資料。",
                "instructions": "校正不足時保留未知，不啟動模型或工具。",
            },
        )
        agent = await call(
            "POST",
            "/v1/agents",
            {
                "name": "M5 預測驗證員",
                "persona": "只依提供的證據回覆，明確標示未知。",
                "model": "mock-model",
                "policy": {"allowed_tools": []},
                "planning_skills": [{"skill_id": forecast_skill["skill_id"], "revision": 1}],
            },
        )
        aid = agent["agent_id"]
        await call(
            "PUT",
            f"/v1/access/agent/{aid}",
            {
                "expected_version": 1,
                "grants": [{"subject": "acl-bob", "permission": "operate"}],
            },
        )
        session = await call("POST", f"/v1/agents/{aid}/planning-sessions", {}, "bob")
        assert (
            session["snapshot"]["root_agent"]["model_route"]["publication"]["publication_sha256"]
            == publication["publication_sha256"]
        )
        sid = session["session_id"]
        saved = await call(
            "POST",
            f"/v1/planning-sessions/{sid}/plans",
            {
                "expected_version": session["version"],
                "plan": {
                    "schema_version": 2,
                    "objective": "整理事件證據",
                    "nodes": [
                        {
                            "node_id": "review",
                            "agent_id": aid,
                            "query": "整理現有證據與未知事項",
                        }
                    ],
                },
            },
            "bob",
        )
        forecast_body = {"expected_version": saved["version"], "plan_revision": 1}
        first = await call("POST", f"/v1/planning-sessions/{sid}/forecasts", forecast_body, "bob")
        assert first["status"] == "insufficient_calibration"
        assert first["provider_calls_started"] == 0

        goal_ids = []
        for index in range(5):
            goal = await call(
                "POST",
                f"/v1/agents/{aid}/goals",
                {
                    "objective": f"M5 synthetic calibration {index}",
                    "steps": [{"query": "Produce a concise evidence report."}],
                },
                "bob",
            )
            goal_ids.append(goal["goal_id"])
        for _ in range(180):
            goals = [await call("GET", f"/v1/goals/{goal_id}", user="bob") for goal_id in goal_ids]
            if all(goal["status"] in {"completed", "failed"} for goal in goals):
                break
            await asyncio.sleep(0.25)
        assert all(goal["status"] == "completed" for goal in goals), goals

        telemetry = await call("GET", "/v1/models/telemetry?limit=100")
        exact = [
            item
            for item in telemetry["records"]
            if item["publication_sha256"] == publication["publication_sha256"]
            and item["provider_reported"] is True
            and item["status"] == "completed"
        ]
        assert len(exact) >= 5
        assert all("prompt" not in item and "output" not in item for item in exact)
        calibrated = await call(
            "POST",
            "/v1/models/calibrations",
            {
                "alias": "mock-model",
                "publication_revision": 1,
                "expected_revision": 0,
            },
        )
        assert calibrated["status"] == "calibrated"
        assert calibrated["provider_calls_started"] == 0
        assert calibrated["revision"] == 1
        await call(
            "POST",
            "/v1/models/calibrations",
            {
                "alias": "mock-model",
                "publication_revision": 1,
                "expected_revision": 0,
            },
            status=409,
        )

        post_calibration = await call(
            "POST",
            f"/v1/agents/{aid}/goals",
            {
                "objective": "M5 post-calibration delta",
                "steps": [{"query": "Produce another concise evidence report."}],
            },
            "bob",
        )
        for _ in range(120):
            post_goal = await call("GET", f"/v1/goals/{post_calibration['goal_id']}", user="bob")
            if post_goal["status"] in {"completed", "failed"}:
                break
            await asyncio.sleep(0.25)
        assert post_goal["status"] == "completed", post_goal
        after_calibration_records = (await call("GET", "/v1/models/telemetry?limit=100"))["records"]
        enriched = [
            item
            for item in after_calibration_records
            if item["goal_id"] == post_calibration["goal_id"]
        ]
        assert (
            enriched
            and enriched[-1]["estimate_calibration_sha256"] == calibrated["calibration_sha256"]
        )
        assert type(enriched[-1]["actual_minus_estimated_total_tokens"]) is int

        telemetry_before = len((await call("GET", "/v1/models/telemetry?limit=100"))["records"])
        estimated = await call(
            "POST", f"/v1/planning-sessions/{sid}/forecasts", forecast_body, "bob"
        )
        repeated = await call(
            "POST", f"/v1/planning-sessions/{sid}/forecasts", forecast_body, "bob"
        )
        telemetry_after = len((await call("GET", "/v1/models/telemetry?limit=100"))["records"])
        assert estimated["status"] == "estimated"
        assert estimated["scenarios"]["base"]["token_estimate"] > 0
        assert estimated["scenarios"]["base"]["model_only_makespan_seconds"] > 0
        assert estimated["waits"]["queue"]["status"] == "unknown"
        assert repeated["deduplicated"] is True
        assert repeated["forecast_sha256"] == estimated["forecast_sha256"]
        assert telemetry_before == telemetry_after

        await call(
            "POST",
            "/v1/models/publications",
            {
                "alias": "mock-model",
                "expected_revision": 1,
                "actual_model_version": "synthetic-weights-v2",
            },
        )
        await call(
            "POST",
            f"/v1/planning-sessions/{sid}/forecasts",
            forecast_body,
            "bob",
            409,
        )
        print(
            {
                "provider_reported_calls": len(exact) + len(enriched),
                "first_status": first["status"],
                "estimated_status": estimated["status"],
                "deduplicated": repeated["deduplicated"],
                "publication_stale_rejected": True,
            }
        )


asyncio.run(main())
