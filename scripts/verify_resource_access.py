"""Synthetic authenticated HTTP checks inside scripts/test_acl_stack.sh only."""

import asyncio
import json
import os

import httpx

from app.core.config import get_settings
from app.core.redis_runtime import RedisRuntime


async def main():
    runtime = RedisRuntime(get_settings().redis_url)
    key = "tenant:acl-e2e:test:ids"
    try:
        async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=60) as client:

            async def call(method, path, body=None, user="admin", expected=200):
                response = await client.request(
                    method, path, json=body, headers={"X-API-Key": "acl-" + user}
                )
                assert response.status_code == expected, (method, path, response.status_code)
                return (
                    response.json()
                    if response.headers.get("content-type", "").startswith("application/json")
                    else response.text
                )

            async def share(kind, resource_id, grants, user="admin", version=1):
                return await call(
                    "PUT",
                    f"/v1/access/{kind}/{resource_id}",
                    {"expected_version": version, "grants": grants},
                    user,
                )

            phase = os.environ["ACL_TEST_PHASE"]
            if phase == "redis-outage":
                for user in ("bob", "admin"):
                    await call("GET", "/v1/history/records?kind=goal", user=user, expected=503)
                return
            if phase == "prepare":
                skill = await call("POST", "/v1/skills/install", {"name": "evidence-review"})
                agent = await call(
                    "POST",
                    "/v1/agents",
                    {
                        "name": "ACL 隔離測試",
                        "persona": "只整理證據。",
                        "model": "mock-model",
                        "policy": {"allowed_tools": []},
                        "skills": [{"skill_id": skill["skill_id"], "revision": 1}],
                    },
                )
                aid, sid = agent["agent_id"], skill["skill_id"]
                assert (await call("GET", "/v1/agents", user="bob"))["agents"] == []
                await call("GET", f"/v1/agents/{aid}", user="bob", expected=404)
                await call("GET", f"/v1/skills/{sid}/export", user="bob", expected=404)
                await call(
                    "POST", f"/v1/agents/{aid}/goals", {"objective": "bounded proof"}, "bob", 404
                )
                await share("agent", aid, [{"subject": "acl-bob", "permission": "operate"}])
                session = await call("POST", f"/v1/agents/{aid}/planning-sessions", {}, "bob")
                plan_id = session["session_id"]
                await call("GET", f"/v1/planning-sessions/{plan_id}", user="carol", expected=404)
                goal = await call(
                    "POST", f"/v1/agents/{aid}/goals", {"objective": "bounded proof"}, "bob"
                )
                hidden = await call(
                    "POST", f"/v1/agents/{aid}/goals", {"objective": "hidden proof"}
                )
                gid = goal["goal_id"]
                await call("GET", f"/v1/goals/{gid}", user="carol", expected=404)
                await call("GET", f"/v1/goals/{gid}", user="foreign", expected=404)
                await share("goal", gid, [{"subject": "acl-carol", "permission": "read"}], "bob")
                await call("GET", f"/v1/goals/{gid}", user="carol")
                await call("POST", f"/v1/goals/{gid}/cancel", {}, "carol", 404)
                for goal_id in (gid, hidden["goal_id"]):
                    for _ in range(240):
                        current = await call("GET", f"/v1/goals/{goal_id}")
                        if current["status"] in {"completed", "failed", "cancelled"}:
                            break
                        await asyncio.sleep(0.25)
                    assert current["status"] == "completed", current["status"]
                await runtime._client.set(
                    key, json.dumps({"goal_id": gid, "hidden_id": hidden["goal_id"]})
                )
            elif phase == "verify":
                ids = json.loads(await runtime._client.get(key))
                gid = ids["goal_id"]
                path = f"/v1/history/records/goal/{gid}"
                await call("GET", path, user="carol")
                rows = (
                    await call(
                        "GET", "/v1/history/search?q=completed&kind=goal&limit=1", user="bob"
                    )
                )["results"]
                assert len(rows) == 1 and rows[0]["resource_id"] == gid
                await call(
                    "GET", f"/v1/history/records/goal/{ids['hidden_id']}", user="bob", expected=404
                )
                await share("goal", gid, [], "bob", version=2)
                await call("GET", path, user="carol", expected=404)
                assert (await call("GET", "/v1/history/records?kind=goal", user="carol"))[
                    "records"
                ] == []
                assert (await call("GET", "/v1/history/events?kind=goal", user="carol"))[
                    "events"
                ] == []
            else:
                raise AssertionError("Unknown test phase")
    finally:
        await runtime.close()


asyncio.run(main())
