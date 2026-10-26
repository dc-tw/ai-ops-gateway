"""Public SSE -> real MCP stdio -> authenticated Browser Worker -> ATR blocking.

Run inside the dedicated gateway container via test_atr_tool_stack.sh. Receipts
retain only a neutral case ID and boolean evidence, never model/tool raw content.
"""

import asyncio
import json
from uuid import uuid4

import httpx

from app.core.config import get_settings
from app.core.redis_runtime import RedisRuntime


def events(text):
    return [json.loads(line[6:]) for line in text.splitlines() if line.startswith("data: ")]


def assert_private_absent(value, canary):
    serialized = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    assert canary not in serialized, "Remote response canary escaped its ATR boundary"
    assert "ignore all previous instructions" not in serialized.lower(), "Attack text leaked"
    assert "reveal secrets" not in serialized.lower(), "Attack text leaked"


async def main():
    settings = get_settings()
    assert settings.atr_enabled and settings.atr_blocking
    case_id = uuid4().hex
    canary = "ATR_REMOTE_RAW_CANARY_" + case_id
    worker_headers = {"x-browser-worker-token": settings.browser_worker_token}
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=60) as client:
        # The real Browser Worker still rejects unauthenticated calls. No public
        # target, private-network exception or Chromium installation is needed.
        denied = await client.post(
            settings.browser_worker_url + "/internal/tools/browser_close",
            json={"session_id": "atr-remote-" + case_id},
            headers={"x-browser-worker-token": "deliberately-wrong-token"},
        )
        assert denied.status_code == 401
        async with client.stream(
            "POST",
            "/v1/agent/stream",
            json={
                "conversation_id": "atr-remote-" + case_id,
                "query": (
                    f"atr-remote-tool-e2e:{case_id} Use the browser close tool to close "
                    f"the supplied session atr-remote-{case_id}. Report the result."
                ),
                "max_retries": 0,
                "max_tool_rounds": 1,
            },
        ) as response:
            response.raise_for_status()
            request_id = response.headers["x-request-id"]
            live = (await response.aread()).decode()
        observed = events(live)
        blocked = [
            event
            for event in observed
            if event.get("type") == "threat_detection"
            and event.get("event_type") == "tool_response"
            and event.get("location") == "tool_response:browser_close"
            and event.get("status") == "blocked"
        ]
        assert blocked and blocked[0]["findings"], (
            "No real ATR tool-response block occurred: "
            + str(
                [
                    {
                        key: event[key]
                        for key in ("type", "tool", "status", "location", "error")
                        if key in event
                    }
                    for event in observed
                    if event.get("type") in {"tool_status", "threat_detection", "error"}
                ]
            )
        )
        assert any(
            event.get("type") == "tool_status"
            and event.get("status") == "failed"
            and "ATR blocked" in event.get("error", "")
            for event in observed
        ), "The tool result was not rejected"
        assert not any(
            event.get("type") == "tool_status" and event.get("status") == "completed"
            for event in observed
        ), "The blocked tool response was accepted"
        assert any(event.get("type") == "completed" for event in observed)
        assert_private_absent(live, canary)

        replay_lines = []
        async with client.stream("GET", f"/v1/agent/runs/{request_id}/events") as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                replay_lines.append(line)
                if line.startswith("data: ") and json.loads(line[6:]).get("type") == "completed":
                    break
        replay = "\n".join(replay_lines)
        assert_private_absent(replay, canary)
        assert any(
            event.get("location") == "tool_response:browser_close"
            and event.get("status") == "blocked"
            for event in events(replay)
        )

        transport = (await client.get("/test/atr-transport")).json()
        assert transport == {
            "mcp_executor": True,
            "stdio_session_initialized": True,
            "production_mcp_server": True,
        }, transport
        browser = await client.get(
            settings.browser_worker_url + "/test/atr-browser-receipts/" + case_id,
            headers=worker_headers,
        )
        browser.raise_for_status()
        browser_receipts = browser.json()["receipts"]
        assert browser_receipts == [
            {"case_id": case_id, "production_handler": True, "injected": True}
        ], browser_receipts
        model = await client.get("http://mock-llm:8080/test/atr-model-receipts/" + case_id)
        model.raise_for_status()
        model_receipts = model.json()["receipts"]
        assert any(entry["stage"] == "planning" for entry in model_receipts), (
            "No real planning model call"
        )
        assert any(entry["stage"] == "generation" for entry in model_receipts), (
            "No post-tool generation call"
        )
        assert all(not entry["contains_canary"] for entry in model_receipts), (
            "Blocked remote response entered a model request"
        )
        assert all(
            set(entry) == {"case_id", "stream", "stage", "contains_canary"}
            for entry in model_receipts
        )

    runtime = RedisRuntime(settings.redis_url)
    try:
        raw = await runtime._client.xrange(f"tenant:default:agent:events:{request_id}")
        assert raw
        assert_private_absent(raw, canary)
        assert_private_absent(await runtime.get_run_state(request_id, "default"), canary)
        findings = await runtime.list_threat_findings("default")
        matching = [value for value in findings if value["context_id"] == request_id]
        assert matching and all(value["enforcement_status"] == "blocked" for value in matching)
        assert_private_absent(matching, canary)
        assert_private_absent(await runtime._client.xrange("tenant:default:audit"), canary)
    finally:
        await runtime.close()
    print(
        json.dumps(
            {
                "status": "passed",
                "case_id": case_id,
                "mcp_stdio": True,
                "authenticated_browser_worker": True,
                "tool_response_atr_block": True,
                "model_context_clean": True,
                "live_sse_clean": True,
                "replay_and_redis_clean": True,
            }
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
