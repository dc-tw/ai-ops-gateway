import json
from types import SimpleNamespace

import httpx
import pytest
from acp.schema import AllowedOutcome, RequestPermissionResponse, TextContentBlock

from app.acp_adapter import AdapterConfig, GatewayAcpAgent


class AcpClient:
    def __init__(self, allow=True):
        self.updates = []
        self.allow = allow

    async def session_update(self, session_id, update, **kwargs):
        self.updates.append((session_id, update.model_dump(by_alias=True, exclude_none=True)))

    async def request_permission(self, session_id, tool_call, options, **kwargs):
        option = "allow-once" if self.allow else "reject-once"
        return RequestPermissionResponse(
            outcome=AllowedOutcome(outcome="selected", option_id=option)
        )


def config():
    return AdapterConfig(
        base_url="http://127.0.0.1:8000",
        api_key="test-key",
        allow_insecure_localhost=True,
    )


def sse(*events):
    rows = []
    for index, (name, payload) in enumerate(events, 1):
        rows.append(
            f"id: {index}-0\nevent: {name}\ndata: {json.dumps(payload)}\n\n"
        )
    return "".join(rows)


@pytest.mark.asyncio
async def test_adapter_session_mode_prompt_and_history_replay():
    requests = []
    mode = "plan"
    version = 1
    session = {
        "session_id": "session-one",
        "conversation_id": "session-one",
        "mode": "plan",
        "version": 1,
    }

    async def handler(request):
        nonlocal mode, version
        requests.append(request)
        path = request.url.path
        if request.method == "POST" and path == "/v1/acp/sessions":
            body = json.loads(request.content)
            assert len(body["cwd_sha256"]) == 64
            assert "/private/work" not in request.content.decode()
            return httpx.Response(200, json={"session": session})
        if request.method == "GET" and path == "/v1/acp/sessions/session-one":
            include_history = request.url.params.get("include_history") != "false"
            return httpx.Response(
                200,
                json={
                    "session": {**session, "mode": mode, "version": version},
                    "history": (
                        [
                            {"role": "user", "content": "先前問題", "request_id": "r0"},
                            {"role": "assistant", "content": "先前回答", "request_id": "r0"},
                        ]
                        if include_history
                        else []
                    ),
                },
            )
        if request.method == "PUT" and path.endswith("/mode"):
            body = json.loads(request.content)
            assert body == {"expected_version": 1, "mode": "execute"}
            mode = "execute"
            version = 2
            return httpx.Response(200, json={"session": {**session, **body, "version": 2}})
        if request.method == "POST" and path == "/v1/agent/stream":
            body = json.loads(request.content)
            assert body["conversation_id"] == "session-one"
            assert body["query"].startswith("目前是規劃模式")
            return httpx.Response(
                200,
                headers={"x-request-id": "run-one", "content-type": "text/event-stream"},
                text=sse(
                    ("token", {"type": "token", "content": "安全"}),
                    ("completed", {"type": "completed", "answer": "安全完成"}),
                ),
            )
        raise AssertionError(f"unexpected request: {request.method} {path}")

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=config().base_url)
    agent = GatewayAcpAgent(config(), http)
    client = AcpClient()
    agent.on_connect(client)
    created = await agent.new_session(cwd="/private/work")
    assert created.session_id == "session-one"
    assert created.modes.current_mode_id == "plan"
    result = await agent.prompt(
        "session-one", [TextContentBlock(type="text", text="建立計畫")]
    )
    assert result.stop_reason == "end_turn"
    assert any(request.url.query == b"include_history=false" for request in requests)
    assert any(row[1].get("sessionUpdate") == "agent_message_chunk" for row in client.updates)
    await agent.set_session_mode("session-one", "execute")
    loaded = await agent.load_session(cwd="/other/path", session_id="session-one")
    assert loaded.modes.current_mode_id == "execute"
    assert [row[1]["sessionUpdate"] for row in client.updates[-2:]] == [
        "user_message_chunk",
        "agent_message_chunk",
    ]
    await http.aclose()


@pytest.mark.asyncio
async def test_adapter_maps_one_time_permission_and_replays_after_approval():
    approval = []

    async def handler(request):
        if request.method == "GET" and request.url.path == "/v1/acp/sessions/session-two":
            return httpx.Response(
                200,
                json={
                    "session": {"session_id": "session-two", "mode": "execute", "version": 1},
                    "history": [],
                },
            )
        if request.url.path == "/v1/agent/stream":
            return httpx.Response(
                200,
                headers={"x-request-id": "run-two"},
                text=sse(
                    (
                        "approval_required",
                        {
                            "type": "approval_required",
                            "action_id": "action-one",
                            "tool": "browser_click",
                            "arguments": {"selector": "#deploy"},
                        },
                    )
                ),
            )
        if request.url.path.endswith("/approval"):
            approval.append(json.loads(request.content))
            return httpx.Response(200, json={"status": "resuming"})
        if request.url.path.endswith("/events"):
            assert request.headers["last-event-id"] == "1-0"
            return httpx.Response(
                200,
                text=sse(
                    ("token", {"type": "token", "content": "已完成"}),
                    ("completed", {"type": "completed", "answer": "已完成"}),
                ),
            )
        raise AssertionError(request.url.path)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=config().base_url)
    agent = GatewayAcpAgent(config(), http)
    agent._sessions["session-two"] = {"session_id": "session-two", "mode": "execute"}
    client = AcpClient(allow=True)
    agent.on_connect(client)
    result = await agent.prompt(
        "session-two", [TextContentBlock(type="text", text="執行已核可工作")]
    )
    assert result.stop_reason == "end_turn"
    assert approval == [
        {"approved": True, "reason": "ACP human selected a one-time permission"}
    ]
    assert any(row[1].get("sessionUpdate") == "tool_call" for row in client.updates)
    await http.aclose()


@pytest.mark.asyncio
async def test_adapter_cancel_uses_remote_cancel_and_client_grants_never_expand_authority():
    called = []

    async def handler(request):
        called.append((request.method, request.url.path))
        return httpx.Response(200, json={"status": "cancelled"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=config().base_url)
    agent = GatewayAcpAgent(config(), http)
    run = SimpleNamespace(request_id="run-three", cancelled=__import__("asyncio").Event())
    agent._active["session-three"] = run
    await agent.cancel("session-three")
    assert run.cancelled.is_set()
    assert called == [("POST", "/v1/agent/runs/run-three/cancel")]
    with pytest.raises(Exception) as directories:
        await agent.new_session(cwd="/work", additional_directories=["/tmp"])
    assert "filesystem or MCP" in directories.value.data["reason"]
    with pytest.raises(Exception) as mcp:
        await agent.new_session(cwd="/work", mcp_servers=[{"name": "unsafe"}])
    assert "filesystem or MCP" in mcp.value.data["reason"]
    await http.aclose()


def test_adapter_config_requires_https_unless_localhost_is_explicit():
    with pytest.raises(ValueError, match="HTTPS"):
        AdapterConfig(base_url="http://gateway.example", api_key="secret").validate()
    with pytest.raises(ValueError, match="HTTPS"):
        AdapterConfig(base_url="https://gateway.example/subpath", api_key="secret").validate()
    AdapterConfig(
        base_url="http://localhost:8000",
        api_key="secret",
        allow_insecure_localhost=True,
    ).validate()
