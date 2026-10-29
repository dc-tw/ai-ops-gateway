"""Opt-in official ACP reference-client/stdio transport verification."""

import asyncio
import os
import socket
import sys
import time
from pathlib import Path

import pytest
import uvicorn
from acp import PROTOCOL_VERSION, spawn_agent_process
from acp.schema import DeniedOutcome, RequestPermissionResponse, TextContentBlock
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_ACP_STDIO_E2E") != "true",
    reason="Set RUN_ACP_STDIO_E2E=true for the subprocess/localhost protocol test",
)
ROOT = Path(__file__).resolve().parent.parent


def wait_started(server):
    while not server.started:
        time.sleep(0.01)


class ReferenceClient:
    def __init__(self):
        self.updates = []

    async def session_update(self, session_id, update, **kwargs):
        self.updates.append(update.model_dump(by_alias=True, exclude_none=True))

    async def request_permission(self, session_id, tool_call, options, **kwargs):
        return RequestPermissionResponse(outcome=DeniedOutcome(outcome="cancelled"))


@pytest.mark.asyncio
async def test_official_reference_client_drives_stdio_adapter():
    app = FastAPI()
    cancel_started = asyncio.Event()
    cancel_received = asyncio.Event()

    @app.post("/v1/acp/sessions")
    async def create_session(request: Request):
        assert request.headers["x-api-key"] == "stdio-secret"
        return {
            "session": {
                "session_id": "stdio-session",
                "conversation_id": "stdio-session",
                "mode": "plan",
                "version": 1,
            }
        }

    @app.get("/v1/acp/sessions/{session_id}")
    async def get_session(session_id: str, include_history: bool = True):
        assert session_id == "stdio-session"
        assert include_history is False
        return {
            "session": {
                "session_id": "stdio-session",
                "conversation_id": "stdio-session",
                "mode": "plan",
                "version": 1,
            },
            "history": [],
        }

    @app.post("/v1/agent/stream")
    async def prompt(request: Request):
        body = await request.json()

        async def rows():
            if "取消測試" in body["query"]:
                cancel_started.set()
                yield 'id: 1-0\nevent: accepted\ndata: {"type":"accepted"}\n\n'
                await cancel_received.wait()
                yield 'id: 2-0\nevent: cancelled\ndata: {"type":"cancelled"}\n\n'
                return
            yield 'id: 1-0\nevent: token\ndata: {"type":"token","content":"stdio-ok"}\n\n'
            yield 'id: 2-0\nevent: completed\ndata: {"type":"completed","answer":"stdio-ok"}\n\n'

        return StreamingResponse(
            rows(), media_type="text/event-stream", headers={"x-request-id": "stdio-run"}
        )

    @app.post("/v1/agent/runs/{request_id}/cancel")
    async def cancel_run(request_id: str):
        assert request_id == "stdio-run"
        cancel_received.set()
        return {"status": "cancelled"}

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    config = uvicorn.Config(app, log_level="error", lifespan="off")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve(sockets=[sock]))
    await asyncio.to_thread(wait_started, server)

    client = ReferenceClient()
    env = {
        **os.environ,
        "GATEWAY_BASE_URL": f"http://127.0.0.1:{port}",
        "GATEWAY_API_KEY": "stdio-secret",
        "GATEWAY_ACP_ALLOW_INSECURE_LOCALHOST": "true",
    }
    try:
        async with spawn_agent_process(
            client,
            sys.executable,
            "-m",
            "app.acp_adapter",
            env=env,
            cwd=ROOT,
        ) as (connection, process):
            initialized = await connection.initialize(protocol_version=PROTOCOL_VERSION)
            assert initialized.protocol_version == 1
            assert initialized.agent_capabilities.load_session is True
            session = await connection.new_session(cwd=str(ROOT), mcp_servers=[])
            assert session.session_id == "stdio-session"
            result = await connection.prompt(
                session_id=session.session_id,
                prompt=[TextContentBlock(type="text", text="驗證 stdio")],
            )
            assert result.stop_reason == "end_turn"
            assert any(
                update.get("content", {}).get("text") == "stdio-ok"
                for update in client.updates
            )
            pending = asyncio.create_task(
                connection.prompt(
                    session_id=session.session_id,
                    prompt=[TextContentBlock(type="text", text="取消測試")],
                )
            )
            await asyncio.wait_for(cancel_started.wait(), 2)
            await connection.cancel(session_id=session.session_id)
            cancelled = await asyncio.wait_for(pending, 2)
            assert cancelled.stop_reason == "cancelled"
            assert process.returncode is None
    finally:
        server.should_exit = True
        await server_task
