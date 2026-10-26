"""The real MCP SDK gets only explicitly allowed browser service settings."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import mcp
import mcp.client.stdio
import pytest

from app.tools.registry import MCPBrowserToolExecutor


@pytest.mark.parametrize("configured", [False, True])
async def test_mcp_child_receives_browser_settings_but_not_unrelated_credentials(
    monkeypatch, configured
):
    captured = {}
    allowed = (
        "BROWSER_WORKER_URL",
        "BROWSER_WORKER_TOKEN",
        "PLAYWRIGHT_ALLOWED_DOMAINS",
        "PLAYWRIGHT_HEADLESS",
        "PLAYWRIGHT_TIMEOUT_MS",
        "PLAYWRIGHT_MAX_CONTENT_CHARS",
        "PLAYWRIGHT_BROWSERS_PATH",
        "BROWSER_SESSION_TTL_SECONDS",
        "BROWSER_MAX_SESSIONS",
    )
    for name in allowed:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("LLM_API_KEY", "PRIVATE_MODEL_KEY")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "PRIVATE_CLOUD_KEY")
    monkeypatch.setenv("API_KEY", "PRIVATE_GATEWAY_KEY")
    expected = {}
    if configured:
        expected = {
            "BROWSER_WORKER_URL": "http://browser:8010",
            "BROWSER_WORKER_TOKEN": "browser-only-test-token",
        }
        for name, value in expected.items():
            monkeypatch.setenv(name, value)

    def parameters(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(**kwargs)

    @asynccontextmanager
    async def stdio_client(params):
        yield object(), object()

    class Session:
        def __init__(self, *args):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def initialize(self):
            captured["initialized"] = True

    monkeypatch.setattr(mcp, "StdioServerParameters", parameters)
    monkeypatch.setattr(mcp, "ClientSession", Session)
    monkeypatch.setattr(mcp.client.stdio, "stdio_client", stdio_client)
    executor = MCPBrowserToolExecutor()
    try:
        await executor._connect()
        assert captured["env"] == expected
        assert captured["args"] == ["-m", "app.mcp_server"]
        assert captured["initialized"] is True
        assert all(
            name not in captured["env"]
            for name in ("LLM_API_KEY", "AWS_SECRET_ACCESS_KEY", "API_KEY")
        )
    finally:
        await executor.close()
