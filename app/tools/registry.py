from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any, Protocol

from app.tools.browser import BrowserScraper, BrowserSessionManager


@dataclass(frozen=True, slots=True)
class ToolSpec:
    name: str
    description: str
    requires_approval: bool = False


TOOL_SPECS = {
    "scrape_dynamic_page": ToolSpec(
        "scrape_dynamic_page", "Render a public webpage and extract cleaned text."
    ),
    "browser_start": ToolSpec("browser_start", "Start a persistent isolated browser session."),
    "browser_click": ToolSpec(
        "browser_click", "Click an element in a browser session.", requires_approval=True
    ),
    "browser_type": ToolSpec(
        "browser_type", "Type into an element in a browser session.", requires_approval=True
    ),
    "browser_screenshot": ToolSpec(
        "browser_screenshot", "Capture a PNG screenshot from a browser session."
    ),
    "browser_close": ToolSpec("browser_close", "Close a persistent browser session."),
}


class ToolExecutor(Protocol):
    def specs(self) -> list[ToolSpec]: ...

    async def invoke(self, name: str, arguments: dict[str, Any]) -> dict[str, object]: ...


class LocalBrowserToolExecutor:
    def __init__(self, scraper: BrowserScraper, sessions: BrowserSessionManager) -> None:
        self._scraper = scraper
        self._sessions = sessions

    def specs(self) -> list[ToolSpec]:
        return list(TOOL_SPECS.values())

    async def invoke(self, name: str, arguments: dict[str, Any]) -> dict[str, object]:
        scope = (
            {"allowed_domains": arguments["allowed_domains"]}
            if "allowed_domains" in arguments
            else {}
        )
        if name == "scrape_dynamic_page":
            return await self._scraper.scrape(str(arguments["url"]), **scope)
        if name == "browser_start":
            return await self._sessions.start(str(arguments["url"]), **scope)
        if name == "browser_click":
            return await self._sessions.click(
                str(arguments["session_id"]), str(arguments["selector"])
            )
        if name == "browser_type":
            return await self._sessions.type_text(
                str(arguments["session_id"]),
                str(arguments["selector"]),
                str(arguments["text"]),
                clear=bool(arguments.get("clear", True)),
            )
        if name == "browser_screenshot":
            return await self._sessions.screenshot(
                str(arguments["session_id"]), full_page=bool(arguments.get("full_page", False))
            )
        if name == "browser_close":
            return {
                "session_id": str(arguments["session_id"]),
                "closed": await self._sessions.close(str(arguments["session_id"])),
            }
        raise ValueError(f"Unknown tool: {name}")


class MCPBrowserToolExecutor:
    """Invoke browser tools through one persistent, serialized MCP stdio session."""

    def __init__(self, command: str | None = None, args: list[str] | None = None) -> None:
        self._command = command or sys.executable
        self._args = args or ["-m", "app.mcp_server"]
        self._stack: AsyncExitStack | None = None
        self._session: Any = None
        self._lock = asyncio.Lock()

    def specs(self) -> list[ToolSpec]:
        return list(TOOL_SPECS.values())

    async def _connect(self) -> None:
        if self._session is not None:
            return
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        stack = AsyncExitStack()
        try:
            # MCP deliberately forwards only a minimal OS environment. Pass the
            # browser server configuration explicitly, never unrelated model,
            # cloud or gateway credentials from the parent process.
            browser_environment = {
                key: os.environ[key]
                for key in (
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
                if key in os.environ
            }
            params = StdioServerParameters(
                command=self._command, args=self._args, env=browser_environment
            )
            reader, writer = await stack.enter_async_context(stdio_client(params))
            session = await stack.enter_async_context(ClientSession(reader, writer))
            await session.initialize()
        except Exception:
            await stack.aclose()
            raise
        self._stack = stack
        self._session = session

    async def close(self) -> None:
        async with self._lock:
            stack, self._stack = self._stack, None
            self._session = None
            if stack is not None:
                await stack.aclose()

    async def invoke(self, name: str, arguments: dict[str, Any]) -> dict[str, object]:
        if name not in TOOL_SPECS:
            raise ValueError(f"Unknown tool: {name}")
        async with self._lock:
            await self._connect()
            try:
                result = await self._session.call_tool(name, arguments=arguments)
            except Exception:
                stack, self._stack = self._stack, None
                self._session = None
                if stack is not None:
                    await stack.aclose()
                raise
        if result.isError:
            detail = " ".join(
                block.text for block in result.content if getattr(block, "type", None) == "text"
            )
            raise RuntimeError(detail or f"MCP tool failed: {name}")
        structured = getattr(result, "structuredContent", None)
        if isinstance(structured, dict):
            return structured
        for block in result.content:
            if getattr(block, "type", None) == "text":
                value = json.loads(block.text)
                if isinstance(value, dict):
                    return value
        raise RuntimeError(f"MCP tool returned no object result: {name}")


class RemoteBrowserToolExecutor:
    """HTTP client for the network-isolated browser worker."""

    def __init__(self, base_url: str, token: str, timeout_seconds: float = 60.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout_seconds

    def specs(self) -> list[ToolSpec]:
        return list(TOOL_SPECS.values())

    async def invoke(self, name: str, arguments: dict[str, Any]) -> dict[str, object]:
        if name not in TOOL_SPECS:
            raise ValueError(f"Unknown tool: {name}")
        import httpx

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                f"{self._base_url}/internal/tools/{name}",
                json=arguments,
                headers={"x-browser-worker-token": self._token},
            )
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict):
            raise RuntimeError("Browser worker returned a non-object result")
        return result


class RemoteBrowserScraper:
    def __init__(self, executor: RemoteBrowserToolExecutor) -> None:
        self._executor = executor

    async def scrape(
        self, url: str, *, allowed_domains: list[str] | None = None
    ) -> dict[str, object]:
        arguments: dict[str, Any] = {"url": url}
        if allowed_domains is not None:
            arguments["allowed_domains"] = allowed_domains
        return await self._executor.invoke("scrape_dynamic_page", arguments)
