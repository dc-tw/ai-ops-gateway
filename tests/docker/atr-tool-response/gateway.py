"""Test-only transport wiring; all public routes, identity and graph are production."""

from contextlib import asynccontextmanager

from fastapi import Request

from app.core.security import authorize
from app.main import create_app
from app.tools.registry import MCPBrowserToolExecutor

app = create_app()
production_lifespan = app.router.lifespan_context


@asynccontextmanager
async def mcp_lifespan(application):
    async with production_lifespan(application):
        # BROWSER_WORKER_URL normally chooses direct HTTP before the MCP branch.
        # For this dedicated fixture, use the real stdio client instead; its real
        # app.mcp_server child inherits that URL and calls the HTTP Browser Worker.
        application.state.tool_executor = MCPBrowserToolExecutor()
        yield


app.router.lifespan_context = mcp_lifespan


@app.get("/test/atr-transport")
async def transport_receipt(request: Request):
    await authorize(request, "viewer")
    executor = request.app.state.tool_executor
    return {
        "mcp_executor": isinstance(executor, MCPBrowserToolExecutor),
        "stdio_session_initialized": executor._session is not None,
        "production_mcp_server": executor._args == ["-m", "app.mcp_server"],
    }
