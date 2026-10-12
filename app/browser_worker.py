from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, HTTPException

from app.core.config import get_settings
from app.tools.browser import BrowserScraper, BrowserSessionManager
from app.tools.registry import TOOL_SPECS, LocalBrowserToolExecutor

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    scraper = BrowserScraper(settings)
    sessions = BrowserSessionManager(settings)
    app.state.executor = LocalBrowserToolExecutor(scraper, sessions)
    yield
    await sessions.close_all()


app = FastAPI(title="Isolated Browser Worker", lifespan=lifespan, docs_url=None, redoc_url=None)


@app.get("/healthz")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/internal/tools/{tool_name}")
async def invoke_tool(
    tool_name: str,
    arguments: dict[str, Any],
    x_browser_worker_token: str = Header(default=""),
) -> dict[str, object]:
    if settings.browser_worker_token and x_browser_worker_token != settings.browser_worker_token:
        raise HTTPException(status_code=401, detail="Invalid browser worker token")
    if tool_name not in TOOL_SPECS:
        raise HTTPException(status_code=404, detail="Unknown tool")
    try:
        return await app.state.executor.invoke(tool_name, arguments)
    except (KeyError, ValueError, PermissionError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
