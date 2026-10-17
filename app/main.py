import asyncio
import contextlib
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.api.security_routes import router as security_router
from app.core.config import Settings, get_settings
from app.core.observability import install_observability
from app.core.redis_runtime import RedisRuntime
from app.core.threat_detection import ThreatDetector
from app.goals import router as goals_router
from app.llm.client import ModelRouter
from app.tools.browser import BrowserScraper, BrowserSessionManager
from app.tools.registry import (
    LocalBrowserToolExecutor,
    MCPBrowserToolExecutor,
    RemoteBrowserScraper,
    RemoteBrowserToolExecutor,
)


def create_app(*, graph=None, settings: Settings | None = None, redis_runtime=None) -> FastAPI:
    app_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = app_settings
        app.state.model_router = ModelRouter(app_settings)
        app.state.threat_detector = ThreatDetector.from_settings(app_settings)
        app.state.scraper = BrowserScraper(app_settings)
        app.state.browser_sessions = BrowserSessionManager(app_settings)
        if app_settings.browser_worker_url:
            remote = RemoteBrowserToolExecutor(
                app_settings.browser_worker_url,
                app_settings.browser_worker_token,
                timeout_seconds=max(60.0, app_settings.playwright_timeout_ms / 1_000 + 10),
            )
            app.state.tool_executor = remote
            app.state.scraper = RemoteBrowserScraper(remote)
        elif app_settings.agent_tool_transport == "mcp":
            app.state.tool_executor = MCPBrowserToolExecutor()
        else:
            app.state.tool_executor = LocalBrowserToolExecutor(
                app.state.scraper, app.state.browser_sessions
            )
        app.state.graph = graph
        app.state.active_runs = set()
        app.state.active_run_ids = set()
        app.state.active_run_tasks = {}
        app.state.redis = redis_runtime
        if app.state.redis is None:
            candidate = RedisRuntime(app_settings.redis_url, app_settings.redis_stream_maxlen)
            try:
                await candidate.ping()
                app.state.redis = candidate
            except Exception:
                await candidate.close()
                app.state.redis = None
        app.state.checkpointer = None
        if app_settings.langgraph_checkpointing and isinstance(app.state.redis, RedisRuntime):
            from langgraph.checkpoint.redis.aio import AsyncRedisSaver

            checkpointer = AsyncRedisSaver(
                redis_url=app_settings.redis_url,
                ttl={"default_ttl": app_settings.checkpoint_ttl_minutes, "refresh_on_read": True},
            )
            await checkpointer.__aenter__()
            app.state.checkpointer = checkpointer
        yield
        for task in list(app.state.active_runs):
            task.cancel()
        if app.state.active_runs:
            await asyncio.gather(*app.state.active_runs, return_exceptions=True)
        if app.state.checkpointer:
            with contextlib.suppress(Exception):
                await app.state.checkpointer.__aexit__(None, None, None)
        if app.state.redis:
            with contextlib.suppress(Exception):
                await app.state.redis.close()
        close_tool_executor = getattr(app.state.tool_executor, "close", None)
        if close_tool_executor:
            with contextlib.suppress(Exception):
                await close_tool_executor()
        await app.state.browser_sessions.close_all()

    app = FastAPI(
        title="AI Operations Gateway",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(router)
    app.include_router(security_router)
    app.include_router(goals_router)
    install_observability(app, app_settings)
    return app


app = create_app()
