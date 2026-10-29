import asyncio
import contextlib
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.access import router as access_router
from app.acp_sessions import router as acp_sessions_router
from app.agent_credentials import router as agent_credentials_router
from app.agents import router as agents_router
from app.api.routes import router
from app.api.security_routes import router as security_router
from app.auth import router as auth_router
from app.automations import router as automations_router
from app.collaboration import router as collaboration_router
from app.core.checkpoint_bootstrap import open_checkpointer
from app.core.config import Settings, get_settings
from app.core.observability import install_observability
from app.core.redis_runtime import RedisRuntime
from app.core.threat_detection import ThreatDetector
from app.goals import router as goals_router
from app.history import router as history_router
from app.llm.client import ModelRouter
from app.model_registry import router as model_registry_router
from app.notifications import router as notifications_router
from app.planning import router as planning_router
from app.skill_imports import router as skill_imports_router
from app.skills import router as skills_router
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
            candidate = RedisRuntime(
                app_settings.redis_url,
                app_settings.redis_stream_maxlen,
                durable_source_id=(
                    app_settings.postgres_source_id if app_settings.postgres_dsn else None
                ),
            )
            try:
                await candidate.ping()
                app.state.redis = candidate
            except Exception:
                await candidate.close()
                app.state.redis = None
        app.state.postgres = None
        if app_settings.postgres_dsn:
            from app.core.postgres_store import PostgresStore

            store = PostgresStore(app_settings.postgres_dsn, app_settings.postgres_source_id)
            try:
                await store.open()
            except Exception:
                await store.close()
                raise RuntimeError(
                    "Postgres history startup failed; check schema and service"
                ) from None
            app.state.postgres = store
        app.state.checkpointer = None
        if app_settings.langgraph_checkpointing and isinstance(app.state.redis, RedisRuntime):
            checkpointer = await open_checkpointer(app.state.redis, app_settings)
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
        if app.state.postgres:
            with contextlib.suppress(Exception):
                await app.state.postgres.close()
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
    app.include_router(acp_sessions_router)
    app.include_router(auth_router)
    app.include_router(collaboration_router)
    app.include_router(security_router)
    app.include_router(goals_router)
    app.include_router(automations_router)
    app.include_router(agents_router)
    app.include_router(agent_credentials_router)
    app.include_router(access_router)
    app.include_router(skills_router)
    app.include_router(skill_imports_router)
    app.include_router(planning_router)
    app.include_router(history_router)
    app.include_router(model_registry_router)
    app.include_router(notifications_router)
    console = Path(__file__).resolve().parent.parent / "ui" / "dist"
    if console.is_dir():
        app.mount("/console", StaticFiles(directory=console, html=True), name="console")

    @app.middleware("http")
    async def durable_execution_boundary(request: Request, call_next):
        if (
            app_settings.postgres_dsn
            and getattr(app.state, "redis", None) is None
            and request.url.path.startswith("/v1/")
            and not request.url.path.startswith("/v1/history")
        ):
            # Archive availability is NOT permission to fall back to in-memory
            # execution or revive an old approval/Skill/Goal from Postgres.
            return JSONResponse(status_code=503, content={"detail": "Execution requires Redis"})
        try:
            return await call_next(request)
        except (RedisConnectionError, RedisTimeoutError):
            if not app_settings.postgres_dsn:
                raise
            return JSONResponse(status_code=503, content={"detail": "Execution requires Redis"})
        except ResponseError as exc:
            if "DURABLE_" not in str(exc):
                raise
            if "DURABLE_PAYLOAD" in str(exc):
                return JSONResponse(
                    status_code=422, content={"detail": "Content cannot be archived safely"}
                )
            return JSONResponse(
                status_code=503, content={"detail": "Durable writes temporarily unavailable"}
            )

    install_observability(app, app_settings)
    return app


app = create_app()
