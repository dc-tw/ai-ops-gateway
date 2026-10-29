"""ACP v1 stdio adapter for a remote AI Ops Gateway.

Stdout belongs exclusively to the ACP transport. Credentials are read from the
launch environment and are never echoed into protocol messages.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import httpx
from acp import Agent, RequestError, run_agent
from acp.interfaces import Client
from acp.schema import (
    AgentCapabilities,
    AgentMessageChunk,
    Implementation,
    InitializeResponse,
    LoadSessionResponse,
    NewSessionResponse,
    PermissionOption,
    PromptResponse,
    SessionMode,
    SessionModeState,
    SetSessionModeResponse,
    TextContentBlock,
    ToolCallProgress,
    ToolCallStart,
    ToolCallUpdate,
    UserMessageChunk,
)


@dataclass(frozen=True)
class AdapterConfig:
    base_url: str
    api_key: str
    model: str | None = None
    allow_insecure_localhost: bool = False

    @classmethod
    def from_env(cls) -> AdapterConfig:
        value = cls(
            base_url=os.environ.get("GATEWAY_BASE_URL", "").rstrip("/"),
            api_key=os.environ.get("GATEWAY_API_KEY", ""),
            model=os.environ.get("GATEWAY_MODEL") or None,
            allow_insecure_localhost=(
                os.environ.get("GATEWAY_ACP_ALLOW_INSECURE_LOCALHOST", "").lower() == "true"
            ),
        )
        value.validate()
        return value

    def validate(self) -> None:
        parsed = urlparse(self.base_url)
        local = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        if (
            not self.base_url
            or parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or parsed.scheme != "https"
            and not (local and self.allow_insecure_localhost)
        ):
            raise ValueError("GATEWAY_BASE_URL must be HTTPS (or explicitly allowed localhost)")
        if not self.api_key or len(self.api_key) > 4_096:
            raise ValueError("GATEWAY_API_KEY is required")


@dataclass
class ActiveRun:
    cancelled: asyncio.Event
    request_id: str = ""
    last_event_id: str = "0-0"
    started_tools: set[str] = field(default_factory=set)


def _modes(current: str) -> SessionModeState:
    return SessionModeState(
        current_mode_id=current,
        available_modes=[
            SessionMode(
                id="plan",
                name="規劃",
                description="只要求規劃輸出；切換模式本身不執行，也不核准任何寫入。",
            ),
            SessionMode(
                id="execute",
                name="受治理執行",
                description="允許請求執行；所有既有工具、ATR、ACL 與逐次核准仍然生效。",
            ),
        ],
    )


class GatewayAcpAgent(Agent):
    def __init__(self, config: AdapterConfig, http: httpx.AsyncClient | None = None) -> None:
        config.validate()
        self.config = config
        self._http = http or httpx.AsyncClient(
            base_url=config.base_url,
            headers={"x-api-key": config.api_key},
            timeout=httpx.Timeout(10.0, read=None),
            follow_redirects=False,
        )
        self._owns_http = http is None
        self._conn: Client | None = None
        self._sessions: dict[str, dict[str, Any]] = {}
        self._active: dict[str, ActiveRun] = {}

    def on_connect(self, conn: Client) -> None:
        self._conn = conn

    async def initialize(
        self,
        protocol_version: int,
        client_capabilities=None,
        client_info=None,
        **kwargs: Any,
    ) -> InitializeResponse:
        # Client filesystem/terminal/MCP declarations are deliberately not adopted.
        return InitializeResponse(
            protocol_version=1,
            agent_capabilities=AgentCapabilities(load_session=True),
            agent_info=Implementation(
                name="ai-ops-gateway",
                title="AI Ops Gateway（遠端受治理）",
                version="0.1.0",
            ),
        )

    @staticmethod
    def _reject_local_capabilities(
        additional_directories: list[str] | None, mcp_servers: list[Any] | None
    ) -> None:
        if additional_directories or mcp_servers:
            raise RequestError.invalid_params(
                {"reason": "Gateway ACP adapter does not accept client filesystem or MCP grants"}
            )

    async def _json(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        try:
            response = await self._http.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise RequestError.internal_error({"reason": "Gateway unavailable"}) from exc
        if response.status_code == 401:
            raise RequestError.auth_required()
        if response.status_code == 404:
            raise RequestError.resource_not_found(path)
        if not response.is_success:
            raise RequestError.internal_error(
                {"status": response.status_code, "reason": "Gateway request failed"}
            )
        try:
            value = response.json()
        except ValueError as exc:
            raise RequestError.internal_error({"reason": "Invalid Gateway response"}) from exc
        if not isinstance(value, dict):
            raise RequestError.internal_error({"reason": "Invalid Gateway response"})
        return value

    async def new_session(
        self,
        cwd: str,
        additional_directories: list[str] | None = None,
        mcp_servers: list[Any] | None = None,
        **kwargs: Any,
    ) -> NewSessionResponse:
        self._reject_local_capabilities(additional_directories, mcp_servers)
        value = await self._json(
            "POST",
            "/v1/acp/sessions",
            json={
                "cwd_sha256": hashlib.sha256(cwd.encode("utf-8")).hexdigest(),
                "client_name": "acp-v1-stdio",
            },
        )
        session = value.get("session")
        if not isinstance(session, dict) or not isinstance(session.get("session_id"), str):
            raise RequestError.internal_error({"reason": "Invalid ACP session response"})
        self._sessions[session["session_id"]] = session
        return NewSessionResponse(session_id=session["session_id"], modes=_modes("plan"))

    async def load_session(
        self,
        cwd: str,
        session_id: str,
        mcp_servers: list[Any] | None = None,
        additional_directories: list[str] | None = None,
        **kwargs: Any,
    ) -> LoadSessionResponse:
        self._reject_local_capabilities(additional_directories, mcp_servers)
        value = await self._json("GET", f"/v1/acp/sessions/{session_id}")
        session = value.get("session")
        history = value.get("history", [])
        if not isinstance(session, dict) or not isinstance(history, list):
            raise RequestError.internal_error({"reason": "Invalid ACP session history"})
        self._sessions[session_id] = session
        if self._conn is None:
            raise RequestError.internal_error({"reason": "ACP connection unavailable"})
        for index, item in enumerate(history):
            if not isinstance(item, dict) or item.get("role") not in {"user", "assistant"}:
                continue
            content = str(item.get("content", ""))
            block = TextContentBlock(type="text", text=content)
            message_id = str(item.get("request_id") or f"history-{index}")
            if item["role"] == "user":
                update = UserMessageChunk(
                    session_update="user_message_chunk", content=block, message_id=message_id
                )
            else:
                update = AgentMessageChunk(
                    session_update="agent_message_chunk", content=block, message_id=message_id
                )
            await self._conn.session_update(session_id=session_id, update=update)
        return LoadSessionResponse(modes=_modes(str(session.get("mode", "plan"))))

    async def set_session_mode(
        self, session_id: str, mode_id: str, **kwargs: Any
    ) -> SetSessionModeResponse:
        if mode_id not in {"plan", "execute"}:
            raise RequestError.invalid_params({"modeId": mode_id})
        session = self._sessions.get(session_id)
        if session is None:
            value = await self._json("GET", f"/v1/acp/sessions/{session_id}")
            session = value.get("session")
        if not isinstance(session, dict) or type(session.get("version")) is not int:
            raise RequestError.resource_not_found(session_id)
        value = await self._json(
            "PUT",
            f"/v1/acp/sessions/{session_id}/mode",
            json={"expected_version": session["version"], "mode": mode_id},
        )
        updated = value.get("session")
        if not isinstance(updated, dict):
            raise RequestError.internal_error({"reason": "Invalid mode response"})
        self._sessions[session_id] = updated
        return SetSessionModeResponse()

    @staticmethod
    def _prompt_text(prompt: list[Any]) -> str:
        values = []
        for block in prompt:
            block_type = (
                block.get("type") if isinstance(block, dict) else getattr(block, "type", None)
            )
            if block_type != "text":
                raise RequestError.invalid_params(
                    {"reason": "Gateway ACP adapter supports text prompt blocks only"}
                )
            text = block.get("text") if isinstance(block, dict) else getattr(block, "text", None)
            if not isinstance(text, str):
                raise RequestError.invalid_params({"reason": "Invalid text prompt"})
            values.append(text)
        joined = "\n\n".join(values).strip()
        if not joined or len(joined) > 8_000:
            raise RequestError.invalid_params({"reason": "Prompt must contain 1-8000 characters"})
        return joined

    async def _update(self, session_id: str, update: Any) -> None:
        if self._conn is None:
            raise RequestError.internal_error({"reason": "ACP connection unavailable"})
        await self._conn.session_update(session_id=session_id, update=update)

    async def _message(self, session_id: str, text: str, message_id: str) -> None:
        await self._update(
            session_id,
            AgentMessageChunk(
                session_update="agent_message_chunk",
                content=TextContentBlock(type="text", text=text),
                message_id=message_id,
            ),
        )

    @staticmethod
    async def _events(response: httpx.Response):
        event: dict[str, str] = {}
        async for line in response.aiter_lines():
            if line == "":
                if "data" in event:
                    try:
                        value = json.loads(event["data"])
                    except (TypeError, ValueError):
                        value = None
                    if isinstance(value, dict):
                        yield event.get("id", ""), event.get("event", "message"), value
                event = {}
            elif line.startswith("id:"):
                event["id"] = line[3:].strip()
            elif line.startswith("event:"):
                event["event"] = line[6:].strip()
            elif line.startswith("data:"):
                value = line[5:].lstrip()
                event["data"] = event.get("data", "") + value

    async def _permission(self, session_id: str, run: ActiveRun, event: dict[str, Any]) -> None:
        action_id = str(event.get("action_id") or f"approval-{run.request_id}")
        title = f"核准工具：{str(event.get('tool', 'external action'))}"
        await self._update(
            session_id,
            ToolCallStart(
                session_update="tool_call",
                tool_call_id=action_id,
                title=title,
                kind="execute",
                status="pending",
                raw_input=event.get("arguments"),
            ),
        )
        if self._conn is None:
            return
        approved = False
        reason = "ACP client rejected or cancelled the one-time permission"
        try:
            decision = await self._conn.request_permission(
                session_id=session_id,
                tool_call=ToolCallUpdate(
                    tool_call_id=action_id,
                    title=title,
                    kind="execute",
                    status="pending",
                    raw_input=event.get("arguments"),
                ),
                options=[
                    PermissionOption(
                        option_id="allow-once", name="只核准這次工具與參數", kind="allow_once"
                    ),
                    PermissionOption(
                        option_id="reject-once", name="拒絕並終止這次執行", kind="reject_once"
                    ),
                ],
            )
            outcome = decision.outcome
            approved = (
                getattr(outcome, "outcome", "") == "selected"
                and getattr(outcome, "option_id", "") == "allow-once"
            )
            reason = "ACP human selected a one-time permission"
        except Exception:
            # Connection loss is never interpreted as approval.
            approved = False
        if run.cancelled.is_set():
            return
        try:
            response = await self._http.post(
                f"/v1/agent/runs/{run.request_id}/approval",
                json={"approved": approved, "reason": reason},
            )
        except httpx.HTTPError:
            response = None
        if response is None or response.status_code == 403:
            await self._message(
                session_id,
                "此啟動憑證不是人類身分，不能代為核准。請在已登入的 Gateway 控制台"
                "逐次決定；ACP 會繼續等待或可取消。",
                f"approval-help-{action_id}",
            )
            return
        if not response.is_success:
            raise RequestError.internal_error({"reason": "Gateway approval decision failed"})
        await self._update(
            session_id,
            ToolCallProgress(
                session_update="tool_call_update",
                tool_call_id=action_id,
                status="in_progress" if approved else "failed",
            ),
        )

    async def _consume(
        self, session_id: str, run: ActiveRun, response: httpx.Response
    ) -> tuple[str | None, bool]:
        stop: str | None = None
        waiting = False
        emitted = False
        async for event_id, event_name, event in self._events(response):
            if event_id:
                run.last_event_id = event_id
            kind = str(event.get("type", event_name))
            if kind == "token" and isinstance(event.get("content"), str):
                emitted = True
                await self._message(session_id, event["content"], run.request_id)
            elif kind == "tool_status":
                tool = str(event.get("tool", "tool"))
                status = str(event.get("status", "pending"))
                tool_id = f"{run.request_id}:{tool}"
                mapped = (
                    status
                    if status in {"pending", "in_progress", "completed", "failed"}
                    else "in_progress"
                )
                if tool_id not in run.started_tools:
                    run.started_tools.add(tool_id)
                    update = ToolCallStart(
                        session_update="tool_call",
                        tool_call_id=tool_id,
                        title=tool,
                        kind="execute",
                        status=mapped,
                    )
                else:
                    update = ToolCallProgress(
                        session_update="tool_call_update",
                        tool_call_id=tool_id,
                        title=tool,
                        kind="execute",
                        status=mapped,
                    )
                await self._update(session_id, update)
            elif kind == "approval_required":
                waiting = True
                await self._permission(session_id, run, event)
            elif kind == "completed":
                if not emitted and isinstance(event.get("answer"), str):
                    await self._message(session_id, event["answer"], run.request_id)
                stop = "end_turn"
            elif kind in {"cancelled", "approval_rejected"}:
                stop = "cancelled" if kind == "cancelled" else "refusal"
            elif kind == "error":
                await self._message(
                    session_id,
                    f"Gateway 執行失敗：{str(event.get('error', 'unknown'))}",
                    f"error-{run.request_id}",
                )
                stop = "refusal"
        return stop, waiting

    async def prompt(self, session_id: str, prompt: list[Any], **kwargs: Any) -> PromptResponse:
        if session_id in self._active:
            raise RequestError.invalid_request(
                {"reason": "A prompt is already active for this session"}
            )
        # Mode may have been changed by another authenticated control surface.
        # Refresh metadata before every turn, without transferring history.
        value = await self._json("GET", f"/v1/acp/sessions/{session_id}?include_history=false")
        session = value.get("session")
        if not isinstance(session, dict):
            raise RequestError.resource_not_found(session_id)
        self._sessions[session_id] = session
        query = self._prompt_text(prompt)
        if session.get("mode") == "plan":
            query = (
                "目前是規劃模式：只提出分析與可覆核計畫，不把切換模式視為執行或寫入核准。\n\n"
                + query
            )
        payload: dict[str, Any] = {"conversation_id": session_id, "query": query}
        if self.config.model:
            payload["model"] = self.config.model
        run = ActiveRun(cancelled=asyncio.Event())
        self._active[session_id] = run
        try:
            async with self._http.stream("POST", "/v1/agent/stream", json=payload) as response:
                if response.status_code == 401:
                    raise RequestError.auth_required()
                if not response.is_success:
                    raise RequestError.internal_error(
                        {"status": response.status_code, "reason": "Gateway prompt failed"}
                    )
                run.request_id = response.headers.get("x-request-id", "")
                if not run.request_id:
                    raise RequestError.internal_error({"reason": "Gateway run ID missing"})
                if run.cancelled.is_set():
                    await self._cancel_remote(run)
                stop, waiting = await self._consume(session_id, run, response)
            if waiting and stop is None and not run.cancelled.is_set():
                async with self._http.stream(
                    "GET",
                    f"/v1/agent/runs/{run.request_id}/events",
                    headers={"last-event-id": run.last_event_id},
                ) as replay:
                    if replay.is_success:
                        stop, _ = await self._consume(session_id, run, replay)
            if run.cancelled.is_set():
                return PromptResponse(stop_reason="cancelled")
            return PromptResponse(stop_reason=stop or "end_turn")
        except asyncio.CancelledError:
            run.cancelled.set()
            await asyncio.shield(self._cancel_remote(run))
            raise
        finally:
            self._active.pop(session_id, None)

    async def _cancel_remote(self, run: ActiveRun) -> None:
        if not run.request_id:
            return
        try:
            await self._http.post(f"/v1/agent/runs/{run.request_id}/cancel")
        except httpx.HTTPError:
            pass

    async def cancel(self, session_id: str, **kwargs: Any) -> None:
        run = self._active.get(session_id)
        if run is None:
            return
        run.cancelled.set()
        await self._cancel_remote(run)

    async def close(self) -> None:
        for run in self._active.values():
            run.cancelled.set()
            await self._cancel_remote(run)
        if self._owns_http:
            await self._http.aclose()


async def _main() -> None:
    agent = GatewayAcpAgent(AdapterConfig.from_env())
    try:
        await run_agent(agent)
    finally:
        await agent.close()


def main() -> None:
    try:
        asyncio.run(_main())
    except Exception as exc:
        # Never contaminate ACP stdout framing.
        sys.stderr.write(f"ai-ops-gateway ACP adapter failed: {type(exc).__name__}\n")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
