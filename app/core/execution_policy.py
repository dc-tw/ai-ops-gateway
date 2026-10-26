"""Delegated tool scope and per-Goal model accounting for unattended execution."""

from __future__ import annotations

import inspect
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any
from urllib.parse import urlsplit

from app.llm.client import ModelChunk, ModelResult, OpenAICompatibleModel, validated_token_usage
from app.tools.registry import (
    LocalBrowserToolExecutor,
    MCPBrowserToolExecutor,
    RemoteBrowserToolExecutor,
    ToolExecutor,
    ToolSpec,
)


class TokenBudgetExceeded(RuntimeError):
    pass


class ToolReconciliationRequired(RuntimeError):
    """An earlier external write has an uncertain checkpoint outcome."""


class BudgetedModel:
    """Charge every call, including planning and interrupted/failed generation.

    Provider token counts reconcile successful calls. Before a call, UTF-8 byte
    length plus a chat-format allowance reserves input and bounds max generation.
    Interrupted calls retain that reservation; they cannot become free on resume.
    """

    def __init__(
        self,
        model: Any,
        token_budget: int,
        *,
        existing_usage: dict[str, Any] | None = None,
        on_usage: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
        max_output_tokens: int = 1_024,
        persona: str = "",
    ) -> None:
        self.model = model
        self.token_budget = token_budget
        self.max_output_tokens = max_output_tokens
        self.on_usage = on_usage
        self.persona = persona
        self.usage: dict[str, Any] = {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "cost_usd": 0.0,
            "model_calls": 0,
            "provider_reported_calls": 0,
            "provider_reported": False,
            "policy_charged_tokens": 0,
            **(existing_usage or {}),
        }

    async def _publish(self) -> None:
        if self.on_usage:
            await self.on_usage(dict(self.usage))

    async def _begin(self, system: str, prompt: str) -> tuple[dict[str, Any], int, int]:
        before = dict(self.usage)
        used = int(before.get("policy_charged_tokens", before.get("total_tokens", 0)))
        input_bound = len((system + prompt).encode("utf-8")) + 256
        cap = min(self.max_output_tokens, self.token_budget - used - input_bound)
        if cap < 1:
            raise TokenBudgetExceeded("Goal token budget cannot fund another model call")
        self.usage["model_calls"] = int(before.get("model_calls", 0)) + 1
        self.usage["policy_charged_tokens"] = used + input_bound + cap
        self.usage["provider_reported"] = False
        await self._publish()
        return before, input_bound, cap

    async def _finish(
        self,
        before: dict[str, Any],
        input_bound: int,
        output: str,
        reported: dict[str, Any],
        output_cap: int,
    ) -> dict[str, Any]:
        counts = validated_token_usage(reported, has_output=bool(output))
        is_reported = bool(reported.get("provider_reported")) and counts is not None
        incoming = counts["input_tokens"] if is_reported else input_bound
        outgoing = (
            counts["output_tokens"] if is_reported else max(output_cap, len(output.encode("utf-8")))
        )
        total = counts["total_tokens"] if is_reported else (incoming + outgoing)
        delta = {
            "input_tokens": incoming,
            "output_tokens": outgoing,
            "total_tokens": total,
            "cost_usd": float(reported.get("cost_usd", 0)) if is_reported else 0.0,
            "provider_reported": is_reported,
        }
        for key in ("input_tokens", "output_tokens", "total_tokens", "cost_usd"):
            self.usage[key] = before.get(key, 0) + delta[key]
        previous_charged = int(before.get("policy_charged_tokens", before.get("total_tokens", 0)))
        self.usage["policy_charged_tokens"] = previous_charged + total
        reported_calls = int(before.get("provider_reported_calls", 0)) + int(is_reported)
        self.usage["provider_reported_calls"] = reported_calls
        self.usage["provider_reported"] = reported_calls == self.usage["model_calls"]
        await self._publish()
        if self.usage["policy_charged_tokens"] > self.token_budget:
            raise TokenBudgetExceeded("Goal token budget exhausted")
        return delta

    async def complete(self, system: str, prompt: str) -> str:
        return (await self.complete_result(system, prompt)).text

    async def complete_result(self, system: str, prompt: str) -> ModelResult:
        if self.persona:
            system += "\n\nOperator-configured Agent role and persona:\n" + self.persona
        before, input_bound, cap = await self._begin(system, prompt)
        if isinstance(self.model, OpenAICompatibleModel):
            result = await self.model.complete_result(system, prompt, max_output_tokens=cap)
        elif getattr(self.model, "complete_result", None):
            result = await self.model.complete_result(system, prompt)
        else:
            result = ModelResult(await self.model.complete(system, prompt), {})
        delta = await self._finish(before, input_bound, result.text, result.usage, cap)
        return ModelResult(result.text, delta)

    async def stream(self, system: str, prompt: str) -> AsyncIterator[str]:
        async for chunk in self.stream_result(system, prompt):
            if chunk.text:
                yield chunk.text

    async def stream_result(self, system: str, prompt: str) -> AsyncIterator[ModelChunk]:
        if self.persona:
            system += "\n\nOperator-configured Agent role and persona:\n" + self.persona
        before, input_bound, cap = await self._begin(system, prompt)
        if isinstance(self.model, OpenAICompatibleModel):
            iterator = self.model.stream_result(system, prompt, max_output_tokens=cap)
        elif getattr(self.model, "stream_result", None):
            iterator = self.model.stream_result(system, prompt)
        else:
            iterator = self.model.stream(system, prompt)
        text: list[str] = []
        reported: dict[str, Any] = {}
        try:
            async for chunk in iterator:
                content = str(getattr(chunk, "text", chunk))
                metadata = getattr(chunk, "usage", None)
                if isinstance(metadata, dict) and metadata.get("provider_reported"):
                    reported = dict(metadata)
                text.append(content)
                # An adapter without a generation limit cannot stream beyond the
                # reservation; successful provider calls are bounded at request time.
                if not isinstance(self.model, OpenAICompatibleModel) and (
                    len("".join(text).encode("utf-8")) > cap
                ):
                    raise TokenBudgetExceeded("Model output exceeded reserved token budget")
                if content:
                    yield ModelChunk(content, {})
            delta = await self._finish(before, input_bound, "".join(text), reported, cap)
            yield ModelChunk("", delta)
        finally:
            close = getattr(iterator, "aclose", None)
            if close:
                result = close()
                if inspect.isawaitable(result):
                    await result


class PolicyToolExecutor:
    """Enforce server-saved job tool scope independently of model instructions."""

    def __init__(
        self,
        executor: ToolExecutor,
        policy: dict[str, Any],
        *,
        runtime: Any = None,
        tenant_id: str = "default",
        goal_id: str = "",
        execution_owner: str | None = None,
    ) -> None:
        self.executor = executor
        self.allowed = frozenset(policy.get("allowed_tools", [s.name for s in executor.specs()]))
        self.domains = frozenset(
            str(d).lower().rstrip(".") for d in policy.get("allowed_domains", [])
        )
        self.runtime = runtime
        self.tenant_id = tenant_id
        self.goal_id = goal_id
        self.execution_owner = execution_owner
        self.sessions: dict[str, str] = {}

    def specs(self) -> list[ToolSpec]:
        return [s for s in self.executor.specs() if s.name in self.allowed]

    def _url(self, value: str) -> None:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise PermissionError("Tool URL must be HTTP(S)")
        if self.domains and parsed.hostname.lower().rstrip(".") not in self.domains:
            raise PermissionError("Tool domain is outside the delegated job policy")

    def _session_key(self, session_id: str) -> str:
        return self.runtime._tenant_key(
            self.tenant_id, f"goal:{self.goal_id}:browser-session:{session_id}"
        )

    async def invoke(self, name: str, arguments: dict[str, Any]) -> dict[str, object]:
        if name not in self.allowed:
            raise PermissionError("Tool is outside the delegated job policy")
        if "url" in arguments:
            self._url(str(arguments["url"]))
        session_id = str(arguments.get("session_id", ""))
        if session_id:
            known = self.sessions.get(session_id)
            if self.runtime is not None and hasattr(self.runtime, "_client"):
                known = await self.runtime._client.get(self._session_key(session_id))
            if not known:
                raise PermissionError("Browser session does not belong to this Goal")
            self._url(known)
        await self._check_owner()
        delegated = dict(arguments)
        if name in {"scrape_dynamic_page", "browser_start"} and isinstance(
            self.executor,
            (LocalBrowserToolExecutor, RemoteBrowserToolExecutor, MCPBrowserToolExecutor),
        ):
            # The model cannot widen the context's network policy.
            delegated["allowed_domains"] = list(self.domains)
        result = await self.executor.invoke(name, delegated)
        if result.get("url"):
            self._url(str(result["url"]))
        if name == "browser_start" and result.get("session_id"):
            new_id = str(result["session_id"])
            url = str(result.get("url", arguments.get("url", "")))
            self.sessions[new_id] = url
            if self.runtime is not None and hasattr(self.runtime, "_client"):
                await self.runtime._client.set(self._session_key(new_id), url, ex=86_400)
        return result

    async def _check_owner(self) -> None:
        if self.execution_owner is None:
            return
        goal = await self.runtime.get_goal(self.goal_id, self.tenant_id)
        if (
            not goal
            or goal.get("status") != "running"
            or (
                goal.get("execution_owner") != self.execution_owner
                or float(goal.get("execution_lease_until", 0)) <= time.time()
            )
        ):
            raise PermissionError("Goal execution ownership was revoked")
        if goal.get("agent_config"):
            from app.agents import AgentStore

            agent = await AgentStore(self.runtime).get(self.tenant_id, goal["agent_id"])
            if not agent or not agent["enabled"]:
                raise PermissionError("Agent was disabled")
        if goal.get("parent_goal_id"):
            parent = await self.runtime.get_goal(goal["parent_goal_id"], self.tenant_id)
            if not parent or parent.get("status") in {"cancelled", "failed", "completed"}:
                raise PermissionError("Chief task is no longer active")

    async def invoke_action(
        self, name: str, arguments: dict[str, Any], action_id: str
    ) -> dict[str, object]:
        spec = next((item for item in self.specs() if item.name == name), None)
        if spec is None:
            raise PermissionError("Tool is outside the delegated job policy")
        if not spec.requires_approval or self.runtime is None:
            return await self.invoke(name, arguments)
        await self._check_owner()
        # External effects cannot be rolled back with a Redis transaction. A
        # prior attempt with no committed graph checkpoint requires operator
        # reconciliation instead of blindly repeating the side effect.
        key = self.runtime._tenant_key(
            self.tenant_id, f"goal:{self.goal_id}:tool-receipt:{action_id}"
        )
        started = json.dumps({"status": "started", "tool": name, "action_id": action_id})
        if not await self.runtime._client.set(key, started, nx=True):
            raise ToolReconciliationRequired(
                "This action was already attempted; inspect the external result before retrying"
            )
        result = await self.invoke(name, arguments)
        # Receipts contain no raw arguments or unscanned tool content.
        await self.runtime._client.set(
            key, json.dumps({"status": "completed", "tool": name, "action_id": action_id})
        )
        return result
