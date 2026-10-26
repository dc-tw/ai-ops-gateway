import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlsplit

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from app.core.config import Settings


class ModelClient(Protocol):
    async def complete(self, system: str, prompt: str) -> str: ...

    def stream(self, system: str, prompt: str) -> AsyncIterator[str]: ...


@dataclass(slots=True)
class ModelResult:
    text: str
    usage: dict[str, float | int]


@dataclass(slots=True)
class ModelChunk:
    text: str
    usage: dict[str, float | int]


def validated_token_usage(metadata: object, *, has_output: bool = False) -> dict[str, int] | None:
    """Accept only complete, nonnegative and internally consistent usage."""
    if not isinstance(metadata, dict):
        return None
    fields = ("input_tokens", "output_tokens", "total_tokens")
    if any(type(metadata.get(field)) is not int or metadata[field] < 0 for field in fields):
        return None
    counts = {field: metadata[field] for field in fields}
    if counts["total_tokens"] != counts["input_tokens"] + counts["output_tokens"]:
        return None
    if has_output and counts["total_tokens"] == 0:
        return None
    return counts


def _text(content: object) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
        return "".join(parts)
    return str(content)


class OpenAICompatibleModel:
    """One adapter for Ollama, vLLM, and other OpenAI-compatible inference servers."""

    def __init__(
        self,
        settings: Settings,
        *,
        model: str | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
        input_cost_per_million: float | None = None,
        output_cost_per_million: float | None = None,
    ) -> None:
        self.model_name = model or settings.llm_model
        self._input_cost = (
            settings.llm_input_cost_per_million
            if input_cost_per_million is None
            else input_cost_per_million
        )
        self._output_cost = (
            settings.llm_output_cost_per_million
            if output_cost_per_million is None
            else output_cost_per_million
        )
        self._model = ChatOpenAI(
            model=self.model_name,
            base_url=base_url or settings.llm_base_url,
            api_key=api_key or settings.llm_api_key,
            temperature=settings.llm_temperature,
            timeout=settings.llm_timeout_seconds,
            max_tokens=settings.llm_max_output_tokens,
            max_retries=0,  # LangGraph owns retry policy and observability.
            streaming=True,
            stream_usage=True,
        )

    async def complete(self, system: str, prompt: str) -> str:
        return (await self.complete_result(system, prompt)).text

    def _usage(self, metadata: object, *, has_output: bool = False) -> dict[str, float | int]:
        counts = validated_token_usage(metadata, has_output=has_output)
        value = counts or {}
        input_tokens = value.get("input_tokens", 0)
        output_tokens = value.get("output_tokens", 0)
        total_tokens = value.get("total_tokens", 0)
        cost = (input_tokens * self._input_cost + output_tokens * self._output_cost) / 1_000_000
        return {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "cost_usd": round(cost, 8),
            "provider_reported": counts is not None,
        }

    async def complete_result(
        self, system: str, prompt: str, *, max_output_tokens: int | None = None
    ) -> ModelResult:
        model = self._model.bind(max_tokens=max_output_tokens) if max_output_tokens else self._model
        response = await model.ainvoke(
            [SystemMessage(content=system), HumanMessage(content=prompt)]
        )
        text = _text(response.content).strip()
        return ModelResult(
            text=text,
            usage=self._usage(getattr(response, "usage_metadata", None), has_output=bool(text)),
        )

    async def stream(self, system: str, prompt: str) -> AsyncIterator[str]:
        async for chunk in self.stream_result(system, prompt):
            if chunk.text:
                yield chunk.text

    async def stream_result(
        self, system: str, prompt: str, *, max_output_tokens: int | None = None
    ) -> AsyncIterator[ModelChunk]:
        model = self._model.bind(max_tokens=max_output_tokens) if max_output_tokens else self._model
        last_metadata = None
        has_output = False
        async for chunk in model.astream(
            [SystemMessage(content=system), HumanMessage(content=prompt)]
        ):
            text = _text(chunk.content)
            has_output = has_output or bool(text)
            metadata = getattr(chunk, "usage_metadata", None)
            if metadata:
                last_metadata = metadata
            if text:
                yield ModelChunk(text=text, usage={})
        # Check usage against the complete output; an early all-zero chunk must
        # not become a free completed stream once text arrives afterwards.
        usage = self._usage(last_metadata, has_output=has_output)
        if usage["provider_reported"]:
            yield ModelChunk(text="", usage=usage)


class ModelRouter:
    """Select a model per request while keeping the inference endpoint server-controlled."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._models: dict[str, OpenAICompatibleModel] = {}
        self._routes: dict[str, dict[str, str]] = {}
        if settings.llm_routes_json.strip():
            decoded = json.loads(settings.llm_routes_json)
            if not isinstance(decoded, dict):
                raise ValueError("LLM_ROUTES_JSON must be a JSON object")
            for alias, route in decoded.items():
                if not isinstance(alias, str) or not isinstance(route, dict):
                    raise ValueError("Each LLM route must map an alias to an object")
                base_url = str(route.get("base_url", "")).strip()
                model = str(route.get("model", "")).strip()
                if not base_url.startswith(("http://", "https://")) or not model:
                    raise ValueError(f"Invalid LLM route: {alias}")
                self._routes[alias] = {
                    "base_url": base_url,
                    "model": model,
                    "api_key": str(route.get("api_key", settings.llm_api_key)),
                    "input_cost_per_million": str(
                        route.get("input_cost_per_million", settings.llm_input_cost_per_million)
                    ),
                    "output_cost_per_million": str(
                        route.get("output_cost_per_million", settings.llm_output_cost_per_million)
                    ),
                }

    def _selection(self, model: str | None) -> str:
        selection = (model or self._settings.llm_model).strip()
        if not selection or len(selection) > 200:
            raise ValueError("Invalid model name")
        allowed = {self._settings.llm_model, *self._routes}
        if not self._settings.llm_allow_arbitrary_models and selection not in allowed:
            raise ValueError(f"Model is not allow-listed: {selection}")
        return selection

    def planning_route(self, model: str | None = None) -> dict[str, Any]:
        """Describe configured inference identity without creating a client or making an RPC.

        The endpoint and model identifier do not establish a weights, deployment,
        tokenizer or runtime version. Credentials, pricing and request budgets are
        excluded from identity. Endpoints with URL credentials, query or fragment
        cannot provide an unambiguous identity without risking secret disclosure.
        """
        selection = self._selection(model)
        route = self._routes.get(selection)
        resolved_model = route["model"] if route else selection
        base_url = route["base_url"] if route else self._settings.llm_base_url
        try:
            endpoint = urlsplit(base_url)
            if (
                endpoint.scheme.lower() not in {"http", "https"}
                or not endpoint.hostname
                or endpoint.username is not None
                or endpoint.password is not None
                or endpoint.query
                or endpoint.fragment
            ):
                raise ValueError("Unsupported planning endpoint")
            endpoint_identity = {
                "scheme": endpoint.scheme.lower(),
                "hostname": (endpoint.hostname or "").lower(),
                "port": (
                    endpoint.port
                    if endpoint.port is not None
                    else {"http": 80, "https": 443}.get(endpoint.scheme.lower())
                ),
                "path": endpoint.path if endpoint.path.endswith("/") else endpoint.path + "/",
            }
        except ValueError:
            raise ValueError(
                "Configured inference endpoint cannot provide a safe planning identity"
            ) from None
        identity = {
            "endpoint": endpoint_identity,
            "model": resolved_model,
            "inference": {
                "temperature": self._settings.llm_temperature,
                "max_output_tokens": self._settings.llm_max_output_tokens,
            },
        }
        canonical = json.dumps(identity, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return {
            "alias": selection,
            "resolved_model": resolved_model,
            "route_fingerprint": "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
            "version_disclosure": {
                "actual_model_version": "unknown",
                "identity_scope": "configured_endpoint_model_and_inference_settings",
                "limitation": (
                    "Configured identity only; weights, deployment, tokenizer and runtime versions "
                    "are unverified. Only HTTP(S) endpoints without userinfo, query or fragment "
                    "are supported."
                ),
            },
        }

    def get(self, model: str | None = None) -> OpenAICompatibleModel:
        selection = self._selection(model)
        if selection not in self._models:
            route = self._routes.get(selection)
            self._models[selection] = OpenAICompatibleModel(
                self._settings,
                model=route["model"] if route else selection,
                base_url=route["base_url"] if route else None,
                api_key=route["api_key"] if route else None,
                input_cost_per_million=(float(route["input_cost_per_million"]) if route else None),
                output_cost_per_million=(
                    float(route["output_cost_per_million"]) if route else None
                ),
            )
        return self._models[selection]

    @property
    def default_model(self) -> str:
        return self._settings.llm_model

    def available_models(self) -> list[dict[str, str]]:
        models = [{"id": self._settings.llm_model, "model": self._settings.llm_model}]
        models.extend(
            {"id": alias, "model": route["model"]} for alias, route in sorted(self._routes.items())
        )
        return models
