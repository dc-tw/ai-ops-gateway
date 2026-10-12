import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol

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

    def _usage(self, metadata: object) -> dict[str, float | int]:
        value = metadata if isinstance(metadata, dict) else {}
        input_tokens = int(value.get("input_tokens", 0))
        output_tokens = int(value.get("output_tokens", 0))
        total_tokens = int(value.get("total_tokens", input_tokens + output_tokens))
        cost = (
            input_tokens * self._input_cost + output_tokens * self._output_cost
        ) / 1_000_000
        return {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "cost_usd": round(cost, 8),
            "provider_reported": bool(value),
        }

    async def complete_result(self, system: str, prompt: str) -> ModelResult:
        response = await self._model.ainvoke(
            [SystemMessage(content=system), HumanMessage(content=prompt)]
        )
        return ModelResult(
            text=_text(response.content).strip(),
            usage=self._usage(getattr(response, "usage_metadata", None)),
        )

    async def stream(self, system: str, prompt: str) -> AsyncIterator[str]:
        async for chunk in self.stream_result(system, prompt):
            if chunk.text:
                yield chunk.text

    async def stream_result(self, system: str, prompt: str) -> AsyncIterator[ModelChunk]:
        async for chunk in self._model.astream(
            [SystemMessage(content=system), HumanMessage(content=prompt)]
        ):
            text = _text(chunk.content)
            usage = self._usage(getattr(chunk, "usage_metadata", None))
            if text or usage["provider_reported"]:
                yield ModelChunk(text=text, usage=usage)


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
                        route.get(
                            "input_cost_per_million", settings.llm_input_cost_per_million
                        )
                    ),
                    "output_cost_per_million": str(
                        route.get(
                            "output_cost_per_million", settings.llm_output_cost_per_million
                        )
                    ),
                }

    def get(self, model: str | None = None) -> OpenAICompatibleModel:
        selection = (model or self._settings.llm_model).strip()
        if not selection or len(selection) > 200:
            raise ValueError("Invalid model name")
        allowed = {self._settings.llm_model, *self._routes}
        if not self._settings.llm_allow_arbitrary_models and selection not in allowed:
            raise ValueError(f"Model is not allow-listed: {selection}")
        if selection not in self._models:
            route = self._routes.get(selection)
            self._models[selection] = OpenAICompatibleModel(
                self._settings,
                model=route["model"] if route else selection,
                base_url=route["base_url"] if route else None,
                api_key=route["api_key"] if route else None,
                input_cost_per_million=(
                    float(route["input_cost_per_million"]) if route else None
                ),
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
            {"id": alias, "model": route["model"]}
            for alias, route in sorted(self._routes.items())
        )
        return models
