from types import SimpleNamespace

import pytest

from app.core.config import Settings
from app.llm.client import ModelRouter, OpenAICompatibleModel


def test_model_router_selects_server_controlled_endpoint_alias() -> None:
    router = ModelRouter(
        Settings(
            llm_model="default-model",
            llm_routes_json=(
                '{"ollama-small":{"base_url":"http://ollama:11434/v1",'
                '"model":"qwen2.5:0.5b","api_key":"local"},'
                '"vllm-large":{"base_url":"http://vllm:8000/v1",'
                '"model":"Qwen/Qwen2.5-14B-Instruct","api_key":"local"}}'
            ),
        )
    )

    assert router.get("ollama-small").model_name == "qwen2.5:0.5b"
    assert router.get("vllm-large").model_name == "Qwen/Qwen2.5-14B-Instruct"
    assert [item["id"] for item in router.available_models()] == [
        "default-model",
        "ollama-small",
        "vllm-large",
    ]


def test_model_router_rejects_non_allow_listed_model() -> None:
    router = ModelRouter(Settings(llm_model="allowed-model"))
    with pytest.raises(ValueError, match="not allow-listed"):
        router.get("unapproved-model")


def test_provider_usage_is_priced_from_exact_reported_tokens() -> None:
    model = OpenAICompatibleModel(
        Settings(
            llm_input_cost_per_million=2.0,
            llm_output_cost_per_million=8.0,
        )
    )
    usage = model._usage({"input_tokens": 1_000, "output_tokens": 500, "total_tokens": 1_500})
    assert usage == {
        "input_tokens": 1_000,
        "output_tokens": 500,
        "total_tokens": 1_500,
        "cost_usd": 0.006,
        "provider_reported": True,
    }


@pytest.mark.parametrize(
    "metadata",
    [
        None,
        {},
        {"total_tokens": 18},
        {"input_tokens": 10, "output_tokens": 8},
        {"input_tokens": -1, "output_tokens": 8, "total_tokens": 7},
        {"input_tokens": 10, "output_tokens": -8, "total_tokens": 2},
        {"input_tokens": 10, "output_tokens": 8, "total_tokens": -18},
        {"input_tokens": 10, "output_tokens": 8, "total_tokens": 1},
        {"input_tokens": 10, "output_tokens": 8, "total_tokens": 19},
        {"input_tokens": 10.0, "output_tokens": 8, "total_tokens": 18},
        {"input_tokens": True, "output_tokens": 8, "total_tokens": 9},
        {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
    ],
)
def test_invalid_provider_usage_is_unknown_instead_of_refunding_tokens(metadata):
    model = OpenAICompatibleModel(Settings())
    usage = model._usage(metadata, has_output=True)
    assert usage["provider_reported"] is False
    assert usage["total_tokens"] == 0
    assert usage["cost_usd"] == 0


@pytest.mark.parametrize(
    "final_usage, expected",
    [
        ({"input_tokens": 10, "output_tokens": 8, "total_tokens": 18}, 18),
        ({"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}, None),
        ({"total_tokens": 18}, None),
        ({"input_tokens": -10, "output_tokens": -8, "total_tokens": -18}, None),
    ],
)
async def test_stream_usage_is_validated_after_all_output(final_usage, expected):
    class Endpoint:
        async def astream(self, messages):
            yield SimpleNamespace(
                content="",
                usage_metadata={
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0,
                },
            )
            yield SimpleNamespace(content="nonempty answer", usage_metadata=None)
            yield SimpleNamespace(content="", usage_metadata=final_usage)

    model = object.__new__(OpenAICompatibleModel)
    model._model = Endpoint()
    model._input_cost = model._output_cost = 0
    chunks = [chunk async for chunk in model.stream_result("system", "prompt")]
    assert "".join(chunk.text for chunk in chunks) == "nonempty answer"
    known = [chunk.usage for chunk in chunks if chunk.usage.get("provider_reported")]
    assert [usage["total_tokens"] for usage in known] == ([] if expected is None else [expected])
