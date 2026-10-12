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
    usage = model._usage(
        {"input_tokens": 1_000, "output_tokens": 500, "total_tokens": 1_500}
    )
    assert usage == {
        "input_tokens": 1_000,
        "output_tokens": 500,
        "total_tokens": 1_500,
        "cost_usd": 0.006,
        "provider_reported": True,
    }
