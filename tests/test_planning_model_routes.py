"""Planning captures configured routing identity without an inference client or RPC."""

import json
import re

import pytest

import app.llm.client as clients
from app.core.config import Settings
from app.llm.client import ModelRouter


def settings(**changes):
    return Settings(
        _env_file=None,
        **(
            {
                "llm_model": "default-model",
                "llm_base_url": "http://default-backend:8000/v1",
                "llm_temperature": 0.1,
                "llm_max_output_tokens": 2_048,
                "llm_routes_json": "",
                "llm_allow_arbitrary_models": False,
                "llm_api_key": "PRIVATE_API_KEY_SENTINEL",
            }
            | changes
        ),
    )


def routes(**values):
    return json.dumps(values)


def fingerprint(**changes):
    return ModelRouter(settings(**changes)).planning_route()["route_fingerprint"]


def test_planning_route_constructs_no_adapter_or_inference_client(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Planning attempted to construct an inference client")

    monkeypatch.setattr(clients, "OpenAICompatibleModel", forbidden)
    monkeypatch.setattr(clients, "ChatOpenAI", forbidden)
    router = ModelRouter(
        settings(
            llm_routes_json=routes(
                analysis={"base_url": "http://analysis-backend:8000/v1", "model": "actual-model"}
            )
        )
    )
    snapshot = router.planning_route("analysis")
    assert snapshot["alias"] == "analysis"
    assert snapshot["resolved_model"] == "actual-model"
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", snapshot["route_fingerprint"])
    assert snapshot["version_disclosure"]["actual_model_version"] == "unknown"
    assert router._models == {}


@pytest.mark.parametrize("allow_arbitrary", [False, True])
@pytest.mark.parametrize(
    "selection", [None, "", "default-model", " analysis ", "other-model", "   ", "x" * 201]
)
def test_planning_and_execution_share_selection_rules(monkeypatch, allow_arbitrary, selection):
    class Adapter:
        def __init__(self, configured, *, model, **kwargs):
            self.model_name = model

    monkeypatch.setattr(clients, "OpenAICompatibleModel", Adapter)
    router = ModelRouter(
        settings(
            llm_allow_arbitrary_models=allow_arbitrary,
            llm_routes_json=routes(
                analysis={"base_url": "http://analysis-backend:8000/v1", "model": "actual-model"}
            ),
        )
    )
    try:
        snapshot = router.planning_route(selection)
    except ValueError as planning_error:
        with pytest.raises(ValueError) as execution_error:
            router.get(selection)
        assert str(execution_error.value) == str(planning_error)
    else:
        assert router.get(selection).model_name == snapshot["resolved_model"]
        assert snapshot["alias"] == (selection or "default-model").strip()


def test_default_selection_resolves_an_alias_that_shadows_the_default_name():
    router = ModelRouter(
        settings(
            llm_routes_json=routes(
                **{
                    "default-model": {
                        "base_url": "http://configured-default:8000/v1",
                        "model": "resolved-default",
                    }
                }
            )
        )
    )
    assert router.planning_route()["alias"] == "default-model"
    assert router.planning_route()["resolved_model"] == "resolved-default"
    assert router.planning_route() == router.planning_route("")


def test_aliases_for_the_same_route_share_identity():
    route = {"base_url": "http://shared-backend:8000/v1", "model": "shared-model"}
    router = ModelRouter(settings(llm_routes_json=routes(first=route, second=route)))
    first, second = router.planning_route("first"), router.planning_route("second")
    assert first["alias"] != second["alias"]
    assert first["resolved_model"] == second["resolved_model"]
    assert first["route_fingerprint"] == second["route_fingerprint"]


@pytest.mark.parametrize(
    "change",
    [
        {"llm_model": "another-model"},
        {"llm_base_url": "http://another-backend:8000/v1"},
        {"llm_base_url": "http://default-backend:9000/v1"},
        {"llm_base_url": "http://default-backend:8000/deployment-b/v1"},
        {"llm_base_url": "http://default-backend:8000/v1//"},
        {"llm_base_url": "https://default-backend:8000/v1"},
        {"llm_temperature": 0.7},
        {"llm_max_output_tokens": 1_024},
    ],
)
def test_backend_model_and_supported_inference_changes_have_distinct_identities(change):
    assert fingerprint(**change) != fingerprint()


@pytest.mark.parametrize(
    "change",
    [
        {"llm_api_key": "ROTATED_PRIVATE_API_KEY_SENTINEL"},
        {"llm_input_cost_per_million": 99.9, "llm_output_cost_per_million": 123.4},
        {"llm_timeout_seconds": 300},
        {"request_token_budget": 32_000, "tenant_daily_token_budget": 9_000_000},
    ],
)
def test_credentials_pricing_timeout_and_budget_do_not_change_identity(change):
    assert fingerprint(**change) == fingerprint()


def test_route_credentials_and_prices_are_not_disclosed_or_hashed():
    def route(key, price, url):
        return {
            "base_url": url,
            "model": "configured-model",
            "api_key": key,
            "input_cost_per_million": price,
            "output_cost_per_million": price * 2,
        }

    first = ModelRouter(
        settings(
            llm_routes_json=routes(
                analysis=route(
                    "PRIVATE_ROUTE_KEY_SENTINEL",
                    12.345,
                    "https://internal-backend:8443/v1",
                )
            )
        )
    ).planning_route("analysis")
    second = ModelRouter(
        settings(
            llm_routes_json=routes(
                analysis=route(
                    "ROTATED_ROUTE_KEY_SENTINEL",
                    987.654,
                    "https://internal-backend:8443/v1",
                )
            )
        )
    ).planning_route("analysis")
    assert first == second
    serialized = json.dumps(first)
    for private in (
        "PRIVATE_",
        "ROTATED_",
        "internal-backend",
        "https://",
        "12.345",
        "987.654",
        "api_key",
        "cost_per_million",
    ):
        assert private not in serialized


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://user:PRIVATE_PASSWORD_SENTINEL@internal-backend/v1",
        "https://internal-backend/v1?api_key=PRIVATE_QUERY_SENTINEL",
        "https://internal-backend/v1#PRIVATE_FRAGMENT_SENTINEL",
        "https://internal-backend:PRIVATE_PORT_SENTINEL/v1",
    ],
)
def test_ambiguous_or_secret_bearing_endpoints_are_rejected_without_disclosing_url(endpoint):
    router = ModelRouter(settings(llm_base_url=endpoint))
    with pytest.raises(ValueError) as error:
        router.planning_route()
    assert str(error.value) == (
        "Configured inference endpoint cannot provide a safe planning identity"
    )
    assert "PRIVATE_" not in str(error.value)
    assert "internal-backend" not in str(error.value)


def test_endpoint_equivalences_do_not_split_identity():
    assert fingerprint(llm_base_url="http://BACKEND/v1/") == fingerprint(
        llm_base_url="http://backend:80/v1"
    )


def test_configured_model_name_is_not_reported_as_a_verified_version():
    snapshot = ModelRouter(settings(llm_model="model-2026-10-26-revision-42")).planning_route()
    disclosure = snapshot["version_disclosure"]
    assert disclosure["actual_model_version"] == "unknown"
    assert "unverified" in disclosure["limitation"]
    assert set(snapshot) == {"alias", "resolved_model", "route_fingerprint", "version_disclosure"}
