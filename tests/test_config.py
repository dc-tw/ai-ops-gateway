import pytest
from pydantic import ValidationError

from app.core.config import Settings


def test_allowed_domains_accept_comma_separated_environment_value(monkeypatch) -> None:
    monkeypatch.setenv("PLAYWRIGHT_ALLOWED_DOMAINS", "example.com, docs.example.com")
    settings = Settings(_env_file=None)
    assert settings.playwright_allowed_domains == ["example.com", "docs.example.com"]


def test_invalid_content_policy_regex_fails_at_startup() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, content_blocked_patterns=["["])


def test_atr_rule_lists_accept_comma_separated_environment_values(monkeypatch) -> None:
    monkeypatch.setenv("ATR_BLOCKING_RULE_IDS", "ATR-2026-00001,ATR-2026-00002")
    monkeypatch.setenv("ATR_BLOCKING_EXCLUDED_RULE_IDS", "ATR-2026-00003")
    settings = Settings(_env_file=None)

    assert settings.atr_blocking_rule_ids == ["ATR-2026-00001", "ATR-2026-00002"]
    assert settings.atr_blocking_excluded_rule_ids == ["ATR-2026-00003"]
