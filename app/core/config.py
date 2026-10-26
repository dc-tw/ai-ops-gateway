import re
from functools import lru_cache
from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    app_env: str = "development"
    api_key: str | None = None
    api_keys_json: str = ""
    oidc_jwks_url: str = ""
    oidc_issuer: str = ""
    oidc_audience: str = ""
    oidc_tenant_claim: str = "tenant_id"
    oidc_role_claim: str = "role"
    log_level: str = "INFO"
    otel_service_name: str = "ai-ops-gateway"
    otel_exporter_otlp_endpoint: str = ""

    llm_base_url: str = "http://localhost:11434/v1"
    llm_api_key: str = "local-only"
    llm_model: str = "qwen2.5:7b-instruct"
    llm_routes_json: str = ""
    llm_allow_arbitrary_models: bool = False
    llm_temperature: float = 0.1
    llm_timeout_seconds: float = 120.0
    llm_max_output_tokens: int = Field(default=2_048, ge=64, le=32_768)
    llm_input_cost_per_million: float = Field(default=0.0, ge=0)
    llm_output_cost_per_million: float = Field(default=0.0, ge=0)

    redis_url: str = "redis://localhost:6379/0"
    redis_stream_maxlen: int = 10_000
    postgres_dsn: str = Field(default="", repr=False)
    postgres_source_id: str = Field(default="gateway", pattern=r"^[A-Za-z0-9._-]{1,64}$")
    postgres_history_mode: str = Field(default="shadow", pattern=r"^(shadow|postgres)$")
    postgres_projector_poll_seconds: float = Field(default=1.0, ge=0.1, le=30)
    postgres_outbox_lease_ms: int = Field(default=5_000, ge=1_000, le=300_000)
    langgraph_checkpointing: bool = True
    checkpoint_ttl_minutes: int = Field(default=10_080, ge=60)
    conversation_max_turns: int = Field(default=40, ge=2, le=200)
    conversation_ttl_seconds: int = Field(default=604_800, ge=60)
    conversation_context_max_tokens: int = Field(default=6_000, ge=256, le=64_000)

    playwright_headless: bool = True
    playwright_timeout_ms: int = 30_000
    playwright_max_content_chars: int = 50_000
    playwright_allowed_domains: Annotated[list[str], NoDecode] = Field(default_factory=list)
    browser_session_ttl_seconds: int = Field(default=1_800, ge=60, le=86_400)
    browser_max_sessions: int = Field(default=20, ge=1, le=200)
    browser_worker_url: str = ""
    browser_worker_token: str = ""
    agent_tool_transport: str = Field(default="local", pattern=r"^(local|mcp)$")
    autonomous_tool_planning: bool = True
    agent_max_tool_rounds: int = Field(default=8, ge=1, le=20)
    agent_default_deadline_seconds: float = Field(default=300.0, ge=1, le=3_600)

    agent_max_retries: int = Field(default=2, ge=0, le=5)
    sse_heartbeat_seconds: float = Field(default=15.0, ge=1.0, le=60.0)
    goal_worker_poll_seconds: float = Field(default=2.0, ge=0.1, le=30.0)
    goal_consumer_group: str = "goal-workers"
    goal_lease_ms: int = Field(default=60_000, ge=5_000)
    automation_poll_seconds: float = Field(default=2.0, ge=0.1, le=60.0)
    automation_default_max_runs_per_day: int = Field(default=24, ge=1, le=100_000)
    automation_default_token_budget: int = Field(default=20_000, ge=256, le=10_000_000)
    automation_default_approval_timeout_seconds: int = Field(default=86_400, ge=60, le=2_592_000)
    automation_webhook_secrets_json: str = ""
    tenant_requests_per_minute: int = Field(default=60, ge=1)
    tenant_max_concurrent_runs: int = Field(default=4, ge=1)
    request_token_budget: int = Field(default=16_000, ge=256)
    tenant_daily_token_budget: int = Field(default=1_000_000, ge=1_000)
    pii_redaction: bool = True
    content_blocked_patterns: Annotated[list[str], NoDecode] = Field(default_factory=list)
    atr_enabled: bool = True
    atr_blocking: bool = False
    atr_min_severity: str = Field(
        default="medium", pattern=r"^(informational|low|medium|high|critical)$"
    )
    atr_block_severity: str = Field(default="high", pattern=r"^(low|medium|high|critical)$")
    atr_max_content_chars: int = Field(default=100_000, ge=1_000, le=1_000_000)
    atr_scan_timeout_seconds: float = Field(default=5.0, ge=0.1, le=60)
    atr_scan_max_workers: int = Field(default=4, ge=1, le=32)
    atr_scan_memory_mb: int = Field(default=512, ge=64, le=4096)
    atr_rules_dir: str = ""
    atr_blocking_rule_ids: Annotated[list[str], NoDecode] = Field(default_factory=list)
    atr_blocking_excluded_rule_ids: Annotated[list[str], NoDecode] = Field(default_factory=list)
    atr_findings_ttl_seconds: int = Field(default=2_592_000, ge=3_600)
    atr_findings_max_records: int = Field(default=10_000, ge=100, le=100_000)
    atr_calibration_min_reviews: int = Field(default=20, ge=1, le=10_000)
    atr_calibration_max_false_positive_rate: float = Field(default=0.05, ge=0, le=1)

    @field_validator("playwright_allowed_domains", mode="before")
    @classmethod
    def split_domains(cls, value: object) -> object:
        if isinstance(value, str):
            return [item.strip().lower() for item in value.split(",") if item.strip()]
        return value

    @field_validator("content_blocked_patterns", mode="before")
    @classmethod
    def split_blocked_patterns(cls, value: object) -> object:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("atr_blocking_rule_ids", "atr_blocking_excluded_rule_ids", mode="before")
    @classmethod
    def split_atr_rule_ids(cls, value: object) -> object:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("content_blocked_patterns")
    @classmethod
    def validate_blocked_patterns(cls, value: list[str]) -> list[str]:
        for pattern in value:
            try:
                re.compile(pattern)
            except re.error as exc:
                raise ValueError(f"Invalid content policy regex: {pattern}") from exc
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
