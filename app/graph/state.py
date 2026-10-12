from typing import NotRequired, TypedDict


class AgentState(TypedDict):
    request_id: str
    conversation_id: str
    model: str
    query: str
    conversation_history: list[dict[str, str]]
    context_max_tokens: int
    target_url: str | None
    target_urls: list[str]
    plan: list[str]
    tool_calls: list[dict[str, object]]
    tool_history: list[dict[str, object]]
    tool_round: int
    max_tool_rounds: int
    interactive_approval: bool
    approved_tools: list[str]
    attempt: int
    max_retries: int
    tool_result: str
    candidate: str
    evidence_errors: list[str]
    evidence_validated: bool
    evidence_report: dict[str, object]
    usage: dict[str, float | int]
    answer: str
    last_error: NotRequired[str]
