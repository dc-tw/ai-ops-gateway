from typing import Literal

from pydantic import BaseModel, Field, HttpUrl


class AgentRequest(BaseModel):
    conversation_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    model: str | None = Field(default=None, min_length=1, max_length=200)
    query: str = Field(min_length=1, max_length=8_000)
    target_url: HttpUrl | None = None
    source_urls: list[HttpUrl] = Field(default_factory=list, max_length=5)
    max_retries: int | None = Field(default=None, ge=0, le=5)
    max_tool_rounds: int | None = Field(default=None, ge=1, le=20)
    deadline_seconds: float | None = Field(default=None, ge=1, le=3_600)
    approved_tools: list[str] = Field(default_factory=list, max_length=20)


class ApprovalDecision(BaseModel):
    approved: bool
    reason: str = Field(default="", max_length=500)


class GoalStepRequest(BaseModel):
    query: str = Field(min_length=1, max_length=8_000)
    target_url: HttpUrl | None = None


class GoalCreateRequest(BaseModel):
    objective: str = Field(min_length=1, max_length=8_000)
    model: str | None = Field(default=None, min_length=1, max_length=200)
    steps: list[GoalStepRequest] = Field(default_factory=list, max_length=100)
    max_retries: int = Field(default=2, ge=0, le=5)
    max_tool_rounds: int = Field(default=8, ge=1, le=20)
    step_deadline_seconds: float = Field(default=300.0, ge=1, le=3_600)


class GoalControlResponse(BaseModel):
    goal_id: str
    status: Literal[
        "queued",
        "running",
        "pausing",
        "paused",
        "waiting_approval",
        "waiting_children",
        "completed",
        "failed",
        "cancelled",
    ]
