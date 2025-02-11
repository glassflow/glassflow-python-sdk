from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class Payload(BaseModel):
    model_config = ConfigDict(
        extra="allow",
    )
    message: str


class FunctionLogEntry(BaseModel):
    level: str
    severity_code: int
    timestamp: AwareDatetime
    payload: Payload


class FunctionLogsResponse(BaseModel):
    logs: list[FunctionLogEntry]
    next: str


class EventContext(BaseModel):
    request_id: str
    external_id: str | None = None
    receive_time: AwareDatetime


class ConsumeOutputEvent(BaseModel):
    req_id: str | None = Field(None, description="DEPRECATED")
    receive_time: AwareDatetime | None = Field(None, description="DEPRECATED")
    payload: Any
    event_context: EventContext
    status: str
    response: Any | None = None
    error_details: str | None = None
    stack_trace: str | None = None


class TestFunctionResponse(ConsumeOutputEvent):
    pass


class BasePipeline(BaseModel):
    name: str
    space_id: str
    metadata: dict[str, Any]


class PipelineState(str, Enum):
    running = "running"
    paused = "paused"


class Pipeline(BasePipeline):
    id: str
    created_at: AwareDatetime
    state: PipelineState


class SpacePipeline(Pipeline):
    space_name: str


class ListPipelinesResponse(BaseModel):
    total_amount: int
    pipelines: list[SpacePipeline]


class ConsumeOutputEvent(BaseModel):
    payload: Any
    event_context: EventContext
    status: str
    response: Any | None = None
    error_details: str | None = None
    stack_trace: str | None = None


class ConsumeEventResponse(BaseModel):
    body: ConsumeOutputEvent | None = None
    status_code: int | None = None

    def event(self):
        if self.body:
            return self.body.response
        return None


class PublishEventResponseBody(BaseModel):
    """Message pushed to the pipeline."""

    pass


class PublishEventResponse(BaseModel):
    status_code: int | None = None


class ConsumeFailedResponse(BaseModel):
    body: ConsumeOutputEvent | None = None
    status_code: int | None = None


class AccessToken(BaseModel):
    id: str
    name: str
    token: str
    created_at: AwareDatetime


class ListAccessTokensResponse(BaseModel):
    access_tokens: list[AccessToken]
    total_amount: int
