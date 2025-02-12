from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict


class Payload(BaseModel):
    """
    Logs payload response object.

    Attributes:
        message (str): log message
    """

    model_config = ConfigDict(
        extra="allow",
    )
    message: str


class FunctionLogEntry(BaseModel):
    """
    Logs entry response object.

    Attributes:
        level (int): Log level.
        severity_code (int): Log severity code.
        timestamp (AwareDatetime): Log timestamp.
        payload (Payload): Log payload.
    """

    level: str
    severity_code: int
    timestamp: AwareDatetime
    payload: Payload


class FunctionLogsResponse(BaseModel):
    """
    Response for a function's logs endpoint.

    Attributes:
        logs (list[FunctionLogEntry]): list of logs
        next (str): ID used to retrieve next page of logs
    """

    logs: list[FunctionLogEntry]
    next: str


class EventContext(BaseModel):
    """
    Event context response object.

    Attributes:
        request_id (str): Request ID.
        external_id (str): External ID.
        receive_time (AwareDatetime): Receive time.
    """

    request_id: str
    external_id: str | None = None
    receive_time: AwareDatetime


class ConsumeOutputEvent(BaseModel):
    """
    Consume output event

    Attributes:
        payload (Any): Payload
        event_context (EventContext): Event context
        status (str): Status
        response (Any): request response
        error_details (str): Error details
        stack_trace (str): Error Stack trace
    """

    payload: Any
    event_context: EventContext
    status: str
    response: Any | None = None
    error_details: str | None = None
    stack_trace: str | None = None


class TestFunctionResponse(ConsumeOutputEvent):
    """Response for Test function endpoint."""
    pass


class BasePipeline(BaseModel):
    """
    Base pipeline response object.

    Attributes:
        name (str): Pipeline name.
        space_id (str): Space ID.
        metadata (dict[str, Any]): Pipeline metadata.
    """

    name: str
    space_id: str
    metadata: dict[str, Any]


class PipelineState(str, Enum):
    """
    Pipeline state
    """

    running = "running"
    paused = "paused"


class Pipeline(BasePipeline):
    """
    Pipeline response object.

    Attributes:
        id (str): Pipeline id
        created_at (AwareDatetime): Pipeline creation time
        state (PipelineState): Pipeline state
    """

    id: str
    created_at: AwareDatetime
    state: PipelineState


class SpacePipeline(Pipeline):
    """
    Pipeline with space response object.

    Attributes:
        space_name (str): Space name
    """

    space_name: str


class ListPipelinesResponse(BaseModel):
    """
    Response for list pipelines endpoint

    Attributes:
        total_amount (int): Total amount of pipelines.
        pipelines (list[SpacePipeline]): List of pipelines.
    """

    total_amount: int
    pipelines: list[SpacePipeline]


class ConsumeEventResponse(BaseModel):
    """
    Response from consume event

    Attributes:
        status_code (int): HTTP status code
        body (ConsumeOutputEvent): Body of the response
    """

    body: ConsumeOutputEvent | None = None
    status_code: int | None = None

    def event(self) -> Any:
        """Return event response."""
        if self.body:
            return self.body.response
        return None


class PublishEventResponseBody(BaseModel):
    """Message pushed to the pipeline."""

    pass


class PublishEventResponse(BaseModel):
    """
    Response from publishing event

    Attributes:
        status_code (int): HTTP status code
    """

    status_code: int | None = None


class ConsumeFailedResponse(BaseModel):
    """
    Response from consuming failed event

    Attributes:
        status_code (int): HTTP status code
        body (ConsumeOutputEvent | None): ConsumeOutputEvent
    """

    body: ConsumeOutputEvent | None = None
    status_code: int | None = None


class AccessToken(BaseModel):
    """
    Access Token response object.

    Attributes:
        id (str): The access token id.
        name (str): The access token name.
        token (str): The access token string.
        created_at (AwareDatetime): The access token creation date.
    """

    id: str
    name: str
    token: str
    created_at: AwareDatetime


class ListAccessTokensResponse(BaseModel):
    """
    Response for listing access tokens endpoint.

    Attributes:
        total_amount (int): Total amount of access tokens.
        access_tokens (list[AccessToken]): List of access tokens.
    """

    access_tokens: list[AccessToken]
    total_amount: int
