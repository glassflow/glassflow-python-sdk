from enum import Enum
from typing import Any, Dict, List, Optional

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
    logs: List[FunctionLogEntry]
    next: str


class EventContext(BaseModel):
    request_id: str
    external_id: Optional[str] = None
    receive_time: AwareDatetime


class ConsumeOutputEvent(BaseModel):
    req_id: Optional[str] = Field(None, description="DEPRECATED")
    receive_time: Optional[AwareDatetime] = Field(None, description="DEPRECATED")
    payload: Any
    event_context: EventContext
    status: str
    response: Optional[Any] = None
    error_details: Optional[str] = None
    stack_trace: Optional[str] = None


class TestFunctionResponse(ConsumeOutputEvent):
    pass


class BasePipeline(BaseModel):
    name: str
    space_id: str
    metadata: Dict[str, Any]


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
