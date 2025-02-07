from typing import Optional

from pydantic import BaseModel

from glassflow.models.api.v2 import (
    GetDetailedSpacePipeline,
    SinkConnector,
    SourceConnector,
)

from .base import BaseResponse


class CreatePipelineResponse(BaseResponse):
    body: Optional[GetDetailedSpacePipeline] = None


class UpdatePipelineRequest(BaseModel):
    name: Optional[str] = None
    state: Optional[str] = None
    metadata: Optional[dict] = None
    source_connector: Optional[SourceConnector] = None
    sink_connector: Optional[SinkConnector] = None
