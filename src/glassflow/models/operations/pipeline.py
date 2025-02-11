from __future__ import annotations

from pydantic import AwareDatetime, BaseModel

from glassflow.models.api import (
    PipelineState,
    SinkConnector,
    SourceConnector,
)


class CreatePipeline(BaseModel):
    name: str
    space_id: str
    metadata: dict | None = None
    id: str
    created_at: AwareDatetime
    state: PipelineState
    access_token: str


class UpdatePipelineRequest(BaseModel):
    name: str | None = None
    state: str | None = None
    metadata: dict | None = None
    source_connector: SourceConnector | None = None
    sink_connector: SinkConnector | None = None
