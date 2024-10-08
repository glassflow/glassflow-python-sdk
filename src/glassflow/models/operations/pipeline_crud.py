from __future__ import annotations

import dataclasses
from enum import Enum

from ..api import (
    CreatePipeline,
    GetDetailedSpacePipeline,
    PipelineState,
    SinkConnector,
    SourceConnector,
    SpacePipeline,
)
from .base import BaseManagementRequest, BasePipelineManagementRequest, BaseResponse


@dataclasses.dataclass
class GetPipelineRequest(BasePipelineManagementRequest):
    pass


@dataclasses.dataclass
class GetPipelineResponse(BaseResponse):
    pipeline: GetDetailedSpacePipeline | None = dataclasses.field(default=None)


@dataclasses.dataclass
class CreatePipelineRequest(BaseManagementRequest, CreatePipeline):
    pass


@dataclasses.dataclass
class UpdatePipelineRequest(BaseManagementRequest):
    name: str | None = dataclasses.field(default=None)
    state: PipelineState | None = dataclasses.field(default=None)
    metadata: dict | None = dataclasses.field(default=None)
    source_connector: SourceConnector | None = dataclasses.field(default=None)
    sink_connector: SinkConnector | None = dataclasses.field(default=None)


@dataclasses.dataclass
class UpdatePipelineResponse(BaseResponse):
    pipeline: GetDetailedSpacePipeline | None = dataclasses.field(default=None)


@dataclasses.dataclass
class CreatePipelineResponse(BaseResponse):
    name: str
    space_id: str
    id: str
    created_at: str
    state: PipelineState
    access_token: str
    metadata: dict | None = dataclasses.field(default=None)


@dataclasses.dataclass
class DeletePipelineRequest(BasePipelineManagementRequest):
    pass


class Order(str, Enum):
    asc = "asc"
    desc = "desc"


@dataclasses.dataclass
class ListPipelinesRequest(BaseManagementRequest):
    space_id: list[str] | None = None
    page_size: int = 50
    page: int = 1
    order_by: Order = Order.asc


@dataclasses.dataclass
class ListPipelinesResponse(BaseResponse):
    total_amount: int
    pipelines: list[SpacePipeline]
