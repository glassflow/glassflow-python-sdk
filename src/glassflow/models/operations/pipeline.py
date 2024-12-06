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
from ...utils import generate_metadata_for_query_parameters


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
    space_id: list[str] | None = dataclasses.field(
        default=None,
        metadata=generate_metadata_for_query_parameters("space_id"),
    )
    page_size: int = dataclasses.field(
        default=50,
        metadata=generate_metadata_for_query_parameters("page_size"),
    )
    page: int = dataclasses.field(
        default=1,
        metadata=generate_metadata_for_query_parameters("page"),
    )
    order_by: Order = dataclasses.field(
        default=Order.asc,
        metadata=generate_metadata_for_query_parameters("order_by"),
    )


@dataclasses.dataclass
class ListPipelinesResponse(BaseResponse):
    total_amount: int
    pipelines: list[SpacePipeline]
