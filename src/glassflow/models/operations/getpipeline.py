from __future__ import annotations

import dataclasses

from ..api import CreatePipeline, GetDetailedSpacePipeline, PipelineState
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
class CreatePipelineResponse(BaseResponse):
    name: str
    space_id: str
    id: str
    created_at: str
    state: PipelineState
    access_token: str
    metadata: dict | None = dataclasses.field(default=None)
