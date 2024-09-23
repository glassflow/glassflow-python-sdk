from __future__ import annotations

import dataclasses
from typing import Optional

from .base import BaseResponse, BasePipelineManagementRequest, BaseManagementRequest
from ..api import GetDetailedSpacePipeline, CreatePipeline, PipelineState


@dataclasses.dataclass
class GetPipelineRequest(BasePipelineManagementRequest):
    pass


@dataclasses.dataclass
class GetPipelineResponse(BaseResponse):
    pipeline: Optional[GetDetailedSpacePipeline] = dataclasses.field(default=None)


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
    metadata: Optional[dict] = dataclasses.field(default=None)