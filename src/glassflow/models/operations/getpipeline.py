from __future__ import annotations

import dataclasses
from typing import Optional

from .base import BaseResponse, BasePipelineManagementRequest
from ..api import GetDetailedSpacePipeline


@dataclasses.dataclass
class GetPipelineRequest(BasePipelineManagementRequest):
    pass


@dataclasses.dataclass
class GetPipelineResponse(BaseResponse):
    pipeline: Optional[GetDetailedSpacePipeline] = dataclasses.field(default=None)
