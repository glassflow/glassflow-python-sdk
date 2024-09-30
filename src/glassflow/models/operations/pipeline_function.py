from __future__ import annotations

import dataclasses

from .base import BasePipelineManagementRequest


@dataclasses.dataclass
class PipelineGetFunctionSourceRequest(BasePipelineManagementRequest):
    pass
