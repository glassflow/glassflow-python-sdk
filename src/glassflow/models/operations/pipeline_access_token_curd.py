from __future__ import annotations

import dataclasses

from .base import BasePipelineManagementRequest


@dataclasses.dataclass
class PipelineGetAccessTokensRequest(BasePipelineManagementRequest):
    page_size: int = 50
    page: int = 1
