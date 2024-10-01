from __future__ import annotations

import dataclasses

from ..api import FunctionLogs, SeverityCodeInput
from .base import BasePipelineManagementRequest, BaseResponse


@dataclasses.dataclass
class PipelineFunctionsGetSourceRequest(BasePipelineManagementRequest):
    pass


@dataclasses.dataclass
class PipelineFunctionsGetLogsRequest(BasePipelineManagementRequest):
    page_size: int = 50
    page_token: str = None
    severity_code: SeverityCodeInput | None = None
    start_time: str | None = None
    end_time: str | None = None


@dataclasses.dataclass
class PipelineFunctionsGetLogsResponse(BaseResponse):
    logs: FunctionLogs
    next: str
