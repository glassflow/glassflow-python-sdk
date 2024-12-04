from __future__ import annotations

import dataclasses

from ..api import (
    ConsumeOutputEvent,
    FunctionEnvironments,
    FunctionLogs,
    SeverityCodeInput,
)
from .base import BasePipelineManagementRequest, BaseResponse


@dataclasses.dataclass
class GetFunctionLogsRequest(BasePipelineManagementRequest):
    page_size: int = 50
    page_token: str = None
    severity_code: SeverityCodeInput | None = None
    start_time: str | None = None
    end_time: str | None = None


@dataclasses.dataclass
class GetFunctionLogsResponse(BaseResponse):
    logs: FunctionLogs
    next: str


@dataclasses.dataclass
class FetchFunctionRequest(BasePipelineManagementRequest):
    pass


@dataclasses.dataclass
class UpdateFunctionRequest(BasePipelineManagementRequest):
    environments: FunctionEnvironments | None = dataclasses.field(default=None)


@dataclasses.dataclass
class TestFunctionRequest(BasePipelineManagementRequest):
    request_body: dict = dataclasses.field(
        default=None, metadata={"request": {"media_type": "application/json"}}
    )


@dataclasses.dataclass
class TestFunctionResponse(ConsumeOutputEvent, BaseResponse):
    pass
