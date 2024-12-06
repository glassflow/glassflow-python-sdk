from __future__ import annotations

import dataclasses

from ..api import EventContext, FunctionEnvironments, FunctionLogs, SeverityCodeInput
from .base import BasePipelineManagementRequest, BaseResponse
from ...utils import generate_metadata_for_query_parameters


@dataclasses.dataclass
class GetFunctionLogsRequest(BasePipelineManagementRequest):
    page_size: int = dataclasses.field(
        default=50,
        metadata=generate_metadata_for_query_parameters("page_size"),
    )
    page_token: str = dataclasses.field(
        default=None,
        metadata=generate_metadata_for_query_parameters("page_token"),
    )
    severity_code: SeverityCodeInput | None = dataclasses.field(
        default=None,
        metadata=generate_metadata_for_query_parameters("severity_code"),
    )
    start_time: str | None = dataclasses.field(
        default=None,
        metadata=generate_metadata_for_query_parameters("start_time"),
    )
    end_time: str | None = dataclasses.field(
        default=None,
        metadata=generate_metadata_for_query_parameters("end_time"),
    )


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
class TestFunctionResponse(BaseResponse):
    payload: str
    event_context: EventContext
    status: str
    response: dict
