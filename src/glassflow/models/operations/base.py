import dataclasses
from requests import Response

from typing import Optional


@dataclasses.dataclass
class BaseRequest:
    pass


@dataclasses.dataclass
class BasePipelineRequest(BaseRequest):
    pipeline_id: str = dataclasses.field(
        metadata={
            "path_param": {
                "field_name": "pipeline_id",
                "style": "simple",
                "explode": False,
            }
        }
    )
    organization_id: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "query_param": {
                "field_name": "organization_id",
                "style": "form",
                "explode": True,
            }
        },
    )


@dataclasses.dataclass
class BasePipelineDataRequest(BasePipelineRequest):
    x_pipeline_access_token: str = dataclasses.field(
        default=None,
        metadata={
            "header": {
                "field_name": "X-PIPELINE-ACCESS-TOKEN",
                "style": "simple",
                "explode": False,
            }
        },
    )


@dataclasses.dataclass
class BasePipelineManagementRequest(BasePipelineRequest):
    personal_access_token: str = dataclasses.field(
        default=None,
        metadata={
            "header": {
                "field_name": "Personal-Access-Token",
                "style": "simple",
                "explode": False,
            }
        },
    )


@dataclasses.dataclass
class BaseResponse:
    content_type: str = dataclasses.field()
    status_code: int = dataclasses.field()
    raw_response: Response = dataclasses.field()