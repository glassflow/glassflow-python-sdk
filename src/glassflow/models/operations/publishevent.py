"""Dataclasses for publish event operation"""

from __future__ import annotations

import dataclasses
from typing import Optional

import requests as requests_http


@dataclasses.dataclass
class PublishEventRequestBody:
    pass


@dataclasses.dataclass
class PublishEventRequest:
    """Request to publish an event to a pipeline topic

    Attributes:
        pipeline_id: The id of the pipeline
        organization_id: The id of the organization
        x_pipeline_access_token: The access token of the pipeline
        request_body: The request body / event that should be published to the pipeline
    """

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
    request_body: dict = dataclasses.field(
        default=None, metadata={"request": {"media_type": "application/json"}}
    )


@dataclasses.dataclass
class PublishEventResponseBody:
    """Message pushed to the pipeline"""


@dataclasses.dataclass
class PublishEventResponse:
    """Response object for publish event operation

    Attributes:
        content_type: HTTP response content type for this operation
        status_code: HTTP response status code for this operation
        raw_response: Raw HTTP response; suitable for custom response parsing
        object: Response to the publish operation

    """

    content_type: str = dataclasses.field()
    status_code: int = dataclasses.field()
    raw_response: requests_http.Response = dataclasses.field()
    object: Optional[PublishEventResponseBody] = dataclasses.field(default=None)
