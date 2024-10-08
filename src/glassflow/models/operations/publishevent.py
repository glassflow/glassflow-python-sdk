"""Dataclasses for publish event operation"""

from __future__ import annotations

import dataclasses

from .base import BasePipelineDataRequest, BaseResponse


@dataclasses.dataclass
class PublishEventRequestBody:
    pass


@dataclasses.dataclass
class PublishEventRequest(BasePipelineDataRequest):
    """Request to publish an event to a pipeline topic

    Attributes:
        pipeline_id: The id of the pipeline
        organization_id: The id of the organization
        x_pipeline_access_token: The access token of the pipeline
        request_body: The request body / event that should be published to the pipeline
    """

    request_body: dict = dataclasses.field(
        default=None, metadata={"request": {"media_type": "application/json"}}
    )


@dataclasses.dataclass
class PublishEventResponseBody:
    """Message pushed to the pipeline"""


@dataclasses.dataclass
class PublishEventResponse(BaseResponse):
    """Response object for publish event operation

    Attributes:
        content_type: HTTP response content type for this operation
        status_code: HTTP response status code for this operation
        raw_response: Raw HTTP response; suitable for custom response parsing
        object: Response to the publish operation

    """

    object: PublishEventResponseBody | None = dataclasses.field(default=None)
