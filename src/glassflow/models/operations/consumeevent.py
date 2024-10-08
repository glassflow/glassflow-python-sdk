"""Dataclasses for the consume event operation"""

from __future__ import annotations

import dataclasses

from dataclasses_json import config, dataclass_json

from .base import BasePipelineDataRequest, BaseResponse


@dataclasses.dataclass
class ConsumeEventRequest(BasePipelineDataRequest):
    """Request to consume an event from a pipeline topic

    Attributes:
        pipeline_id: The id of the pipeline
        organization_id: The id of the organization
        x_pipeline_access_token: The access token of the pipeline

    """

    pass


@dataclass_json
@dataclasses.dataclass
class ConsumeEventResponseBody:
    """Event response body after transformation

    Attributes:
        req_id: The request id
        receive_time: The time when the event was received
        event: The event received

    """

    req_id: str = dataclasses.field()
    receive_time: str = dataclasses.field()
    event: dict = dataclasses.field(metadata=config(field_name="response"))


@dataclasses.dataclass
class ConsumeEventResponse(BaseResponse):
    """Response to consume an event from a pipeline topic

    Attributes:
        content_type: HTTP response content type for this operation
        status_code: HTTP response status code for this operation
        raw_response: Raw HTTP response; suitable for custom response parsing
        body: the response body from the api call

    """

    body: ConsumeEventResponseBody | None = dataclasses.field(default=None)

    def json(self):
        """Return the response body as a JSON object.
        This method is to have compatibility with the requests.Response.json() method

        Returns:
            dict: The transformed event as a JSON object
        """
        return self.body.event
