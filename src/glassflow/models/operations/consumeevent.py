"""Dataclasses for the consume event operation
"""

from __future__ import annotations
import dataclasses
import requests as requests_http
from typing import Optional
from dataclasses_json import config, dataclass_json


@dataclasses.dataclass
class ConsumeEventRequest:
    """Request to consume an event from a pipeline topic

    Attributes:
        pipeline_id: The id of the pipeline
        space_id: The id of the space
        organization_id: The id of the organization
        x_pipeline_access_token: The access token of the pipeline

    """
    pipeline_id: str = dataclasses.field(
        metadata={
            'path_param': {
                'field_name': 'pipeline_id',
                'style': 'simple',
                'explode': False
            }
        })
    space_id: str = dataclasses.field(
        metadata={
            'query_param': {
                'field_name': 'space_id',
                'style': 'form',
                'explode': True
            }
        })
    organization_id: Optional[str] = dataclasses.field(
        default=None,
        metadata={
            'query_param': {
                'field_name': 'organization_id',
                'style': 'form',
                'explode': True
            }
        })
    x_pipeline_access_token: str = dataclasses.field(
        default=None,
        metadata={
            'header': {
                'field_name': 'X-PIPELINE-ACCESS-TOKEN',
                'style': 'simple',
                'explode': False
            }
        })


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
class ConsumeEventResponse:
    """Response to consume an event from a pipeline topic

    Attributes:
        content_type: HTTP response content type for this operation
        status_code: HTTP response status code for this operation
        raw_response: Raw HTTP response; suitable for custom response parsing
        body: the response body from the

    """
    content_type: str = dataclasses.field()
    status_code: int = dataclasses.field()
    raw_response: requests_http.Response = dataclasses.field()
    body: Optional[ConsumeEventResponseBody] = dataclasses.field(default=None)

    def json(self):
        """Return the response body as a JSON object.
        This method is to have cmopatibility with the requests.Response.json() method

        Returns:
            dict: The transformed event as a JSON object
        """
        return self.body.event
