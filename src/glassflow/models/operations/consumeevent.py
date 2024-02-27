from __future__ import annotations
import dataclasses
import requests as requests_http
from typing import Optional
from dataclasses_json import config, dataclass_json


@dataclasses.dataclass
class ConsumeEventRequest:
    pipeline_id: str = dataclasses.field(metadata={'path_param': {
                                         'field_name': 'pipeline_id', 'style': 'simple', 'explode': False}})
    space_id: str = dataclasses.field(metadata={'query_param': {
                                      'field_name': 'space_id', 'style': 'form', 'explode': True}})
    organization_id: Optional[str] = dataclasses.field(default=None, metadata={
                                                       'query_param': {'field_name': 'organization_id', 'style': 'form', 'explode': True}})
    x_pipeline_access_token: str = dataclasses.field(default=None, metadata={'header': {
                                                     'field_name': 'X-PIPELINE-ACCESS-TOKEN', 'style': 'simple', 'explode': False}})


@dataclass_json
@dataclasses.dataclass
class ConsumeEventResponseBody:
    r"""Event response body after transformation"""
    req_id: str = dataclasses.field()
    receive_time: str = dataclasses.field()
    event: dict = dataclasses.field(metadata=config(field_name="response"))


@dataclasses.dataclass
class ConsumeEventResponse:
    content_type: str = dataclasses.field()
    r"""HTTP response content type for this operation"""
    status_code: int = dataclasses.field()
    r"""HTTP response status code for this operation"""
    raw_response: requests_http.Response = dataclasses.field()
    r"""Raw HTTP response; suitable for custom response parsing"""
    body: Optional[ConsumeEventResponseBody] = dataclasses.field(default=None)
    r"""An output message of transformation"""
# todo -> have a param which provides the event directly as json
