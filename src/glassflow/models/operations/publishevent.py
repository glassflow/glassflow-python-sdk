from __future__ import annotations
import dataclasses
import requests as requests_http
from typing import Optional


@dataclasses.dataclass
class PublishEventRequestBody:
    pass


@dataclasses.dataclass
class PublishEventRequest:
    space_id: str = dataclasses.field(metadata={'path_param': { 'field_name': 'space_id', 'style': 'simple', 'explode': False }})
    pipeline_id: str = dataclasses.field(metadata={'path_param': { 'field_name': 'pipeline_id', 'style': 'simple', 'explode': False }})
    organization_id: Optional[str] = dataclasses.field(default=None, metadata={'query_param': { 'field_name': 'organization_id', 'style': 'form', 'explode': True }})
    x_pipeline_access_token: str = dataclasses.field(default=None, metadata={'header': { 'field_name': 'X-PIPELINE-ACCESS-TOKEN', 'style': 'simple', 'explode': False }})
    request_body: dict = dataclasses.field(default=None, metadata={'request': { 'media_type': 'application/json' }})




@dataclasses.dataclass
class PublishEventResponseBody:
    r"""Message pushed to the pipeline"""




@dataclasses.dataclass
class PublishEventResponse:
    content_type: str = dataclasses.field()
    r"""HTTP response content type for this operation"""
    status_code: int = dataclasses.field()
    r"""HTTP response status code for this operation"""
    raw_response: requests_http.Response = dataclasses.field()
    r"""Raw HTTP response; suitable for custom response parsing"""
    object: Optional[PublishEventResponseBody] = dataclasses.field(default=None)
    r"""Message pushed to the pipeline"""
