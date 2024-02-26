from __future__ import annotations
import dataclasses
import requests as requests_http
from glassflow.models.components import pipeline as components_pipeline
from typing import Optional


@dataclasses.dataclass
class GetPipelineRequest:
    space_id: str = dataclasses.field(metadata={'path_param': { 'field_name': 'space_id', 'style': 'simple', 'explode': False }})
    pipeline_id: str = dataclasses.field(metadata={'path_param': { 'field_name': 'pipeline_id', 'style': 'simple', 'explode': False }})
    organization_id: Optional[str] = dataclasses.field(default=None, metadata={'query_param': { 'field_name': 'organization_id', 'style': 'form', 'explode': True }})




@dataclasses.dataclass
class GetPipelineResponse:
    content_type: str = dataclasses.field()
    r"""HTTP response content type for this operation"""
    status_code: int = dataclasses.field()
    r"""HTTP response status code for this operation"""
    raw_response: requests_http.Response = dataclasses.field()
    r"""Raw HTTP response; suitable for custom response parsing"""
    pipeline: Optional[components_pipeline.Pipeline] = dataclasses.field(default=None)
    r"""Pipeline object"""
