import dataclasses
from typing import Optional
import requests as requests_http


@dataclasses.dataclass
class PipelineCRUDRequest:
    """Request to Create, Get, Update and Delete pipelines.

    Attributes
        pipeline_id: The if of the pipeline
        organization_id: The if of the pipeline
        glassflow token: The token to authenticate against the Glassflow API
    """
    pipeline_id: str = dataclasses.field(
        metadata={
            'path_param': {
                'field_name': 'pipeline_id',
                'style': 'simple',
                'explode': False
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
    glassflow_token: str = dataclasses.field(
        default=None,
        metadata={
            'header': {
                'field_name': 'Authorization',
                'style': 'simple',
                'explode': False
            }
        })


@dataclasses.dataclass
class SourceConnector:
    kind: str
    config: object


@dataclasses.dataclass
class SinkConnector:
    kind: str
    config: object


@dataclasses.dataclass
class PipelineGetResponseBody:
    name: str
    space_id: str
    metadata: object
    id: str
    created_at: str
    space_name: str
    source_connector: SourceConnector
    sink_connector: SinkConnector
    environment: object


@dataclasses.dataclass
class PipelineGetResponse:
    """Response from Getting a pipeline.

    Attributes
        pipeline_id: The if of the pipeline
    """
    content_type: str = dataclasses.field()
    status_code: int = dataclasses.field()
    raw_response: requests_http.Response = dataclasses.field()
    object: Optional[PipelineGetResponseBody] = dataclasses.field(
        default=None)