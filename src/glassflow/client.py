"""GlassFlow Python Client to interact with GlassFlow API"""

import os
from typing import Optional

import requests as requests_http

from .config import GlassFlowConfig
from .pipelines import PipelineClient


class GlassFlowClient:
    """GlassFlow Client to interact with GlassFlow API and manage pipelines and other resources

    Attributes:
        rclient: requests.Session object to make HTTP requests to GlassFlow API
        glassflow_config: GlassFlowConfig object to store configuration
        organization_id: Organization ID of the user. If not provided, the default organization will be used

    """

    glassflow_config: GlassFlowConfig

    def __init__(self, organization_id: str = None) -> None:
        """Create a new GlassFlowClient object

        Args:
            organization_id: Organization ID of the user. If not provided, the default organization will be used
        """
        rclient = requests_http.Session()
        self.glassflow_config = GlassFlowConfig(rclient)
        self.organization_id = organization_id

    def pipeline_client(
        self,
        pipeline_id: Optional[str] = None,
        pipeline_access_token: Optional[str] = None,
        space_id: Optional[str] = None,
    ) -> PipelineClient:
        """Create a new PipelineClient object to interact with a specific pipeline

        Args:
            pipeline_id: The pipeline id to interact with
            pipeline_access_token: The access token to access the pipeline

        Returns:
            PipelineClient: Client object to publish and consume events from the given pipeline.
        """
        # if no pipeline_id or pipeline_access_token is provided, try to read from environment variables
        if not pipeline_id:
            pipeline_id = os.getenv("PIPELINE_ID")
        if not pipeline_access_token:
            pipeline_access_token = os.getenv("PIPELINE_ACCESS_TOKEN")
        # no pipeline_id provided explicitly or in environment variables
        if not pipeline_id:
            raise ValueError(
                "PIPELINE_ID must be set as an environment variable or provided explicitly"
            )
        if not pipeline_access_token:
            raise ValueError(
                "PIPELINE_ACCESS_TOKEN must be set as an environment variable or provided explicitly"
            )

        return PipelineClient(
            glassflow_client=self,
            pipeline_id=pipeline_id,
            pipeline_access_token=pipeline_access_token,
        )
