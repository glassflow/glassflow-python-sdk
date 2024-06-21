"""GlassFlow Python Client to interact with GlassFlow API
"""

from .pipelines import PipelineClient
from typing import Optional
from .config import GlassFlowConfig
import requests as requests_http
from dotenv import load_dotenv
import os


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

    def pipeline_client(self,
                        pipeline_id: Optional[str] = None,
                        pipeline_access_token: Optional[str] = None,
                        space_id: Optional[str] = None) -> PipelineClient:
        """Create a new PipelineClient object to interact with a specific pipeline

        Args:
            pipeline_id: The pipeline id to interact with
            pipeline_access_token: The access token to access the pipeline

        Returns:
            PipelineClient: Client object to publish and consume events from the given pipeline.
        """
        # if no pipeline_id or pipeline_access_token is provided, try to read from environment variables
        load_dotenv()
        if not pipeline_id:
            pipeline_id = os.getenv('PIPELINE_ID')
        if not pipeline_access_token:
            pipeline_access_token = os.getenv('PIPELINE_ACCESS_TOKEN')

        if not pipeline_id:
            raise ValueError(
                "pipeline_id is required to create a PipelineClient")
        if not pipeline_access_token:
            raise ValueError(
                "pipeline_access_token is required to create a PipelineClient")

        return PipelineClient(glassflow_client=self,
                              pipeline_id=pipeline_id,
                              pipeline_access_token=pipeline_access_token)
