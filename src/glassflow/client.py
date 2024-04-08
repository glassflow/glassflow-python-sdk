"""GlassFlow Python Client to interact with GlassFlow API
"""

from .pipelines import PipelineClient
from .config import GlassFlowConfig
import requests as requests_http


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

    def pipeline_client(self, space_id: str, pipeline_id: str,
                        pipeline_access_token: str) -> PipelineClient:
        """Create a new PipelineClient object to interact with a specific pipeline

        Args:
            space_id: The space id where the pipeline is located
            pipeline_id: The pipeline id to interact with
            pipeline_access_token: The access token to access the pipeline

        Returns:
            PipelineClient: Client object to publish and consume events from the given pipeline.
        """
        return PipelineClient(glassflow_client=self,
                              space_id=space_id,
                              pipeline_id=pipeline_id,
                              pipeline_access_token=pipeline_access_token)
