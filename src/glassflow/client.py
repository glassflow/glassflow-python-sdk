"""GlassFlow Python Client to interact with GlassFlow API"""

from .api_client import APIClient


class GlassFlowClient(APIClient):
    """GlassFlow Client to interact with GlassFlow API and manage pipelines and other resources

    Attributes:
        client: requests.Session object to make HTTP requests to GlassFlow API
        glassflow_config: GlassFlowConfig object to store configuration
        organization_id: Organization ID of the user. If not provided, the default organization will be used

    """

    def __init__(self, personal_access_token: str = None, organization_id: str = None) -> None:
        """Create a new GlassFlowClient object

        Args:
            personal_access_token: GlassFlow Personal Access Token
            organization_id: Organization ID of the user. If not provided, the default organization will be used
        """
        super().__init__()
        self.personal_access_token = personal_access_token
        self.organization_id = organization_id
