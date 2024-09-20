"""GlassFlow Python Client to interact with GlassFlow API"""

from .api_client import APIClient
from .pipeline import Pipeline
from .models import operations, errors


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

    def get_pipeline(self, pipeline_id: str) -> Pipeline:
        request = operations.GetPipelineRequest(
            pipeline_id=pipeline_id,
            organization_id=self.organization_id,
            personal_access_token=self.personal_access_token,
        )

        try:
            res = self.request(
                method="GET",
                endpoint=f"/pipelines/{pipeline_id}",
                request=request,
            )
        except errors.ClientError as e:
            if e.status_code == 404:
                raise errors.PipelineNotFoundError(pipeline_id, e.raw_response)
            elif e.status_code == 401:
                raise errors.UnauthorizedError(e.raw_response)
            else:
                raise e

        return Pipeline(self.personal_access_token, **res.raw_response.json())
