"""GlassFlow Python Client to interact with GlassFlow API"""

from typing import Dict, List

from .api_client import APIClient
from .models.api import PipelineState
from .pipeline import Pipeline


class GlassFlowClient(APIClient):
    """
    GlassFlow Client to interact with GlassFlow API and manage pipelines
    and other resources

    Attributes:
        client: requests.Session object to make HTTP requests to GlassFlow API
        glassflow_config: GlassFlowConfig object to store configuration
        organization_id: Organization ID of the user. If not provided,
            the default organization will be used

    """

    def __init__(
            self,
            personal_access_token: str = None,
            organization_id: str = None
    ) -> None:
        """Create a new GlassFlowClient object

        Args:
            personal_access_token: GlassFlow Personal Access Token
            organization_id: Organization ID of the user. If not provided,
                the default organization will be used
        """
        super().__init__()
        self.personal_access_token = personal_access_token
        self.organization_id = organization_id

    def get_pipeline(self, pipeline_id: str) -> Pipeline:
        """Gets a Pipeline object from the GlassFlow API

        Args:
            pipeline_id: UUID of the pipeline

        Returns:
            Pipeline: Pipeline object from the GlassFlow API

        Raises:
            PipelineNotFoundError: Pipeline does not exist
            UnauthorizedError: User does not have permission to perform the
                requested operation
            ClientError: GlassFlow Client Error
        """
        return Pipeline(
            personal_access_token=self.personal_access_token,
            id=pipeline_id).fetch()

    def create_pipeline(
            self, name: str, space_id: str, transformation_code: str = None,
            transformation_file: str = None,
            requirements: str = None, source_kind: str = None,
            source_config: dict = None, sink_kind: str = None,
            sink_config: dict = None, env_vars: List[Dict[str, str]] = None,
            state: PipelineState = "running", metadata: dict = None,
    ) -> Pipeline:
        """Creates a new GlassFlow pipeline

        Args:
            name: Name of the pipeline
            space_id: ID of the GlassFlow Space you want to create the pipeline in
            transformation_code: String with the transformation function of the
                pipeline. Either transformation_code or transformation_file
                must be provided.
            transformation_file: Path to file with transformation function of
                the pipeline. Either transformation_code or transformation_file
                must be provided.
            requirements: Requirements.txt of the pipeline
            source_kind: Kind of source for the pipeline. If no source is
                provided, the default source will be SDK
            source_config: Configuration of the pipeline's source
            sink_kind: Kind of sink for the pipeline. If no sink is provided,
                the default sink will be SDK
            sink_config: Configuration of the pipeline's sink
            env_vars: Environment variables to pass to the pipeline
            state: State of the pipeline after creation.
                It can be either "running" or "paused"
            metadata: Metadata of the pipeline

        Returns:
            Pipeline: New pipeline

        Raises:
            UnauthorizedError: User does not have permission to perform
                the requested operation
        """
        return Pipeline(
            name=name,
            space_id=space_id,
            transformation_code=transformation_code,
            transformation_file=transformation_file,
            requirements=requirements,
            source_kind=source_kind,
            source_config=source_config,
            sink_kind=sink_kind,
            sink_config=sink_config,
            env_vars=env_vars,
            state=state,
            metadata=metadata,
            personal_access_token=self.personal_access_token,
        ).create()
