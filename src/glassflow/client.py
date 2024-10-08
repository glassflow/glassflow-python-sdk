"""GlassFlow Python Client to interact with GlassFlow API"""

from __future__ import annotations

from .api_client import APIClient
from .models import errors, operations
from .models.api import PipelineState
from .pipeline import Pipeline
from .space import Space


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
        self, personal_access_token: str = None, organization_id: str = None
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
            personal_access_token=self.personal_access_token, id=pipeline_id
        ).fetch()

    def create_pipeline(
        self,
        name: str,
        space_id: str,
        transformation_file: str = None,
        requirements: str = None,
        source_kind: str = None,
        source_config: dict = None,
        sink_kind: str = None,
        sink_config: dict = None,
        env_vars: list[dict[str, str]] = None,
        state: PipelineState = "running",
        metadata: dict = None,
    ) -> Pipeline:
        """Creates a new GlassFlow pipeline

        Args:
            name: Name of the pipeline
            space_id: ID of the GlassFlow Space you want to create the pipeline in
            transformation_file: Path to file with transformation function of
                the pipeline.
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
            transformation_file=transformation_file,
            requirements=requirements,
            source_kind=source_kind,
            source_config=source_config,
            sink_kind=sink_kind,
            sink_config=sink_config,
            env_vars=env_vars,
            state=state,
            metadata=metadata,
            organization_id=self.organization_id,
            personal_access_token=self.personal_access_token,
        ).create()

    def list_pipelines(
        self, space_ids: list[str] | None = None
    ) -> operations.ListPipelinesResponse:
        """
        Lists all pipelines in the GlassFlow API

        Args:
            space_ids: List of Space IDs of the pipelines to list.
                If not specified, all the pipelines will be listed.

        Returns:
            ListPipelinesResponse: Response object with the pipelines listed

        Raises:
            UnauthorizedError: User does not have permission to perform the
                requested operation
        """
        request = operations.ListPipelinesRequest(
            space_id=space_ids,
            organization_id=self.organization_id,
            personal_access_token=self.personal_access_token,
        )
        try:
            res = self._request(
                method="GET",
                endpoint="/pipelines",
                request=request,
            )
            res_json = res.raw_response.json()
        except errors.ClientError as e:
            if e.status_code == 401:
                raise errors.UnauthorizedError(e.raw_response) from e
            else:
                raise e

        return operations.ListPipelinesResponse(
            content_type=res.content_type,
            status_code=res.status_code,
            raw_response=res.raw_response,
            total_amount=res_json["total_amount"],
            pipelines=res_json["pipelines"],
        )

    def list_spaces(self) -> operations.ListSpacesResponse:
        """
        Lists all GlassFlow spaces in the GlassFlow API

        Returns:
            ListSpacesResponse: Response object with the spaces listed

        Raises:
            UnauthorizedError: User does not have permission to perform the
                requested operation
        """
        request = operations.ListSpacesRequest(
            organization_id=self.organization_id,
            personal_access_token=self.personal_access_token,
        )
        try:
            res = self._request(
                method="GET",
                endpoint="/spaces",
                request=request,
            )
            res_json = res.raw_response.json()
        except errors.ClientError as e:
            if e.status_code == 401:
                raise errors.UnauthorizedError(e.raw_response) from e
            else:
                raise e

        return operations.ListSpacesResponse(
            content_type=res.content_type,
            status_code=res.status_code,
            raw_response=res.raw_response,
            total_amount=res_json["total_amount"],
            spaces=res_json["spaces"],
        )

    def create_space(
        self,
        name: str,
    ) -> Space:
        """Creates a new Space

        Args:
            name: Name of the Space

        Returns:
            Space: New space

        Raises:
            UnauthorizedError: User does not have permission to perform
                the requested operation
        """
        return Space(
            name=name,
            personal_access_token=self.personal_access_token,
            organization_id=self.organization_id,
        ).create()
