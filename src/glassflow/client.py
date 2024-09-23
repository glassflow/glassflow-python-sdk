"""GlassFlow Python Client to interact with GlassFlow API"""

from .api_client import APIClient
from .pipeline import Pipeline
from .models import operations, errors, api

from typing import List, Dict


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

    def __init__(self, personal_access_token: str = None, organization_id: str = None) -> None:
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
            UnauthorizedError: User does not have permission to perform the requested operation
            ClientError: GlassFlow Client Error
        """
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

    def create_pipeline(
            self, name: str, space_id: str, transformation_code: str = None,
            transformation_file: str = None,
            requirements: str = None, source_kind: str = None,
            source_config: dict = None, sink_kind: str = None,
            sink_config: dict = None, env_vars: List[Dict[str, str]] = None,
            state: api.PipelineState = "running", metadata: dict = None,
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

        if transformation_code is None and transformation_file is None:
            raise ValueError(
                "Either transformation_code or transformation_file must "
                "be provided")

        if transformation_code is None and transformation_file is not None:
            try:
                transformation_code = open(transformation_file, "r").read()
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"Transformation file was not found in "
                    f"{transformation_file}")

        if source_kind is not None and source_config is not None:
            source_connector = api.SourceConnector(
                kind=source_kind,
                config=source_config,
            )
        elif source_kind is None and source_config is None:
            source_connector = None
        else:
            raise ValueError(
                "Both source_kind and source_config must be provided")

        if sink_kind is not None and sink_config is not None:
            sink_connector = api.SinkConnector(
                kind=sink_kind,
                config=sink_config,
            )
        elif sink_kind is None and sink_config is None:
            sink_connector = None
        else:
            raise ValueError("Both sink_kind and sink_config must be provided")

        create_pipeline = api.CreatePipeline(
            name=name,
            space_id=space_id,
            transformation_function=transformation_code,
            requirements_txt=requirements,
            source_connector=source_connector,
            sink_connector=sink_connector,
            environments=env_vars,
            state=state,
            metadata=metadata if metadata is not None else {},
        )
        request = operations.CreatePipelineRequest(
            organization_id=self.organization_id,
            personal_access_token=self.personal_access_token,
            **create_pipeline.__dict__,
        )

        try:
            base_res = self.request(
                method="POST",
                endpoint=f"/pipelines",
                request=request
            )
            res = operations.CreatePipelineResponse(
                status_code=base_res.status_code,
                content_type=base_res.content_type,
                raw_response=base_res.raw_response,
                **base_res.raw_response.json())
        except errors.ClientError as e:
            if e.status_code == 401:
                raise errors.UnauthorizedError(e.raw_response)
            else:
                raise e
        return Pipeline(
            personal_access_token=self.personal_access_token,
            id=res.id,
            **create_pipeline.__dict__)
