from __future__ import annotations

from .client import APIClient
from .models import api, errors, operations


class Pipeline(APIClient):
    def __init__(
        self,
        personal_access_token: str,
        name: str | None = None,
        space_id: str | None = None,
        id: str | None = None,
        source_kind: str | None = None,
        source_config: str | None = None,
        sink_kind: str | None = None,
        sink_config: str | None = None,
        requirements: str | None = None,
        transformation_code: str | None = None,
        transformation_file: str | None = None,
        env_vars: list[str] | None = None,
        state: api.PipelineState = "running",
        organization_id: str | None = None,
        metadata: dict | None = None,
        created_at: str | None = None,
    ):
        """Creates a new GlassFlow pipeline object

        Args:
            personal_access_token: The personal access token to authenticate
                against GlassFlow
            id: Pipeline ID
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
            created_at: Timestamp when the pipeline was created

        Returns:
            Pipeline: Pipeline object to interact with the GlassFlow API

        Raises:
            FailNotFoundError: If the transformation file is provided and
                does not exist
        """
        super().__init__()
        self.id = id
        self.name = name
        self.space_id = space_id
        self.personal_access_token = personal_access_token
        self.source_kind = source_kind
        self.source_config = source_config
        self.sink_kind = sink_kind
        self.sink_config = sink_config
        self.requirements = requirements
        self.transformation_code = transformation_code
        self.transformation_file = transformation_file
        self.env_vars = env_vars
        self.state = state
        self.organization_id = organization_id
        self.metadata = metadata if metadata is not None else {}
        self.created_at = created_at
        self.access_tokens = []

        if self.transformation_code is None and self.transformation_file is not None:
            try:
                with open(self.transformation_file) as f:
                    self.transformation_code = f.read()
            except FileNotFoundError:
                raise

        if source_kind is not None and self.source_config is not None:
            self.source_connector = api.SourceConnector(
                kind=self.source_kind,
                config=self.source_config,
            )
        elif self.source_kind is None and self.source_config is None:
            self.source_connector = None
        else:
            raise ValueError("Both source_kind and source_config must be provided")

        if self.sink_kind is not None and self.sink_config is not None:
            self.sink_connector = api.SinkConnector(
                kind=sink_kind,
                config=sink_config,
            )
        elif self.sink_kind is None and self.sink_config is None:
            self.sink_connector = None
        else:
            raise ValueError("Both sink_kind and sink_config must be provided")

    def fetch(self) -> Pipeline:
        """
        Fetches pipeline information from the GlassFlow API

        Returns:
            self: Pipeline object

        Raises:
            ValueError: If ID is not provided in the constructor
            PipelineNotFoundError: If ID provided does not match any
                existing pipeline in GlassFlow
            UnauthorizedError: If the Personal Access Token is not
                provider or is invalid
        """
        if self.id is None:
            raise ValueError(
                "Pipeline id must be provided in order to fetch it's details"
            )

        request = operations.GetPipelineRequest(
            pipeline_id=self.id,
            organization_id=self.organization_id,
            personal_access_token=self.personal_access_token,
        )

        try:
            res = self.request(
                method="GET",
                endpoint=f"/pipelines/{self.id}",
                request=request,
            )
            res_json = res.raw_response.json()
        except errors.ClientError as e:
            if e.status_code == 404:
                raise errors.PipelineNotFoundError(self.id, e.raw_response) from e
            elif e.status_code == 401:
                raise errors.UnauthorizedError(e.raw_response) from e
            else:
                raise e

        self.name = res_json["name"]
        self.space_id = res_json["space_id"]
        if res_json["source_connector"]:
            self.source_kind = res_json["source_connector"]["kind"]
            self.source_config = res_json["source_connector"]["config"]
        if res_json["sink_connector"]:
            self.sink_kind = res_json["sink_connector"]["kind"]
            self.sink_config = res_json["sink_connector"]["config"]
        self.created_at = res_json["created_at"]
        self.env_vars = res_json["environments"]
        return self

    def create(self) -> Pipeline:
        """
        Creates a new GlassFlow pipeline

        Returns:
            self: Pipeline object

        Raises:
            ValueError: If name is not provided in the constructor
            ValueError: If space_id is not provided in the constructor
            ValueError: If transformation_code or transformation_file are
                not provided in the constructor
        """
        create_pipeline = api.CreatePipeline(
            name=self.name,
            space_id=self.space_id,
            transformation_function=self.transformation_code,
            requirements_txt=self.requirements,
            source_connector=self.source_connector,
            sink_connector=self.sink_connector,
            environments=self.env_vars,
            state=self.state,
            metadata=self.metadata,
        )
        if self.name is None:
            raise ValueError("Name must be provided in order to create the pipeline")
        if self.space_id is None:
            raise ValueError(
                "Space_id must be provided in order to create the pipeline"
            )
        if self.transformation_code is None and self.transformation_file is None:
            raise ValueError(
                "Either transformation_code or transformation_file must be provided"
            )

        request = operations.CreatePipelineRequest(
            organization_id=self.organization_id,
            personal_access_token=self.personal_access_token,
            **create_pipeline.__dict__,
        )

        try:
            base_res = self.request(
                method="POST", endpoint="/pipelines", request=request
            )
            res = operations.CreatePipelineResponse(
                status_code=base_res.status_code,
                content_type=base_res.content_type,
                raw_response=base_res.raw_response,
                **base_res.raw_response.json(),
            )
        except errors.ClientError as e:
            if e.status_code == 401:
                raise errors.UnauthorizedError(e.raw_response) from e
            else:
                raise e

        self.id = res.id
        self.created_at = res.created_at
        self.access_tokens.append({"name": "default", "token": res.access_token})
        return self

    def delete(self) -> None:
        if self.id is None:
            raise ValueError("Pipeline id must be provided")

        request = operations.DeletePipelineRequest(
            pipeline_id=self.id,
            organization_id=self.organization_id,
            personal_access_token=self.personal_access_token,
        )

        try:
            self.request(
                method="DELETE",
                endpoint=f"/pipelines/{self.id}",
                request=request,
            )
        except errors.ClientError as e:
            if e.status_code == 404:
                raise errors.PipelineNotFoundError(self.id, e.raw_response) from e
            elif e.status_code == 401:
                raise errors.UnauthorizedError(e.raw_response) from e
            else:
                raise e
