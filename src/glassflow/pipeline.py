from __future__ import annotations

import requests

from .client import APIClient
from .models import api, errors, operations, responses
from .models.responses.pipeline import AccessToken
from .pipeline_data import PipelineDataSink, PipelineDataSource


class Pipeline(APIClient):
    def __init__(
        self,
        personal_access_token: str,
        name: str | None = None,
        space_id: str | None = None,
        id: str | None = None,
        source_kind: str | None = None,
        source_config: dict | None = None,
        sink_kind: str | None = None,
        sink_config: dict | None = None,
        requirements: str | None = None,
        transformation_file: str | None = None,
        env_vars: list[dict[str, str]] | None = None,
        state: str = "running",
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
            created_at: Timestamp when the pipeline was created

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
        self.transformation_code = None
        self.transformation_file = transformation_file
        self.env_vars = env_vars
        self.state = state
        self.organization_id = organization_id
        self.metadata = metadata if metadata is not None else {}
        self.created_at = created_at
        self.access_tokens: list[AccessToken] = []

        self.headers = {"Personal-Access-Token": self.personal_access_token}
        self.query_params = {"organization_id": self.organization_id}
        if self.transformation_file is not None:
            self._read_transformation_file()

        if source_kind is not None and self.source_config is not None:
            self.source_connector = dict(
                kind=self.source_kind,
                config=self.source_config,
            )
        elif self.source_kind is None and self.source_config is None:
            self.source_connector = None
        else:
            raise ValueError("Both source_kind and source_config must be provided")

        if self.sink_kind is not None and self.sink_config is not None:
            self.sink_connector = dict(
                kind=sink_kind,
                config=sink_config,
            )
        elif self.sink_kind is None and self.sink_config is None:
            self.sink_connector = None
        else:
            raise ValueError("Both sink_kind and sink_config must be provided")

    def _request(
        self,
        method,
        endpoint,
        request_headers=None,
        json=None,
        request_query_params=None,
        files=None,
        data=None,
    ):
        headers = {**self.headers, **(request_headers or {})}
        query_params = {**self.query_params, **(request_query_params or {})}
        try:
            return super()._request(
                method=method,
                endpoint=endpoint,
                request_headers=headers,
                json=json,
                request_query_params=query_params,
                files=files,
                data=data,
            )
        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 404:
                raise errors.PipelineNotFoundError(
                    self.id, http_err.response
                ) from http_err
            if http_err.response.status_code == 401:
                raise errors.UnauthorizedError(http_err.response) from http_err
            raise http_err

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

        endpoint = f"/pipelines/{self.id}"
        http_res = self._request(method="GET", endpoint=endpoint)
        fetched_pipeline = api.GetDetailedSpacePipeline(**http_res.json())
        self._fill_pipeline_details(fetched_pipeline)
        # Fetch Pipeline Access Tokens
        self._list_access_tokens()
        # Fetch function source
        self._get_function_artifact()
        return self

    def create(self) -> Pipeline:
        """
        Creates a new GlassFlow pipeline

        Returns:
            self: Pipeline object

        Raises:
            ValueError: If name is not provided in the constructor
            ValueError: If space_id is not provided in the constructor
            ValueError: If transformation_file is not provided
                in the constructor
        """

        if self.name is None:
            raise ValueError("Name must be provided in order to create the pipeline")
        if self.space_id is None:
            raise ValueError("Argument space_id must be provided in the constructor")
        if self.transformation_file is None:
            raise ValueError(
                "Argument transformation_file must be provided in the constructor"
            )
        else:
            self._read_transformation_file()

        create_pipeline = api.CreatePipeline(
            name=self.name,
            space_id=self.space_id,
            transformation_function=self.transformation_code,
            requirements_txt=self.requirements,
            source_connector=self.source_connector,
            sink_connector=self.sink_connector,
            environments=self.env_vars,
            state=api.PipelineState(self.state),
            metadata=self.metadata,
        )
        endpoint = "/pipelines"
        http_res = self._request(
            method="POST", endpoint=endpoint, json=create_pipeline.model_dump()
        )
        res_json = http_res.json()
        # using custom operations model because api model does not exist
        res = operations.CreatePipeline(
            **res_json,
        )
        # it seems somebody doesn't lock his laptop
        self.id = res.id
        self.created_at = res.created_at
        self.space_id = res.space_id
        self.access_tokens.append(
            AccessToken(
                name="default",
                token=res.access_token,
                id="default",
                created_at=res.created_at,
            )
        )
        return self

    def update(
        self,
        name: str | None = None,
        state: str | None = None,
        transformation_file: str | None = None,
        requirements: str | None = None,
        metadata: dict | None = None,
        source_kind: str | None = None,
        source_config: dict | None = None,
        sink_kind: str | None = None,
        sink_config: dict | None = None,
        env_vars: list[dict[str, str]] | None = None,
    ) -> Pipeline:
        """
        Updates a GlassFlow pipeline

        Args:

            name: Name of the pipeline
            state: State of the pipeline after creation.
                It can be either "running" or "paused"
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
            metadata: Metadata of the pipeline

        Returns:
            self: Updated pipeline

        """
        self.fetch()
        if transformation_file is not None or requirements is not None:
            if transformation_file is not None:
                with open(transformation_file) as f:
                    file = f.read()
            else:
                file = self.transformation_code

            if requirements is None:
                requirements = self.requirements

            self._upload_function_artifact(file, requirements)
            self.requirements = requirements
            self.transformation_code = file

        if source_kind is not None:
            source_connector = dict(
                kind=source_kind,
                config=source_config,
            )
        else:
            source_connector = self.source_connector

        if sink_kind is not None:
            sink_connector = dict(
                kind=sink_kind,
                config=sink_config,
            )
        else:
            sink_connector = self.sink_connector

        if env_vars is not None:
            self._update_function(env_vars)

        # using custom model because api model does not exist
        pipeline_req = operations.UpdatePipelineRequest(
            name=name if name is not None else self.name,
            state=state if state is not None else self.state,
            metadata=metadata if metadata is not None else self.metadata,
            source_connector=source_connector,
            sink_connector=sink_connector,
        )

        endpoint = f"/pipelines/{self.id}"
        body = pipeline_req.model_dump_json(exclude_none=True)
        http_res = self._request(method="PATCH", endpoint=endpoint, data=body)
        # Fetch updated pipeline details and validate
        updated_pipeline = api.GetDetailedSpacePipeline(**http_res.json())
        self._fill_pipeline_details(updated_pipeline)
        return self

    def delete(self) -> None:
        """
        Deletes a GlassFlow pipeline

        Returns:

        Raises:
            ValueError: If ID is not provided in the constructor
            PipelineNotFoundError: If ID provided does not match any
                existing pipeline in GlassFlow
            UnauthorizedError: If the Personal Access Token is not
                provided or is invalid
        """
        if self.id is None:
            raise ValueError("Pipeline id must be provided")

        endpoint = f"/pipelines/{self.id}"
        self._request(method="DELETE", endpoint=endpoint)

    def get_logs(
        self,
        page_size: int = 50,
        page_token: str | None = None,
        severity_code: int = 100,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> responses.FunctionLogsResponse:
        """
        Get the pipeline's logs

        Args:
            page_size: Pagination size
            page_token: Page token filter (use for pagination)
            severity_code: Severity code filter (100, 200, 300, 400, 500)
            start_time: Start time filter
            end_time: End time filter

        Returns:
            PipelineFunctionsGetLogsResponse: Response with the logs
        """

        query_params = {
            "page_size": page_size,
            "page_token": page_token,
            "severity_code": severity_code,
            "start_time": start_time,
            "end_time": end_time,
        }
        endpoint = f"/pipelines/{self.id}/functions/main/logs"
        http_res = self._request(
            method="GET", endpoint=endpoint, request_query_params=query_params
        )
        base_res_json = http_res.json()
        logs = [responses.FunctionLogEntry(**entry) for entry in base_res_json["logs"]]
        return responses.FunctionLogsResponse(
            logs=logs,
            next=base_res_json["next"],
        )

    def _list_access_tokens(self) -> Pipeline:
        endpoint = f"/pipelines/{self.id}/access_tokens"
        http_res = self._request(method="GET", endpoint=endpoint)
        tokens = responses.ListAccessTokensResponse(**http_res.json())
        self.access_tokens = tokens.access_tokens
        return self

    def _get_function_artifact(self) -> Pipeline:
        """
        Fetch pipeline function source

        Returns:
            self: Pipeline with function source details
        """
        endpoint = f"/pipelines/{self.id}/functions/main/artifacts/latest"
        http_res = self._request(method="GET", endpoint=endpoint)
        res_json = http_res.json()
        self.transformation_code = res_json["transformation_function"]
        # you would never know what else was changed

        if "requirements_txt" in res_json:
            self.requirements = res_json["requirements_txt"]
        return self

    def _upload_function_artifact(self, file: str, requirements: str) -> None:
        files = {"file": file}
        data = {
            "requirementsTxt": requirements,
        }
        endpoint = f"/pipelines/{self.id}/functions/main/artifacts"
        self._request(method="POST", endpoint=endpoint, files=files, data=data)

    def _update_function(self, env_vars):
        """
        Patch pipeline function

        Args:
            env_vars: Environment variables to update

        Returns:
            self: Pipeline with updated function
        """
        endpoint = f"/pipelines/{self.id}/functions/main"
        body = api.PipelineFunctionOutput(environments=env_vars)
        http_res = self._request(
            method="PATCH", endpoint=endpoint, json=body.model_dump()
        )
        self.env_vars = http_res.json()["environments"]
        return self

    def get_source(
        self, pipeline_access_token_name: str | None = None
    ) -> PipelineDataSource:
        """
        Get source client to publish data to the pipeline

        Args:
            pipeline_access_token_name (str | None): Name of the pipeline
                access token to use. If not specified, the default token
                will be used

        Returns:
            PipelineDataSource: Source client to publish data to the pipeline

        Raises:
            ValueError: If pipeline id is not provided in the constructor
        """
        return self._get_data_client("source", pipeline_access_token_name)

    def get_sink(
        self, pipeline_access_token_name: str | None = None
    ) -> PipelineDataSink:
        """
        Get sink client to consume data from the pipeline

        Args:
            pipeline_access_token_name (str | None): Name of the pipeline
                access token to use. If not specified, the default token
                will be used

        Returns:
            PipelineDataSink: Sink client to consume data from the pipeline

        Raises:
            ValueError: If pipeline id is not provided in the constructor
        """
        return self._get_data_client("sink", pipeline_access_token_name)

    def _get_data_client(
        self, client_type: str, pipeline_access_token_name: str | None = None
    ) -> PipelineDataSource | PipelineDataSink:
        if self.id is None:
            raise ValueError("Pipeline id must be provided in the constructor")
        elif len(self.access_tokens) == 0:
            self._list_access_tokens()

        if pipeline_access_token_name is not None:
            for t in self.access_tokens:
                if t.name == pipeline_access_token_name:
                    token = t.token
                    break
            else:
                raise ValueError(
                    f"Token with name {pipeline_access_token_name} was not found"
                )
        else:
            token = self.access_tokens[0].token
        if client_type == "source":
            client = PipelineDataSource(
                pipeline_id=self.id,
                pipeline_access_token=token,
            )
        elif client_type == "sink":
            client = PipelineDataSink(
                pipeline_id=self.id,
                pipeline_access_token=token,
            )
        else:
            raise ValueError("client_type must be either source or sink")
        return client

    def _read_transformation_file(self):
        try:
            with open(self.transformation_file) as f:
                self.transformation_code = f.read()
        except FileNotFoundError:
            raise

    def _fill_pipeline_details(
        self, pipeline_details: api.GetDetailedSpacePipeline
    ) -> Pipeline:
        self.id = pipeline_details.id
        self.name = pipeline_details.name
        self.space_id = pipeline_details.space_id
        self.state = pipeline_details.state
        source_connector = pipeline_details.source_connector
        if source_connector.root:
            self.source_kind = source_connector.root.kind
            self.source_config = source_connector.root.config
        sink_connector = pipeline_details.sink_connector
        if sink_connector.root:
            self.sink_kind = sink_connector.root.kind
            self.sink_config = sink_connector.root.config
        self.created_at = pipeline_details.created_at
        self.env_vars = pipeline_details.environments

        return self

    def test(self, data: dict) -> responses.TestFunctionResponse:
        """
        Test a pipeline's function with a sample input JSON

        Args:
            data: Input JSON

        Returns:
            TestFunctionResponse: Test function response
        """
        endpoint = f"/pipelines/{self.id}/functions/main/test"
        request_body = data
        http_res = self._request(method="POST", endpoint=endpoint, json=request_body)
        base_res_json = http_res.json()
        print("response for test ", base_res_json)
        return responses.TestFunctionResponse(
            **base_res_json,
        )
