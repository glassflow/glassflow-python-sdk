"""GlassFlow Python Client to interact with GlassFlow API"""

from __future__ import annotations

from pathlib import PurePosixPath

import requests

from .api_client import APIClient
from .models import errors, responses
from .models.api import v2 as apiv2
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
        self.request_headers = {"Personal-Access-Token": self.personal_access_token}
        self.request_query_params = {"organization_id": self.organization_id}

    def _request2(
        self,
        method,
        endpoint,
        request_headers=None,
        body=None,
        request_query_params=None,
    ):
        # updated request method that knows the request details and does not use utils
        # Do the https request. check for errors. if no errors, return the raw response http object that the caller can
        # map to a pydantic object
        headers = self._get_headers2()
        headers.update(self.request_headers)
        if request_headers:
            headers.update(request_headers)

        query_params = self.request_query_params
        if request_query_params:
            query_params.update(request_query_params)

        url = (
            f"{self.glassflow_config.server_url.rstrip('/')}/{PurePosixPath(endpoint)}"
        )
        try:
            http_res = self.client.request(
                method, url=url, params=query_params, headers=headers, json=body
            )
            http_res.raise_for_status()
            return http_res
        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 401:
                raise errors.PipelineAccessTokenInvalidError(http_err.response)
            if http_err.response.status_code in [404, 400, 500]:
                raise errors.ClientError(
                    detail="Error in getting response from GlassFlow",
                    status_code=http_err.response.status_code,
                    body=http_err.response.text,
                    raw_response=http_err.response,
                )

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
            id=pipeline_id,
            organization_id=self.organization_id,
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
        state: str = "running",
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
    ) -> responses.ListPipelinesResponse:
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

        endpoint = "/pipelines"
        query_params = {}
        if space_ids:
            query_params = {"space_id": space_ids}
        http_res = self._request2(
            method="GET", endpoint=endpoint, request_query_params=query_params
        )
        res_json = http_res.json()
        pipeline_list = apiv2.ListPipelines(**res_json)
        return responses.ListPipelinesResponse(**pipeline_list.model_dump())

    def list_spaces(self) -> responses.ListSpacesResponse:
        """
        Lists all GlassFlow spaces in the GlassFlow API

        Returns:
            ListSpacesResponse: Response object with the spaces listed

        Raises:
            UnauthorizedError: User does not have permission to perform the
                requested operation
        """

        endpoint = "/spaces"
        http_res = self._request2(method="GET", endpoint=endpoint)
        res_json = http_res.json()
        spaces_list = apiv2.ListSpaceScopes(**res_json)
        return responses.ListSpacesResponse(**spaces_list.model_dump())

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
