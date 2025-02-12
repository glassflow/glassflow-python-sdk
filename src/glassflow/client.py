"""GlassFlow Python Client to interact with GlassFlow API"""

from .api_client import APIClient
from .models import errors, responses
from .pipeline import Pipeline
from .secret import Secret
from .space import Space


class GlassFlowClient(APIClient):
    """
    GlassFlow Client to interact with GlassFlow API and manage pipelines
    and other resources

    Attributes:
        client (requests.Session): Session object to make HTTP requests to GlassFlow API
        glassflow_config (GlassFlowConfig): GlassFlow config object to store configuration
        organization_id (str): Organization ID of the user. If not provided,
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

    def get_pipeline(self, pipeline_id: str) -> Pipeline:
        """Gets a Pipeline object from the GlassFlow API

        Args:
            pipeline_id: UUID of the pipeline

        Returns:
            Pipeline: Pipeline object from the GlassFlow API

        Raises:
            errors.PipelineNotFoundError: Pipeline does not exist
            errors.PipelineUnauthorizedError: User does not have permission to
                perform the requested operation
            errors.ClientError: GlassFlow Client Error
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
            errors.PipelineUnauthorizedError: User does not have permission to perform
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
        self, space_ids: list[str] = None
    ) -> responses.ListPipelinesResponse:
        """
        Lists all pipelines in the GlassFlow API

        Args:
            space_ids: List of Space IDs of the pipelines to list.
                If not specified, all the pipelines will be listed.

        Returns:
            responses.ListPipelinesResponse: Response object with the pipelines listed

        Raises:
            errors.PipelineUnauthorizedError: User does not have permission to
                perform the requested operation
        """

        endpoint = "/pipelines"
        query_params = {"space_id": space_ids} if space_ids else {}
        http_res = self._request(
            method="GET", endpoint=endpoint, request_query_params=query_params
        )
        return responses.ListPipelinesResponse(**http_res.json())

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
            errors.SpaceUnauthorizedError: User does not have permission to perform
                the requested operation
        """
        return Space(
            name=name,
            personal_access_token=self.personal_access_token,
            organization_id=self.organization_id,
        ).create()

    def list_spaces(self) -> responses.ListSpacesResponse:
        """
        Lists all GlassFlow spaces in the GlassFlow API

        Returns:
            response.ListSpacesResponse: Response object with the spaces listed

        Raises:
            errors.SpaceUnauthorizedError: User does not have permission to perform the
                requested operation
        """

        endpoint = "/spaces"
        http_res = self._request(method="GET", endpoint=endpoint)
        return responses.ListSpacesResponse(**http_res.json())

    def create_secret(self, key: str, value: str) -> Secret:
        """
        Creates a new secret

        Args:
            key: Secret key (must be unique in your organization)
            value: Secret value

        Returns:
            Secret: New secret

        Raises:
            errors.SecretUnauthorizedError: User does not have permission to perform the
                requested operation
        """
        return Secret(
            key=key,
            value=value,
            personal_access_token=self.personal_access_token,
            organization_id=self.organization_id,
        ).create()

    def list_secrets(self) -> responses.ListSecretsResponse:
        """
        Lists all GlassFlow secrets in the GlassFlow API

        Returns:
            responses.ListSecretsResponse: Response object with the secrets listed

        Raises:
            errors.SecretUnauthorizedError: User does not have permission to perform the
                requested operation
        """
        endpoint = "/secrets"
        http_res = self._request(method="GET", endpoint=endpoint)
        return responses.ListSecretsResponse(**http_res.json())

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
        headers = {**self.request_headers, **(request_headers or {})}
        query_params = {**self.request_query_params, **(request_query_params or {})}

        try:
            http_res = super()._request(
                method=method,
                endpoint=endpoint,
                request_headers=headers,
                json=json,
                request_query_params=query_params,
                files=files,
                data=data,
            )
            return http_res
        except errors.UnknownError as e:
            if e.status_code == 401:
                raise errors.UnauthorizedError(e.raw_response) from e
            raise e
