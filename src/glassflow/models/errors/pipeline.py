from .clienterror import ClientError, requests_http


class ConnectorConfigValueError(Exception):
    """Value error for missing connector settings."""

    def __init__(self, connector_type: str):
        super().__init__(
            f"{connector_type}_kind and {connector_type}_config "
            f"or {connector_type}_config_secret_refs must be provided"
        )


class PipelineNotFoundError(ClientError):
    """Error caused by a pipeline ID not found."""

    def __init__(self, pipeline_id: str, raw_response: requests_http.Response):
        super().__init__(
            detail=f"Pipeline ID {pipeline_id} does not exist",
            status_code=raw_response.status_code,
            body=raw_response.text,
            raw_response=raw_response,
        )


class PipelineUnauthorizedError(ClientError):
    """Pipeline operation not authorized, invalid Personal Access Token"""

    def __init__(self, pipeline_id: str, raw_response: requests_http.Response):
        super().__init__(
            detail=f"Unauthorized request on pipeline {pipeline_id}, "
            f"Personal Access Token used is invalid",
            status_code=raw_response.status_code,
            body=raw_response.text,
            raw_response=raw_response,
        )


class PipelineArtifactStillInProgressError(ClientError):
    """Error returned when the pipeline artifact is still being processed."""

    def __init__(self, pipeline_id: str, raw_response: requests_http.Response):
        super().__init__(
            detail=f"Artifact from pipeline {pipeline_id} "
            f"is still in process, try again later.",
            status_code=raw_response.status_code,
            body=raw_response.text,
            raw_response=raw_response,
        )


class PipelineTooManyRequestsError(ClientError):
    """Error caused by too many requests to a pipeline."""

    def __init__(self, raw_response: requests_http.Response):
        super().__init__(
            detail="Too many requests",
            status_code=raw_response.status_code,
            body=raw_response.text,
            raw_response=raw_response,
        )


class PipelineAccessTokenInvalidError(ClientError):
    """Error caused by invalid access token."""

    def __init__(self, raw_response: requests_http.Response):
        super().__init__(
            detail="The Pipeline Access Token used is invalid",
            status_code=raw_response.status_code,
            body=raw_response.text,
            raw_response=raw_response,
        )
