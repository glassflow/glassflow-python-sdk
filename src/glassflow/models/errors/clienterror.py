import requests as requests_http


class ClientError(Exception):
    """Represents an error returned by the API.

    Attributes:
        detail: A message describing the error
        status_code: The status code of the response
        body: The response body
        raw_response: The raw response object

    """

    detail: str
    status_code: int
    body: str
    raw_response: requests_http.Response

    def __init__(
        self,
        detail: str,
        status_code: int,
        body: str,
        raw_response: requests_http.Response,
    ):
        """Create a new ClientError object

        Args:
            detail: A message describing the error
            status_code: The status code of the response
            body: The response body
            raw_response: The raw response object
        """
        self.detail = detail
        self.status_code = status_code
        self.body = body
        self.raw_response = raw_response

    def __str__(self):
        """Return a string representation of the error

        Returns:
            str: The string representation of the error

        """
        body = ""
        if len(self.body) > 0:
            body = f"\n{self.body}"

        return f"{self.detail}: Status {self.status_code}{body}"


class PipelineNotFoundError(ClientError):
    """Error caused by a pipeline ID not found."""

    def __init__(self, pipeline_id: str, raw_response: requests_http.Response):
        super().__init__(
            detail=f"Pipeline ID {pipeline_id} does not exist",
            status_code=404,
            body=raw_response.text,
            raw_response=raw_response,
        )


class SpaceNotFoundError(ClientError):
    """Error caused by a pipeline ID not found."""

    def __init__(self, space_id: str, raw_response: requests_http.Response):
        super().__init__(
            detail=f"Space ID {space_id} does not exist",
            status_code=404,
            body=raw_response.text,
            raw_response=raw_response,
        )


class UnauthorizedError(ClientError):
    """Error caused by a user not authorized."""

    def __init__(self, raw_response: requests_http.Response):
        super().__init__(
            detail="Unauthorized request, Personal Access Token used is invalid",
            status_code=401,
            body=raw_response.text,
            raw_response=raw_response,
        )


class PipelineAccessTokenInvalidError(ClientError):
    """Error caused by invalid access token."""

    def __init__(self, raw_response: requests_http.Response):
        super().__init__(
            detail="The Pipeline Access Token used is invalid",
            status_code=401,
            body=raw_response.text,
            raw_response=raw_response,
        )


class UnknownContentTypeError(ClientError):
    """Error caused by an unknown content type response."""

    def __init__(self, raw_response: requests_http.Response):
        content_type = raw_response.headers.get("Content-Type")
        super().__init__(
            detail=f"unknown content-type received: {content_type}",
            status_code=raw_response.status_code,
            body=raw_response.text,
            raw_response=raw_response,
        )


class SpaceIsNotEmptyError(ClientError):
    """Error caused by trying to delete a space that is not empty."""

    def __init__(self, raw_response: requests_http.Response):
        super().__init__(
            detail=raw_response.json()["msg"],
            status_code=409,
            body=raw_response.text,
            raw_response=raw_response,
        )
