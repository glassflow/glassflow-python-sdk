from .clienterror import ClientError, requests_http


class SecretNotFoundError(ClientError):
    """Error caused by a Secret Key not found."""

    def __init__(self, secret_key: str, raw_response: requests_http.Response):
        super().__init__(
            detail=f"Secret Key {secret_key} does not exist",
            status_code=404,
            body=raw_response.text,
            raw_response=raw_response,
        )


class SecretUnauthorizedError(ClientError):
    """Secret operation not authorized, invalid Personal Access Token"""

    def __init__(self, secret_key: str, raw_response: requests_http.Response):
        super().__init__(
            detail=f"Unauthorized request on Secret {secret_key}, "
            f"Personal Access Token used is invalid",
            status_code=raw_response.status_code,
            body=raw_response.text,
            raw_response=raw_response,
        )
