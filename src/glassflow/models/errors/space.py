from .clienterror import ClientError, requests_http


class SpaceNotFoundError(ClientError):
    """Error caused by a space ID not found."""

    def __init__(self, space_id: str, raw_response: requests_http.Response):
        super().__init__(
            detail=f"Space ID {space_id} does not exist",
            status_code=404,
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


class SpaceUnauthorizedError(ClientError):
    """Space operation not authorized, invalid Personal Access Token"""

    def __init__(self, space_id: str, raw_response: requests_http.Response):
        super().__init__(
            detail=f"Unauthorized request on Space {space_id}, "
            f"Personal Access Token used is invalid",
            status_code=raw_response.status_code,
            body=raw_response.text,
            raw_response=raw_response,
        )
