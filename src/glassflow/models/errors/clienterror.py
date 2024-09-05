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
