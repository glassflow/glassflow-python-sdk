from __future__ import annotations

import json
from typing import Any

import httpx

from . import errors
from .models import GlassFlowConfig
from .tracking import Tracking


class APIClient:
    """
    API client
    """

    version = "1"
    glassflow_config = GlassFlowConfig()
    _tracking = Tracking(glassflow_config.analytics.distinct_id)

    def __init__(self, host: str | None = None):
        """Initialize the API Client class.

        Args:
            host: Host URL of the GlassFlow Clickhouse ETL service
        """
        self.host = host if host else self.glassflow_config.glassflow.host
        self.http_client = httpx.Client(base_url=self.host)

    def _request(
        self, method: str, endpoint: str, **kwargs: Any
    ) -> httpx.Response | None:
        """
        Generic request method with centralized error handling.

        Args:
            method: HTTP method (GET, POST, DELETE, etc.)
            endpoint: API endpoint
            **kwargs: Additional arguments to pass to httpx

        Returns:
            httpx.Response: The response object

        Raises:
            httpx.HTTPStatusError: If the API request fails with HTTP errors
                (to be handled by subclasses)
            RequestError: If there is a network error
        """
        try:
            response = self.http_client.request(method, endpoint, **kwargs)
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            self._raise_api_error(e.response)
        except httpx.RequestError as e:
            self._track_event("RequestError", error_type="ConnectionError")
            raise errors.ConnectionError(
                "Failed to connect to GlassFlow ETL API"
            ) from e

    @staticmethod
    def _raise_api_error(response: httpx.Response) -> None:
        """Raise an APIError based on the response."""
        status_code = response.status_code
        try:
            message = response.json().get("message", None)
        except json.JSONDecodeError:
            message = f"{status_code} {response.reason_phrase}"
        if status_code == 400:
            raise errors.ValidationError(status_code, message, response=response)
        elif status_code == 403:
            raise errors.ForbiddenError(status_code, message, response=response)
        elif status_code == 404:
            raise errors.NotFoundError(status_code, message, response=response)
        elif status_code == 422:
            raise errors.UnprocessableContentError(
                status_code, message, response=response
            )
        elif status_code == 500:
            raise errors.ServerError(status_code, message, response=response)
        else:
            raise errors.APIError(
                status_code,
                message="An error occurred: "
                f"({status_code} {response.reason_phrase}) {message}",
                response=response,
            )

    def _track_event(self, event_name: str, **kwargs: Any) -> None:
        """Track an event with the given name and properties."""
        self._tracking.track_event(event_name, kwargs)
