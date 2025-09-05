"""Mock response test data."""

from unittest.mock import MagicMock

import httpx


def create_mock_connection_error():
    """Create a mock connection error."""
    return httpx.ConnectError("Connection failed")


def create_mock_response_factory():
    """
    Create a factory for generating mock responses with
    specific status codes and text.
    """

    def factory(status_code=200, json_data=None, text=None):
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.text = text or ""
        mock_response.json.return_value = json_data or {}

        # For error status codes, we need to raise HTTPStatusError directly
        if status_code >= 400:
            # Create the error that will be raised when the response is used
            error = httpx.HTTPStatusError(
                f"{status_code} Error", request=MagicMock(), response=mock_response
            )
            # Make the response raise the error when raise_for_status is called
            mock_response.raise_for_status.side_effect = error
        else:
            mock_response.raise_for_status.return_value = None

        return mock_response

    return factory
