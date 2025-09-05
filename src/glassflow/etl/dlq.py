from __future__ import annotations

from typing import Any, Dict, List

from . import errors
from .api_client import APIClient
from .errors import InvalidBatchSizeError


class DLQ(APIClient):
    """
    Dead Letter Queue client for managing failed messages.
    """

    def __init__(self, pipeline_id: str, host: str | None = None):
        super().__init__(host)
        self.pipeline_id = pipeline_id
        self.endpoint = f"/api/v1/pipeline/{self.pipeline_id}/dlq"
        self._max_batch_size = 100

    def consume(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        """
        Consume messages from the Dead Letter Queue.

        Args:
            batch_size: Number of messages to consume (between 1 and 1000)

        Returns:
            List of messages from the DLQ
        """
        if (
            not isinstance(batch_size, int)
            or batch_size < 1
            or batch_size > self._max_batch_size
        ):
            raise ValueError("batch_size must be an integer between 1 and 100")

        try:
            response = self._request(
                "GET", f"{self.endpoint}/consume", params={"batch_size": batch_size}
            )
            response.raise_for_status()
            if response.status_code != 204:
                return response.json()
            return []
        except errors.UnprocessableContentError as e:
            raise InvalidBatchSizeError(
                f"Invalid batch size: batch size should be larger than 1 "
                f"and smaller than {self._max_batch_size}"
            ) from e
        except errors.APIError as e:
            raise e

    def state(self) -> Dict[str, Any]:
        """
        Get the current state of the Dead Letter Queue.

        Returns:
            Dictionary containing DLQ state information

        Raises:
            ConnectionError: If there is a network error
            InternalServerError: If the API request fails
        """
        try:
            response = self._request("GET", f"{self.endpoint}/state")
            response.raise_for_status()
            return response.json()
        except errors.NotFoundError as e:
            raise errors.PipelineNotFoundError(
                status_code=e.status_code,
                message=f"Pipeline with id '{self.pipeline_id}' not found",
                response=e.response,
            ) from e
        except errors.APIError as e:
            raise e
