import random
import time
from pathlib import PurePosixPath

import requests

from .api_client import APIClient
from .models import errors
from .models.operations.v2 import (
    ConsumeEventResponse,
    ConsumeFailedResponse,
    PublishEventResponse,
)


class PipelineDataClient(APIClient):
    """Base Client object to publish and consume events from the given pipeline.

    Attributes:
        glassflow_config: GlassFlowConfig object to interact with GlassFlow API
        pipeline_id: The pipeline id to interact with
        pipeline_access_token: The access token to access the pipeline
    """

    def __init__(self, pipeline_id: str, pipeline_access_token: str):
        super().__init__()
        self.pipeline_id = pipeline_id
        self.pipeline_access_token = pipeline_access_token
        self.request_headers = {"X-PIPELINE-ACCESS-TOKEN": self.pipeline_access_token}

    def validate_credentials(self) -> None:
        """
        Check if the pipeline credentials are valid and raise an error if not
        """

        endpoint = f"pipelines/{self.pipeline_id}/status/access_token"
        return self._request2(method="GET", endpoint=endpoint)

    def _request2(
        self, method, endpoint, request_headers=None, body=None, query_params=None
    ):
        # updated request method that knows the request details and does not use utils
        # Do the https request. check for errors. if no errors, return the raw response http object that the caller can
        # map to a pydantic object

        headers = self._get_headers2()
        headers.update(self.request_headers)
        if request_headers:
            headers.update(request_headers)
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
            if http_err.response.status_code == 404:
                raise errors.PipelineNotFoundError(self.pipeline_id, http_err.response)
            if http_err.response.status_code in [400, 500]:
                errors.PipelineUnknownError(self.pipeline_id, http_err.response)


class PipelineDataSource(PipelineDataClient):
    def publish(self, request_body: dict) -> PublishEventResponse:
        """Push a new message into the pipeline

        Args:
            request_body: The message to be published into the pipeline

        Returns:
            PublishEventResponse: Response object containing the status
                code and the raw response

        Raises:
            ClientError: If an error occurred while publishing the event
        """
        endpoint = f"pipelines/{self.pipeline_id}/topics/input/events"
        http_res = self._request2(method="POST", endpoint=endpoint, body=request_body)
        content_type = http_res.headers.get("Content-Type")

        return PublishEventResponse(
            status_code=http_res.status_code,
            content_type=content_type,
            raw_response=http_res,
        )


class PipelineDataSink(PipelineDataClient):
    def __init__(self, pipeline_id: str, pipeline_access_token: str):
        super().__init__(pipeline_id, pipeline_access_token)

        # retry delay for consuming messages (in seconds)
        self._consume_retry_delay_minimum = 1
        self._consume_retry_delay_current = 1
        self._consume_retry_delay_max = 60

    def consume(self) -> ConsumeEventResponse:
        """Consume the last message from the pipeline

        Returns:
            ConsumeEventResponse: Response object containing the status
                code and the raw response

        Raises:
            ClientError: If an error occurred while consuming the event

        """

        endpoint = f"pipelines/{self.pipeline_id}/topics/output/events/consume"
        self._respect_retry_delay()
        http_res = self._request2(method="POST", endpoint=endpoint)
        content_type = http_res.headers.get("Content-Type")
        self._update_retry_delay(http_res.status_code)

        res = ConsumeEventResponse(
            status_code=http_res.status_code,
            content_type=content_type,
            raw_response=http_res,
        )
        if http_res.status_code == 200:
            res.body = http_res.json()
            self._consume_retry_delay_current = self._consume_retry_delay_minimum
        elif res.status_code == 204:
            res.body = None
        elif res.status_code == 429:
            # TODO update the retry delay
            res.body = None
        return res

    def consume_failed(self) -> ConsumeFailedResponse:
        """Consume the failed message from the pipeline

        Returns:
            ConsumeFailedResponse: Response object containing the status
                code and the raw response

        Raises:
            ClientError: If an error occurred while consuming the event

        """

        self._respect_retry_delay()
        endpoint = f"pipelines/{self.pipeline_id}/topics/failed/events/consume"
        http_res = self._request2(method="POST", endpoint=endpoint)
        content_type = http_res.headers.get("Content-Type")
        res = ConsumeFailedResponse(
            status_code=http_res.status_code,
            content_type=content_type,
            raw_response=http_res,
        )

        self._update_retry_delay(res.status_code)
        if res.status_code == 200:
            res.body = http_res.json()
            self._consume_retry_delay_current = self._consume_retry_delay_minimum
        return res

    def _update_retry_delay(self, status_code: int):
        if status_code == 200:
            self._consume_retry_delay_current = self._consume_retry_delay_minimum
        elif status_code == 204 or status_code == 429:
            self._consume_retry_delay_current *= 2
            self._consume_retry_delay_current = min(
                self._consume_retry_delay_current, self._consume_retry_delay_max
            )
            self._consume_retry_delay_current += random.uniform(0, 0.1)

    def _respect_retry_delay(self):
        if self._consume_retry_delay_current > self._consume_retry_delay_minimum:
            # sleep before making the request
            time.sleep(self._consume_retry_delay_current)
