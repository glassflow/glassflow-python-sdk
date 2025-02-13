import random
import time

from .api_client import APIClient
from .models import errors, responses


class PipelineDataClient(APIClient):
    """Base Client object to publish and consume events from the given pipeline.

    Attributes:
        glassflow_config (GlassFlowConfig): GlassFlowConfig object to interact
            with GlassFlow API
        pipeline_id (str): The pipeline id to interact with
        pipeline_access_token (str): The access token to access the pipeline
    """

    def __init__(self, pipeline_id: str, pipeline_access_token: str):
        super().__init__()
        self.pipeline_id = pipeline_id
        self.pipeline_access_token = pipeline_access_token
        self.headers = {"X-PIPELINE-ACCESS-TOKEN": self.pipeline_access_token}
        self.query_params = {}

    def validate_credentials(self) -> None:
        """
        Check if the pipeline credentials are valid and raise an error if not
        """

        endpoint = f"/pipelines/{self.pipeline_id}/status/access_token"
        return self._request(method="GET", endpoint=endpoint)

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
        headers = {**self.headers, **(request_headers or {})}
        query_params = {**self.query_params, **(request_query_params or {})}
        try:
            return super()._request(
                method=method,
                endpoint=endpoint,
                request_headers=headers,
                json=json,
                request_query_params=query_params,
                files=files,
                data=data,
            )
        except errors.UnknownError as e:
            if e.status_code == 401:
                raise errors.PipelineAccessTokenInvalidError(e.raw_response) from e
            if e.status_code == 404:
                raise errors.PipelineNotFoundError(
                    self.pipeline_id, e.raw_response
                ) from e
            if e.status_code == 429:
                return errors.PipelineTooManyRequestsError(e.raw_response)
            raise e


class PipelineDataSource(PipelineDataClient):
    def publish(self, request_body: dict) -> responses.PublishEventResponse:
        """Push a new message into the pipeline

        Args:
            request_body: The message to be published into the pipeline

        Returns:
            Response object containing the status code and the raw response

        Raises:
            errors.ClientError: If an error occurred while publishing the event
        """
        endpoint = f"/pipelines/{self.pipeline_id}/topics/input/events"
        http_res = self._request(method="POST", endpoint=endpoint, json=request_body)
        return responses.PublishEventResponse(
            status_code=http_res.status_code,
        )


class PipelineDataSink(PipelineDataClient):
    def __init__(self, pipeline_id: str, pipeline_access_token: str):
        super().__init__(pipeline_id, pipeline_access_token)

        # retry delay for consuming messages (in seconds)
        self._consume_retry_delay_minimum = 1
        self._consume_retry_delay_current = 1
        self._consume_retry_delay_max = 60

    def consume(self) -> responses.ConsumeEventResponse:
        """Consume the last message from the pipeline

        Returns:
            Response object containing the status code and the raw response

        Raises:
            errors.ClientError: If an error occurred while consuming the event

        """

        endpoint = f"/pipelines/{self.pipeline_id}/topics/output/events/consume"
        self._respect_retry_delay()
        http_res = self._request(method="POST", endpoint=endpoint)
        self._update_retry_delay(http_res.status_code)

        body = None
        if http_res.status_code == 200:
            body = http_res.json()
            self._consume_retry_delay_current = self._consume_retry_delay_minimum

        return responses.ConsumeEventResponse(
            status_code=http_res.status_code, body=body
        )

    def consume_failed(self) -> responses.ConsumeFailedResponse:
        """Consume the failed message from the pipeline

        Returns:
            Response object containing the status code and the raw response

        Raises:
            errors.ClientError: If an error occurred while consuming the event

        """

        self._respect_retry_delay()
        endpoint = f"/pipelines/{self.pipeline_id}/topics/failed/events/consume"
        http_res = self._request(method="POST", endpoint=endpoint)

        self._update_retry_delay(http_res.status_code)
        body = None
        if http_res.status_code == 200:
            body = http_res.json()
            self._consume_retry_delay_current = self._consume_retry_delay_minimum

        return responses.ConsumeFailedResponse(
            status_code=http_res.status_code, body=body
        )

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
