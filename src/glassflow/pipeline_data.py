import random
import time
from typing import Optional

from .api_client import APIClient
from .models import errors, operations
from .models.operations.base import BasePipelineDataRequest, BaseResponse
from .utils import utils


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

    def validate_credentials(self) -> None:
        """
        Check if the pipeline credentials are valid and raise an error if not
        """
        request = operations.StatusAccessTokenRequest(
            pipeline_id=self.pipeline_id,
            x_pipeline_access_token=self.pipeline_access_token,
        )
        self._request(
            method="GET",
            endpoint="/pipelines/{pipeline_id}/status/access_token",
            request=request,
        )

    def _request(
        self, method: str, endpoint: str, request: BasePipelineDataRequest, **kwargs
    ) -> BaseResponse:
        try:
            res = super()._request(method, endpoint, request, **kwargs)
        except errors.ClientError as e:
            if e.status_code == 401:
                raise errors.PipelineAccessTokenInvalidError(e.raw_response) from e
            elif e.status_code == 404:
                raise errors.PipelineNotFoundError(
                    self.pipeline_id, e.raw_response
                ) from e
            else:
                raise e
        return res


class PipelineDataSource(PipelineDataClient):
    def publish(self, request_body: dict) -> operations.PublishEventResponse:
        """Push a new message into the pipeline

        Args:
            request_body: The message to be published into the pipeline

        Returns:
            PublishEventResponse: Response object containing the status
                code and the raw response

        Raises:
            ClientError: If an error occurred while publishing the event
        """
        request = operations.PublishEventRequest(
            pipeline_id=self.pipeline_id,
            x_pipeline_access_token=self.pipeline_access_token,
            request_body=request_body,
        )
        base_res = self._request(
            method="POST",
            endpoint="/pipelines/{pipeline_id}/topics/input/events",
            request=request,
        )

        return operations.PublishEventResponse(
            status_code=base_res.status_code,
            content_type=base_res.content_type,
            raw_response=base_res.raw_response,
        )


class PipelineDataSink(PipelineDataClient):
    def __init__(self, pipeline_id: str, pipeline_access_token: str):
        super().__init__(pipeline_id, pipeline_access_token)

        # retry delay for consuming messages (in seconds)
        self._consume_retry_delay_minimum = 1
        self._consume_retry_delay_current = 1
        self._consume_retry_delay_max = 60

    def consume(self) -> operations.ConsumeEventResponse:
        """Consume the last message from the pipeline

        Returns:
            ConsumeEventResponse: Response object containing the status
                code and the raw response

        Raises:
            ClientError: If an error occurred while consuming the event

        """
        request = operations.ConsumeEventRequest(
            pipeline_id=self.pipeline_id,
            x_pipeline_access_token=self.pipeline_access_token,
        )

        self._respect_retry_delay()
        base_res = self._request(
            method="POST",
            endpoint="/pipelines/{pipeline_id}/topics/output/events/consume",
            request=request,
        )

        res = operations.ConsumeEventResponse(
            status_code=base_res.status_code,
            content_type=base_res.content_type,
            raw_response=base_res.raw_response,
        )

        self._update_retry_delay(base_res.status_code)
        if res.status_code == 200:
            if not utils.match_content_type(res.content_type, "application/json"):
                raise errors.UnknownContentTypeError(res.raw_response)

            self._consume_retry_delay_current = self._consume_retry_delay_minimum
            body = utils.unmarshal_json(
                res.raw_response.text, Optional[operations.ConsumeEventResponseBody]
            )
            res.body = body
        elif res.status_code == 204:
            # No messages to be consumed.
            # update the retry delay
            # Return an empty response body
            body = operations.ConsumeEventResponseBody("", "", {})
            res.body = body
        elif res.status_code == 429:
            # update the retry delay
            body = operations.ConsumeEventResponseBody("", "", {})
            res.body = body
        elif not utils.match_content_type(res.content_type, "application/json"):
            raise errors.UnknownContentTypeError(res.raw_response)

        return res

    def consume_failed(self) -> operations.ConsumeFailedResponse:
        """Consume the failed message from the pipeline

        Returns:
            ConsumeFailedResponse: Response object containing the status
                code and the raw response

        Raises:
            ClientError: If an error occurred while consuming the event

        """
        request = operations.ConsumeFailedRequest(
            pipeline_id=self.pipeline_id,
            x_pipeline_access_token=self.pipeline_access_token,
        )

        self._respect_retry_delay()
        base_res = self._request(
            method="POST",
            endpoint="/pipelines/{pipeline_id}/topics/failed/events/consume",
            request=request,
        )

        res = operations.ConsumeFailedResponse(
            status_code=base_res.status_code,
            content_type=base_res.content_type,
            raw_response=base_res.raw_response,
        )

        self._update_retry_delay(res.status_code)
        if res.status_code == 200:
            if not utils.match_content_type(res.content_type, "application/json"):
                raise errors.UnknownContentTypeError(res.raw_response)

            self._consume_retry_delay_current = self._consume_retry_delay_minimum
            body = utils.unmarshal_json(
                res.raw_response.text, Optional[operations.ConsumeFailedResponseBody]
            )
            res.body = body
        elif res.status_code == 204:
            # No messages to be consumed. Return an empty response body
            body = operations.ConsumeFailedResponseBody("", "", {})
            res.body = body
        elif res.status_code == 429:
            # update the retry delay
            body = operations.ConsumeEventResponseBody("", "", {})
            res.body = body
        elif not utils.match_content_type(res.content_type, "application/json"):
            raise errors.UnknownContentTypeError(res.raw_response)
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
