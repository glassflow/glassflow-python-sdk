import dataclasses
import sys
from .models import operations, errors
from typing import Dict, Optional
import glassflow.utils as utils
import random
import time


class PipelineClient():
    """Client object to publish and consume events from the given pipeline.

    Attributes:
        glassflow_client: GlassFlowClient object to interact with GlassFlow API
        pipeline_id: The pipeline id to interact with
        organization_id: Organization ID of the user. If not provided, the default organization will be used
        pipeline_access_token: The access token to access the pipeline
    """

    def __init__(self, glassflow_client, pipeline_id: str,
                 pipeline_access_token: str) -> None:
        """Create a new PipelineClient object to interact with a specific pipeline

        Args:
            glassflow_client: GlassFlowClient object to interact with GlassFlow API
            pipeline_id: The pipeline id to interact with
            pipeline_access_token: The access token to access the pipeline
        """
        self.glassflow_client = glassflow_client
        self.pipeline_id = pipeline_id
        self.organization_id = self.glassflow_client.organization_id
        self.pipeline_access_token = pipeline_access_token
        # retry delay for consuming messages (in seconds)
        self._consume_retry_delay_minimum = 1
        self._consume_retry_delay_current = 1
        self._consume_retry_delay_max = 60

    def publish(self, request_body: dict) -> operations.PublishEventResponse:
        """Push a new message into the pipeline

        Args:
            request_body: The message to be published into the pipeline

        Returns:
            PublishEventResponse: Response object containing the status code and the raw response

        Raises:
            ClientError: If an error occurred while publishing the event
        """
        request = operations.PublishEventRequest(
            organization_id=self.organization_id,
            pipeline_id=self.pipeline_id,
            x_pipeline_access_token=self.pipeline_access_token,
            request_body=request_body,
        )

        base_url = self.glassflow_client.glassflow_config.server_url

        url = utils.generate_url(
            operations.PublishEventRequest, base_url,
            '/pipelines/{pipeline_id}/topics/input/events', request)

        req_content_type, data, form = utils.serialize_request_body(
            request, operations.PublishEventRequest, "request_body", False,
            True, 'json')

        headers = self._get_headers(request, req_content_type)
        query_params = utils.get_query_params(operations.PublishEventRequest,
                                              request)

        client = self.glassflow_client.glassflow_config.client

        http_res = client.request('POST',
                                  url,
                                  params=query_params,
                                  data=data,
                                  files=form,
                                  headers=headers)
        content_type = http_res.headers.get('Content-Type')

        res = operations.PublishEventResponse(status_code=http_res.status_code,
                                              content_type=content_type,
                                              raw_response=http_res)

        if http_res.status_code == 200:
            pass
        elif http_res.status_code in [400, 500]:
            if utils.match_content_type(content_type, 'application/json'):
                out = utils.unmarshal_json(http_res.text, errors.Error)
                out.raw_response = http_res
                raise out
            else:
                raise errors.ClientError(
                    f'unknown content-type received: {content_type}',
                    http_res.status_code, http_res.text, http_res)
        elif http_res.status_code == 401 or http_res.status_code == 404 or http_res.status_code >= 400 and http_res.status_code < 500 or http_res.status_code >= 500 and http_res.status_code < 600:
            raise errors.ClientError('API error occurred',
                                     http_res.status_code, http_res.text,
                                     http_res)

        return res

    def consume(self) -> operations.ConsumeEventResponse:
        """Consume the last message from the pipeline

        Returns:
            ConsumeEventResponse: Response object containing the status code and the raw response

        Raises:
            ClientError: If an error occurred while consuming the event

        """
        request = operations.ConsumeEventRequest(
            pipeline_id=self.pipeline_id,
            organization_id=self.organization_id,
            x_pipeline_access_token=self.pipeline_access_token,
        )

        base_url = self.glassflow_client.glassflow_config.server_url

        url = utils.generate_url(
            operations.ConsumeEventRequest, base_url,
            '/pipelines/{pipeline_id}/topics/output/events/consume', request)
        headers = self._get_headers(request)
        query_params = utils.get_query_params(operations.ConsumeEventRequest,
                                              request)

        client = self.glassflow_client.glassflow_config.client
        # make the request
        self._respect_retry_delay()

        http_res = client.request('POST',
                                  url,
                                  params=query_params,
                                  headers=headers)
        content_type = http_res.headers.get('Content-Type')

        res = operations.ConsumeEventResponse(status_code=http_res.status_code,
                                              content_type=content_type,
                                              raw_response=http_res)

        self._update_retry_delay(http_res.status_code)
        if http_res.status_code == 200:
            self._consume_retry_delay_current = self._consume_retry_delay_minimum
            if utils.match_content_type(content_type, 'application/json'):
                body = utils.unmarshal_json(
                    http_res.text,
                    Optional[operations.ConsumeEventResponseBody])
                res.body = body
            else:
                raise errors.ClientError(
                    f'unknown content-type received: {content_type}',
                    http_res.status_code, http_res.text, http_res)
        elif http_res.status_code == 204:
            # No messages to be consumed.
            # update the retry delay
            # Return an empty response body
            body = operations.ConsumeEventResponseBody("", "", {})
            res.body = body
        elif http_res.status_code == 429:
            # update the retry delay
            body = operations.ConsumeEventResponseBody("", "", {})
            res.body = body
        elif http_res.status_code in [400, 500]:
            if utils.match_content_type(content_type, 'application/json'):
                out = utils.unmarshal_json(http_res.text, errors.Error)
                out.raw_response = http_res
                raise out
            else:
                raise errors.ClientError(
                    f'unknown content-type received: {content_type}',
                    http_res.status_code, http_res.text, http_res)
        elif http_res.status_code == 401 or http_res.status_code == 404 or http_res.status_code >= 400 and http_res.status_code < 500 or http_res.status_code >= 500 and http_res.status_code < 600:
            raise errors.ClientError('API error occurred',
                                     http_res.status_code, http_res.text,
                                     http_res)

        return res

    def consume_failed(self) -> operations.ConsumeFailedResponse:
        """Consume the failed message from the pipeline

        Returns:
            ConsumeFailedResponse: Response object containing the status code and the raw response

        Raises:
            ClientError: If an error occurred while consuming the event

        """
        request = operations.ConsumeFailedRequest(
            pipeline_id=self.pipeline_id,
            organization_id=self.organization_id,
            x_pipeline_access_token=self.pipeline_access_token,
        )

        base_url = self.glassflow_client.glassflow_config.server_url

        url = utils.generate_url(
            operations.ConsumeFailedRequest, base_url,
            '/pipelines/{pipeline_id}/topics/failed/events/consume', request)
        headers = self._get_headers(request)
        query_params = utils.get_query_params(operations.ConsumeFailedRequest,
                                              request)

        client = self.glassflow_client.glassflow_config.client
        self._respect_retry_delay()
        http_res = client.request('POST',
                                  url,
                                  params=query_params,
                                  headers=headers)
        content_type = http_res.headers.get('Content-Type')

        res = operations.ConsumeFailedResponse(
            status_code=http_res.status_code,
            content_type=content_type,
            raw_response=http_res)

        self._update_retry_delay(http_res.status_code)
        if http_res.status_code == 200:
            if utils.match_content_type(content_type, 'application/json'):
                body = utils.unmarshal_json(
                    http_res.text,
                    Optional[operations.ConsumeFailedResponseBody])
                res.body = body
            else:
                raise errors.ClientError(
                    f'unknown content-type received: {content_type}',
                    http_res.status_code, http_res.text, http_res)
        elif http_res.status_code == 204:
            # No messages to be consumed. Return an empty response body
            body = operations.ConsumeFailedResponseBody("", "", {})
            res.body = body
        elif http_res.status_code in [400, 500]:
            if utils.match_content_type(content_type, 'application/json'):
                out = utils.unmarshal_json(http_res.text, errors.Error)
                out.raw_response = http_res
                raise out
            else:
                raise errors.ClientError(
                    f'unknown content-type received: {content_type}',
                    http_res.status_code, http_res.text, http_res)
        elif http_res.status_code == 401 or http_res.status_code == 404 or http_res.status_code >= 400 and http_res.status_code < 500 or http_res.status_code >= 500 and http_res.status_code < 600:
            raise errors.ClientError('API error occurred',
                                     http_res.status_code, http_res.text,
                                     http_res)

        return res

    def _get_headers(self,
                     request: dataclasses.dataclass,
                     req_content_type: Optional[str] = None) -> dict:

        headers = utils.get_req_specific_headers(request)
        headers['Accept'] = 'application/json'
        headers[
            'Gf-Client'] = self.glassflow_client.glassflow_config.glassflow_client
        headers[
            'User-Agent'] = self.glassflow_client.glassflow_config.user_agent
        headers['Gf-Python-Version'] = (f'{sys.version_info.major}.'
                                        f'{sys.version_info.minor}.'
                                        f'{sys.version_info.micro}')

        if (req_content_type and req_content_type
                not in ('multipart/form-data', 'multipart/mixed')):
            headers['content-type'] = req_content_type

        return headers

    def _update_retry_delay(self, status_code: int):
        if status_code == 200:
            self._consume_retry_delay_current = self._consume_retry_delay_minimum
        elif status_code == 204 or status_code == 429:
            self._consume_retry_delay_current *= 2
            self._consume_retry_delay_current = min(
                self._consume_retry_delay_current,
                self._consume_retry_delay_max)
            self._consume_retry_delay_current += random.uniform(0, 0.1)

    def _respect_retry_delay(self):
        if self._consume_retry_delay_current > self._consume_retry_delay_minimum:
            # sleep before making the request
            time.sleep(self._consume_retry_delay_current)
