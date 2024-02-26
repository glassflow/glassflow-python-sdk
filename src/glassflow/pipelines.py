
from .models import components, operations, errors
from typing import Dict, Optional
import glassflow.utils as utils

class PipelineClient():
    def __init__(self, glassflow_client, space_id: str, pipeline_id: str) -> None:
        self.glassflow_client = glassflow_client
        self.space_id = space_id
        self.pipeline_id = pipeline_id
        self.organization_id = self.glassflow_client.organization_id


    def get_pipeline(self) -> operations.GetPipelineResponse:
        r"""Get the pipeline"""
        request = operations.GetPipelineRequest(
            space_id=self.space_id,
            pipeline_id=self.pipeline_id,
            organization_id=self.organization_id,
        )

        base_url = self.glassflow_client.glassflow_config.server_url

        url = utils.generate_url(operations.GetPipelineRequest, base_url, '/spaces/{space_id}/pipelines/{pipeline_id}', request)
        headers = {}
        query_params = utils.get_query_params(operations.GetPipelineRequest, request)
        headers['Accept'] = 'application/json'
        headers['user-agent'] = self.glassflow_client.glassflow_config.user_agent
        client = self.glassflow_client.glassflow_config.client

        http_res = client.request('GET', url, params=query_params, headers=headers)
        content_type = http_res.headers.get('Content-Type')

        res = operations.GetPipelineResponse(status_code=http_res.status_code, content_type=content_type, raw_response=http_res)

        if http_res.status_code == 200:
            if utils.match_content_type(content_type, 'application/json'):
                out = utils.unmarshal_json(http_res.text, Optional[components.Pipeline])
                res.pipeline = out
            else:
                raise errors.ClientError(f'unknown content-type received: {content_type}', http_res.status_code, http_res.text, http_res)
        elif http_res.status_code in [400, 500]:
            if utils.match_content_type(content_type, 'application/json'):
                out = utils.unmarshal_json(http_res.text, errors.Error)
                out.raw_response = http_res
                raise out
            else:
                raise errors.ClientError(f'unknown content-type received: {content_type}', http_res.status_code, http_res.text, http_res)
        elif http_res.status_code == 401 or http_res.status_code == 404 or http_res.status_code >= 400 and http_res.status_code < 500 or http_res.status_code >= 500 and http_res.status_code < 600:
            raise errors.ClientError('API error occurred', http_res.status_code, http_res.text, http_res)

        return res


    def publish(self, request_body: dict, pipeline_access_token: str) -> operations.PublishEventResponse:
        r"""Push a new message into the pipeline"""
        request = operations.PublishEventRequest(
            organization_id=self.organization_id,
            space_id=self.space_id,
            pipeline_id=self.pipeline_id,
            x_pipeline_access_token=pipeline_access_token,
            request_body=request_body,
        )

        base_url = self.glassflow_client.glassflow_config.server_url

        url = utils.generate_url(operations.PublishEventRequest, base_url, '/spaces/{space_id}/pipelines/{pipeline_id}/topics/input/events', request)
        headers = utils.get_headers(request)

        req_content_type, data, form = utils.serialize_request_body(request, operations.PublishEventRequest, "request_body", False, True, 'json')
        if req_content_type not in ('multipart/form-data', 'multipart/mixed'):
            headers['content-type'] = req_content_type

        query_params = utils.get_query_params(operations.PublishEventRequest, request)
        headers['Accept'] = 'application/json'
        headers['user-agent'] = self.glassflow_client.glassflow_config.user_agent

        client = self.glassflow_client.glassflow_config.client

        http_res = client.request('POST', url, params=query_params, data=data, files=form, headers=headers)
        content_type = http_res.headers.get('Content-Type')

        res = operations.PublishEventResponse(status_code=http_res.status_code, content_type=content_type, raw_response=http_res)

        if http_res.status_code == 200:
            pass
        elif http_res.status_code in [400, 500]:
            if utils.match_content_type(content_type, 'application/json'):
                out = utils.unmarshal_json(http_res.text, errors.Error)
                out.raw_response = http_res
                raise out
            else:
                raise errors.ClientError(f'unknown content-type received: {content_type}', http_res.status_code, http_res.text, http_res)
        elif http_res.status_code == 401 or http_res.status_code == 404 or http_res.status_code >= 400 and http_res.status_code < 500 or http_res.status_code >= 500 and http_res.status_code < 600:
            raise errors.ClientError('API error occurred', http_res.status_code, http_res.text, http_res)

        return res


    def consume(self, pipeline_access_token: str) -> operations.ConsumeEventResponse:
        r"""Consume the last message from the pipeline"""
        request = operations.ConsumeEventRequest(
            space_id=self.space_id,
            pipeline_id=self.pipeline_id,
            organization_id=self.organization_id,
            x_pipeline_access_token=pipeline_access_token,
        )

        base_url = self.glassflow_client.glassflow_config.server_url

        url = utils.generate_url(operations.ConsumeEventRequest, base_url, '/spaces/{space_id}/pipelines/{pipeline_id}/topics/output/events/consume', request)
        print(url)
        headers = utils.get_headers(request)
        query_params = utils.get_query_params(operations.ConsumeEventRequest, request)
        headers['Accept'] = 'application/json'
        headers['user-agent'] = self.glassflow_client.glassflow_config.user_agent
        client = self.glassflow_client.glassflow_config.client
        http_res = client.request('POST', url, params=query_params, headers=headers)
        content_type = http_res.headers.get('Content-Type')
        print('content_type', content_type)
        res = operations.ConsumeEventResponse(status_code=http_res.status_code, content_type=content_type, raw_response=http_res)

        if http_res.status_code == 200:
            out = utils.unmarshal_json(http_res.text, Optional[operations.ConsumeEventResponseBody])
            print(out)
            if utils.match_content_type(content_type, 'application/json'):
                out = utils.unmarshal_json(http_res.text, Optional[operations.ConsumeEventResponseBody])
                res.object = out
            else:
                raise errors.ClientError(f'unknown content-type received: {content_type}', http_res.status_code, http_res.text, http_res)
        elif http_res.status_code in [400, 500]:
            if utils.match_content_type(content_type, 'application/json'):
                out = utils.unmarshal_json(http_res.text, errors.Error)
                out.raw_response = http_res
                raise out
            else:
                raise errors.ClientError(f'unknown content-type received: {content_type}', http_res.status_code, http_res.text, http_res)
        elif http_res.status_code == 401 or http_res.status_code == 404 or http_res.status_code >= 400 and http_res.status_code < 500 or http_res.status_code >= 500 and http_res.status_code < 600:
            raise errors.ClientError('API error occurred', http_res.status_code, http_res.text, http_res)

        return res