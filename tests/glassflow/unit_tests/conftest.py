import pytest

from glassflow import GlassFlowClient
from glassflow.models import api


@pytest.fixture
def client():
    return GlassFlowClient()


@pytest.fixture
def get_pipeline_request_mock(client, requests_mock, fetch_pipeline_response):
    return requests_mock.get(
        client.glassflow_config.server_url + "/pipelines/test-id",
        json=fetch_pipeline_response.model_dump(mode="json"),
        status_code=200,
        headers={"Content-Type": "application/json"},
    )


@pytest.fixture
def get_access_token_request_mock(
    client, requests_mock, fetch_pipeline_response, access_tokens_response
):
    return requests_mock.get(
        client.glassflow_config.server_url
        + f"/pipelines/{fetch_pipeline_response.id}/access_tokens",
        json=access_tokens_response,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )


@pytest.fixture
def get_pipeline_function_source_request_mock(
    client, requests_mock, fetch_pipeline_response, function_source_response
):
    return requests_mock.get(
        client.glassflow_config.server_url
        + f"/pipelines/{fetch_pipeline_response.id}/functions/main/artifacts/latest",
        json=function_source_response,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )


@pytest.fixture
def update_pipeline_request_mock(
    client, requests_mock, fetch_pipeline_response, update_pipeline_response
):
    return requests_mock.patch(
        client.glassflow_config.server_url + f"/pipelines/{fetch_pipeline_response.id}",
        json=update_pipeline_response.model_dump(mode="json"),
        status_code=200,
        headers={"Content-Type": "application/json"},
    )


@pytest.fixture
def fetch_pipeline_response():
    return api.GetDetailedSpacePipeline(
        **{
            "id": "test-id",
            "name": "test-name",
            "space_id": "test-space-id",
            "metadata": {},
            "created_at": "2024-09-23T10:08:45.529Z",
            "state": "running",
            "space_name": "test-space-name",
            "source_connector": {
                "kind": "google_pubsub",
                "config": {
                    "project_id": "test-project",
                    "subscription_id": "test-subscription",
                    "credentials_json": "credentials.json",
                },
            },
            "sink_connector": {
                "kind": "webhook",
                "config": {
                    "url": "www.test-url.com",
                    "method": "GET",
                    "headers": [
                        {"name": "header1", "value": "header1"},
                        {"name": "header2", "value": "header2"},
                    ],
                },
            },
            "environments": [{"name": "test-var", "value": "test-var"}],
        }
    )


@pytest.fixture
def update_pipeline_response(fetch_pipeline_response):
    fetch_pipeline_response.name = "updated name"
    fetch_pipeline_response.source_connector = api.SourceConnector(root=None)
    return fetch_pipeline_response


@pytest.fixture
def create_pipeline_response():
    return {
        "name": "test-name",
        "space_id": "string",
        "metadata": {"additionalProp1": {}},
        "id": "test-id",
        "created_at": "2024-09-23T10:08:45.529Z",
        "state": "running",
        "access_token": "string",
    }


@pytest.fixture
def create_space_response():
    return {
        "name": "test-space",
        "id": "test-space-id",
        "created_at": "2024-09-30T02:47:51.901Z",
    }


@pytest.fixture
def access_tokens_response():
    return {
        "total_amount": 2,
        "access_tokens": [
            {
                "name": "token1",
                "id": "string",
                "token": "string",
                "created_at": "2024-09-25T10:46:18.468Z",
            },
            {
                "name": "token2",
                "id": "string",
                "token": "string",
                "created_at": "2024-09-26T04:28:51.782Z",
            },
        ],
    }


@pytest.fixture
def function_source_response():
    return {
        "files": [{"name": "string", "content": "string"}],
        "transformation_function": "string",
        "requirements_txt": "string",
    }


@pytest.fixture
def get_logs_response():
    return {
        "logs": [
            {
                "level": "INFO",
                "severity_code": 0,
                "timestamp": "2024-09-30T16:04:08.211Z",
                "payload": {"message": "Info Message Log", "additionalProp1": {}},
            },
            {
                "level": "ERROR",
                "severity_code": 500,
                "timestamp": "2024-09-30T16:04:08.211Z",
                "payload": {"message": "Error Message Log", "additionalProp1": {}},
            },
        ],
        "next": "string",
    }


@pytest.fixture
def test_pipeline_response():
    return {
        "req_id": "string",
        "receive_time": "2024-11-06T09:37:46.310Z",
        "payload": {"message": "Test Message"},
        "event_context": {
            "request_id": "string",
            "receive_time": "2024-11-06T09:37:46.310Z",
            "external_id": "2141513fa4ed38wyfphce",
        },
        "status": "string",
        "response": {"message": "Test Response"},
        "error_details": "Error message",
        "stack_trace": "Error Stack trace",
    }


@pytest.fixture
def create_secret_response():
    return {
        "name": "test-name",
    }
