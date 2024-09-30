import pytest

from glassflow import GlassFlowClient


@pytest.fixture
def client():
    return GlassFlowClient()


@pytest.fixture
def fetch_pipeline_response():
    return {
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
        "environments": [{"test-var": "test-var"}],
    }


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
def access_tokens():
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
