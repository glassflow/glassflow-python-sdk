import pytest

from glassflow import GlassFlowClient


@pytest.fixture
def client():
    return GlassFlowClient()


@pytest.fixture
def pipeline_dict():
    return {
        "id": "test-id",
        "name": "test-name",
        "space_id": "test-space-id",
        "metadata": {},
        "created_at": "",
        "state": "running",
        "space_name": "test-space-name",
        "source_connector": {},
        "sink_connector": {},
        "environments": [],
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
        "created_at": "2024-09-30T02:47:51.901Z"
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
