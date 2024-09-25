from glassflow import GlassFlowClient
import pytest


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
