import pytest

from glassflow import GlassFlowClient
from glassflow.models import errors


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
        "environments": []
    }


@pytest.fixture
def create_pipeline_response():
    return {
        "name": "test-name",
        "space_id": "string",
        "metadata": {
            "additionalProp1": {}
        },
        "id": "test-id",
        "created_at": "2024-09-23T10:08:45.529Z",
        "state": "running",
        "access_token": "string"
    }


def test_get_pipeline_ok(requests_mock, pipeline_dict):
    client = GlassFlowClient()
    requests_mock.get(
        client.glassflow_config.server_url + '/pipelines/test-id',
        json=pipeline_dict,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )

    pipeline = client.get_pipeline(pipeline_id="test-id")

    assert pipeline.id == "test-id"


def test_get_pipeline_404(requests_mock, pipeline_dict):
    client = GlassFlowClient()
    requests_mock.get(
        client.glassflow_config.server_url + '/pipelines/test-id',
        json=pipeline_dict,
        status_code=404,
        headers={"Content-Type": "application/json"},
    )

    with pytest.raises(errors.PipelineNotFoundError):
        client.get_pipeline(pipeline_id="test-id")


def test_get_pipeline_401(requests_mock, pipeline_dict):
    client = GlassFlowClient()
    requests_mock.get(
        client.glassflow_config.server_url + '/pipelines/test-id',
        json=pipeline_dict,
        status_code=401,
        headers={"Content-Type": "application/json"},
    )

    with pytest.raises(errors.UnauthorizedError):
        client.get_pipeline(pipeline_id="test-id")


def test_create_pipeline_ok(requests_mock, pipeline_dict, create_pipeline_response):
    client = GlassFlowClient()

    requests_mock.post(
        client.glassflow_config.server_url + '/pipelines',
        json=create_pipeline_response,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )
    pipeline = client.create_pipeline(
        name=create_pipeline_response["name"],
        space_id=create_pipeline_response["space_id"],
        transformation_code="transformation code...",
    )

    assert pipeline.id == "test-id"
    assert pipeline.name == "test-name"
