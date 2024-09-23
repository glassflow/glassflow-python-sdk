import pytest

from glassflow import GlassFlowClient
from glassflow.models import errors


@pytest.fixture
def pipeline():
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


def test_get_pipeline_ok(requests_mock, pipeline):
    client = GlassFlowClient()
    requests_mock.get(
        client.glassflow_config.server_url + '/pipelines/test-id',
        json=pipeline,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )

    pipeline = client.get_pipeline(pipeline_id="test-id")

    assert pipeline.id == "test-id"


def test_get_pipeline_404(requests_mock, pipeline):
    client = GlassFlowClient()
    requests_mock.get(
        client.glassflow_config.server_url + '/pipelines/test-id',
        json=pipeline,
        status_code=404,
        headers={"Content-Type": "application/json"},
    )

    with pytest.raises(errors.PipelineNotFoundError):
        client.get_pipeline(pipeline_id="test-id")


def test_get_pipeline_401(requests_mock, pipeline):
    client = GlassFlowClient()
    requests_mock.get(
        client.glassflow_config.server_url + '/pipelines/test-id',
        json=pipeline,
        status_code=401,
        headers={"Content-Type": "application/json"},
    )

    with pytest.raises(errors.UnauthorizedError):
        client.get_pipeline(pipeline_id="test-id")
