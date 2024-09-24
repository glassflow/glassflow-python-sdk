import pytest

from glassflow import PipelineDataSink, PipelineDataSource
from glassflow.models import errors


@pytest.fixture
def consume_payload():
    return {
        "req_id": "string",
        "receive_time": "2024-09-23T07:28:27.958Z",
        "payload": {},
        "event_context": {
            "request_id": "string",
            "external_id": "string",
            "receive_time": "2024-09-23T07:28:27.958Z",
        },
        "status": "string",
        "response": {},
    }


def test_pipeline_data_source_push_ok(requests_mock):
    source = PipelineDataSource(
        pipeline_id="test-id",
        pipeline_access_token="test-access-token",
    )
    requests_mock.post(
        source.glassflow_config.server_url + "/pipelines/test-id/topics/input/events",
        status_code=200,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": "test-access-token",
        },
    )

    res = source.publish({"test": "test"})

    assert res.status_code == 200
    assert res.content_type == "application/json"


def test_pipeline_data_source_push_404(requests_mock):
    source = PipelineDataSource(
        pipeline_id="test-id",
        pipeline_access_token="test-access-token",
    )
    requests_mock.post(
        source.glassflow_config.server_url + "/pipelines/test-id/topics/input/events",
        status_code=404,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": "test-access-token",
        },
    )

    with pytest.raises(errors.PipelineNotFoundError):
        source.publish({"test": "test"})


def test_pipeline_data_source_push_401(requests_mock):
    source = PipelineDataSource(
        pipeline_id="test-id",
        pipeline_access_token="test-access-token",
    )
    requests_mock.post(
        source.glassflow_config.server_url + "/pipelines/test-id/topics/input/events",
        status_code=401,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": "test-access-token",
        },
    )

    with pytest.raises(errors.PipelineAccessTokenInvalidError):
        source.publish({"test": "test"})


def test_pipeline_data_sink_consume_ok(requests_mock, consume_payload):
    sink = PipelineDataSink(
        pipeline_id="test-id",
        pipeline_access_token="test-access-token",
    )
    requests_mock.post(
        sink.glassflow_config.server_url
        + "/pipelines/test-id/topics/output/events/consume",
        json=consume_payload,
        status_code=200,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": "test-access-token",
        },
    )

    res = sink.consume()

    assert res.status_code == 200
    assert res.content_type == "application/json"
    assert res.body.req_id == consume_payload["req_id"]


def test_pipeline_data_sink_consume_404(requests_mock):
    sink = PipelineDataSink(
        pipeline_id="test-id",
        pipeline_access_token="test-access-token",
    )
    requests_mock.post(
        sink.glassflow_config.server_url
        + "/pipelines/test-id/topics/output/events/consume",
        json={"test-data": "test-data"},
        status_code=404,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": "test-access-token",
        },
    )

    with pytest.raises(errors.PipelineNotFoundError):
        sink.consume()


def test_pipeline_data_sink_consume_401(requests_mock):
    sink = PipelineDataSink(
        pipeline_id="test-id",
        pipeline_access_token="test-access-token",
    )
    requests_mock.post(
        sink.glassflow_config.server_url
        + "/pipelines/test-id/topics/output/events/consume",
        json={"test-data": "test-data"},
        status_code=401,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": "test-access-token",
        },
    )

    with pytest.raises(errors.PipelineAccessTokenInvalidError):
        sink.consume()


def test_pipeline_data_sink_consume_failed_ok(requests_mock, consume_payload):
    sink = PipelineDataSink(
        pipeline_id="test-id",
        pipeline_access_token="test-access-token",
    )
    requests_mock.post(
        sink.glassflow_config.server_url
        + "/pipelines/test-id/topics/failed/events/consume",
        json=consume_payload,
        status_code=200,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": "test-access-token",
        },
    )

    res = sink.consume_failed()

    assert res.status_code == 200
    assert res.content_type == "application/json"
    assert res.body.req_id == consume_payload["req_id"]


def test_pipeline_data_sink_consume_failed_404(requests_mock):
    sink = PipelineDataSink(
        pipeline_id="test-id",
        pipeline_access_token="test-access-token",
    )
    requests_mock.post(
        sink.glassflow_config.server_url
        + "/pipelines/test-id/topics/failed/events/consume",
        json={"test-data": "test-data"},
        status_code=404,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": "test-access-token",
        },
    )

    with pytest.raises(errors.PipelineNotFoundError):
        sink.consume_failed()


def test_pipeline_data_sink_consume_failed_401(requests_mock):
    sink = PipelineDataSink(
        pipeline_id="test-id",
        pipeline_access_token="test-access-token",
    )
    requests_mock.post(
        sink.glassflow_config.server_url
        + "/pipelines/test-id/topics/failed/events/consume",
        json={"test-data": "test-data"},
        status_code=401,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": "test-access-token",
        },
    )

    with pytest.raises(errors.PipelineAccessTokenInvalidError):
        sink.consume_failed()
