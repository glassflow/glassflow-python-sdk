import pytest

from glassflow import PipelineDataSink, PipelineDataSource
from glassflow.models import errors
from glassflow.pipeline_data import PipelineDataClient


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


def test_validate_credentials_ok(requests_mock):
    data_client = PipelineDataClient(
        pipeline_id="test-id",
        pipeline_access_token="test-token",
    )
    requests_mock.get(
        data_client.glassflow_config.server_url
        + "/pipelines/test-id/status/access_token",
        status_code=200,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": data_client.pipeline_access_token,
        },
    )
    data_client.validate_credentials()


def test_push_to_pipeline_data_source_ok(requests_mock):
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


def test_push_to_pipeline_data_source_fail_with_404(requests_mock):
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


def test_push_to_pipeline_data_source_fail_with_401(requests_mock):
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


def test_consume_from_pipeline_data_sink_ok(requests_mock, consume_payload):
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
    assert res.body.event_context.request_id == consume_payload["req_id"]


def test_consume_from_pipeline_data_sink_fail_with_404(requests_mock):
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


def test_consume_from_pipeline_data_sink_fail_with_401(requests_mock):
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


def test_consume_from_pipeline_data_sink_ok_with_empty_response(requests_mock):
    sink = PipelineDataSink(
        pipeline_id="test-id",
        pipeline_access_token="test-access-token",
    )
    requests_mock.post(
        sink.glassflow_config.server_url
        + "/pipelines/test-id/topics/output/events/consume",
        status_code=204,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": "test-access-token",
        },
    )

    res = sink.consume()

    assert res.status_code == 204
    assert res.body is None


def test_consume_from_pipeline_data_sink_ok_with_too_many_requests(requests_mock):
    sink = PipelineDataSink(
        pipeline_id="test-id",
        pipeline_access_token="test-access-token",
    )
    requests_mock.post(
        sink.glassflow_config.server_url
        + "/pipelines/test-id/topics/output/events/consume",
        status_code=429,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": "test-access-token",
        },
    )

    res = sink.consume()

    assert res.status_code == 429
    assert res.body is None


def test_consume_failed_from_pipeline_data_sink_ok(requests_mock, consume_payload):
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
    assert res.body.event_context.request_id == consume_payload["req_id"]


def test_consume_failed_from_pipeline_data_sink_ok_with_empty_response(requests_mock):
    sink = PipelineDataSink(
        pipeline_id="test-id",
        pipeline_access_token="test-access-token",
    )
    requests_mock.post(
        sink.glassflow_config.server_url
        + "/pipelines/test-id/topics/failed/events/consume",
        status_code=204,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": "test-access-token",
        },
    )

    res = sink.consume_failed()

    assert res.status_code == 204
    assert res.body is None


def test_consume_failed_from_pipeline_data_sink_ok_with_too_many_requests(
    requests_mock,
):
    sink = PipelineDataSink(
        pipeline_id="test-id",
        pipeline_access_token="test-access-token",
    )
    requests_mock.post(
        sink.glassflow_config.server_url
        + "/pipelines/test-id/topics/failed/events/consume",
        status_code=429,
        headers={
            "Content-Type": "application/json",
            "X-pipeline-access-token": "test-access-token",
        },
    )

    res = sink.consume_failed()

    assert res.status_code == 429
    assert res.body is None


def test_consume_failed_from_pipeline_data_sink_fail_with_404(requests_mock):
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


def test_consume_failed_from_pipeline_data_sink_fail_with_401(requests_mock):
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
