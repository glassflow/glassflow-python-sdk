import pytest

from glassflow import Pipeline, Secret, errors


def test_pipeline_with_transformation_file():
    try:
        p = Pipeline(
            transformation_file="tests/data/transformation.py",
            personal_access_token="test-token",
        )
        p._read_transformation_file()
        assert p.transformation_code is not None
    except Exception as e:
        pytest.fail(e)


def test_pipeline_fail_with_file_not_found():
    with pytest.raises(FileNotFoundError):
        p = Pipeline(
            transformation_file="fake_file.py", personal_access_token="test-token"
        )
        p._read_transformation_file()


def test_pipeline_fail_with_connection_config_value_error():
    with pytest.raises(errors.ConnectorConfigValueError):
        Pipeline(
            transformation_file="tests/data/transformation.py",
            personal_access_token="test-token",
            sink_kind="webhook",
        )

    with pytest.raises(errors.ConnectorConfigValueError):
        Pipeline(
            transformation_file="tests/data/transformation.py",
            personal_access_token="test-token",
            source_kind="google_pubsub",
        )

    with pytest.raises(errors.ConnectorConfigValueError):
        Pipeline(
            transformation_file="tests/data/transformation.py",
            personal_access_token="test-token",
            source_config={"url": "test-url"},
        )


def test_pipeline_with_connector_secret_ok():
    p = Pipeline(
        transformation_file="tests/data/transformation.py",
        personal_access_token="test-token",
        sink_kind="webhook",
        sink_config={
            "url": Secret(key="testKey", personal_access_token="test-token"),
            "method": "POST",
            "headers": [{"name": "Content-Type", "value": "application/json"}],
        },
        source_kind="google_pubsub",
        source_config={
            "project_id": "test-project-id",
            "subscription_id": "test-subscription-id",
            "credentials_json": Secret(
                key="testCredentials", personal_access_token="test-token"
            ),
        },
    )

    expected_source_connector = {
        "kind": "google_pubsub",
        "configuration": {
            "project_id": {"value": "test-project-id"},
            "subscription_id": {"value": "test-subscription-id"},
            "credentials_json": {
                "secret_ref": {"type": "organization", "key": "testCredentials"}
            },
        },
    }
    expected_sink_connector = {
        "kind": "webhook",
        "configuration": {
            "url": {"secret_ref": {"type": "organization", "key": "testKey"}},
            "method": {"value": "POST"},
            "headers": [{"name": "Content-Type", "value": "application/json"}],
        },
    }
    assert (
        p.source_connector.model_dump(mode="json", exclude_none=True)
        == expected_source_connector
    )
    assert (
        p.sink_connector.model_dump(mode="json", exclude_none=True)
        == expected_sink_connector
    )


def test_fetch_pipeline_ok(
    get_pipeline_request_mock,
    get_access_token_request_mock,
    get_pipeline_function_source_request_mock,
    fetch_pipeline_response,
    function_source_response,
):
    pipeline = Pipeline(
        id=fetch_pipeline_response.id,
        personal_access_token="test-token",
    ).fetch()

    assert pipeline.name == fetch_pipeline_response.name
    assert len(pipeline.access_tokens) > 0
    assert (
        pipeline.transformation_code
        == function_source_response["transformation_function"]
    )
    assert pipeline.requirements == function_source_response["requirements_txt"]


def test_fetch_pipeline_fail_with_404(requests_mock, fetch_pipeline_response, client):
    requests_mock.get(
        client.glassflow_config.server_url + "/pipelines/test-id",
        json=fetch_pipeline_response.model_dump(mode="json"),
        status_code=404,
        headers={"Content-Type": "application/json"},
    )

    with pytest.raises(errors.PipelineNotFoundError):
        Pipeline(
            id=fetch_pipeline_response.id,
            personal_access_token="test-token",
        ).fetch()


def test_fetch_pipeline_fail_with_401(requests_mock, fetch_pipeline_response, client):
    requests_mock.get(
        client.glassflow_config.server_url + "/pipelines/test-id",
        json=fetch_pipeline_response.model_dump(mode="json"),
        status_code=401,
        headers={"Content-Type": "application/json"},
    )

    with pytest.raises(errors.PipelineUnauthorizedError):
        Pipeline(
            id=fetch_pipeline_response.id,
            personal_access_token="test-token",
        ).fetch()


def test_create_pipeline_ok(
    requests_mock, fetch_pipeline_response, create_pipeline_response, client
):
    requests_mock.post(
        client.glassflow_config.server_url + "/pipelines",
        json=create_pipeline_response,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )
    pipeline = Pipeline(
        name=fetch_pipeline_response.name,
        space_id=create_pipeline_response["space_id"],
        transformation_file="tests/data/transformation.py",
        personal_access_token="test-token",
    ).create()

    assert pipeline.id == "test-id"
    assert pipeline.name == "test-name"


def test_create_pipeline_fail_with_missing_name(client):
    with pytest.raises(ValueError) as e:
        Pipeline(
            space_id="test-space-id",
            transformation_file="tests/data/transformation.py",
            personal_access_token="test-token",
        ).create()

    assert e.value.__str__() == (
        "Name must be provided in order to create the pipeline"
    )


def test_create_pipeline_fail_with_missing_space_id(client):
    with pytest.raises(ValueError) as e:
        Pipeline(
            name="test-name",
            transformation_file="tests/data/transformation.py",
            personal_access_token="test-token",
        ).create()

    assert str(e.value) == ("Argument space_id must be provided in the constructor")


def test_create_pipeline_fail_with_missing_transformation(client):
    with pytest.raises(ValueError) as e:
        Pipeline(
            name="test-name",
            space_id="test-space-id",
            personal_access_token="test-token",
        ).create()

    assert str(e.value) == (
        "Argument transformation_file must be provided in the constructor"
    )


def test_update_pipeline_ok(
    get_pipeline_request_mock,
    get_access_token_request_mock,
    get_pipeline_function_source_request_mock,
    update_pipeline_request_mock,
    fetch_pipeline_response,
    update_pipeline_response,
):
    pipeline = (
        Pipeline(personal_access_token="test-token")
        ._fill_pipeline_details(fetch_pipeline_response)
        .update()
    )

    assert pipeline.name == update_pipeline_response.name
    assert pipeline.source_connector == update_pipeline_response.source_connector


def test_delete_pipeline_ok(requests_mock, client):
    requests_mock.delete(
        client.glassflow_config.server_url + "/pipelines/test-pipeline-id",
        status_code=204,
        headers={"Content-Type": "application/json"},
    )
    pipeline = Pipeline(
        id="test-pipeline-id",
        personal_access_token="test-token",
    )
    pipeline.delete()


def test_delete_pipeline_fail_with_missing_pipeline_id(client):
    pipeline = Pipeline(
        personal_access_token="test-token",
    )
    with pytest.raises(ValueError):
        pipeline.delete()


def test_get_source_from_pipeline_ok(
    client,
    fetch_pipeline_response,
    get_pipeline_request_mock,
    get_access_token_request_mock,
    get_pipeline_function_source_request_mock,
    access_tokens_response,
):
    p = client.get_pipeline(fetch_pipeline_response.id)
    source = p.get_source()
    source2 = p.get_source(pipeline_access_token_name="token2")

    assert source.pipeline_id == p.id
    assert (
        source.pipeline_access_token
        == access_tokens_response["access_tokens"][0]["token"]
    )

    assert source2.pipeline_id == p.id
    assert (
        source2.pipeline_access_token
        == access_tokens_response["access_tokens"][1]["token"]
    )


def test_get_source_from_pipeline_fail_with_missing_id(client):
    pipeline = Pipeline(personal_access_token="test-token")
    with pytest.raises(ValueError) as e:
        pipeline.get_source()

    assert e.value.__str__() == "Pipeline id must be provided in the constructor"


def test_get_sink_from_pipeline_ok(
    client,
    fetch_pipeline_response,
    get_pipeline_request_mock,
    get_access_token_request_mock,
    get_pipeline_function_source_request_mock,
    access_tokens_response,
):
    p = client.get_pipeline(fetch_pipeline_response.id)
    sink = p.get_sink()
    sink2 = p.get_sink(pipeline_access_token_name="token2")

    assert sink.pipeline_id == p.id
    assert (
        sink.pipeline_access_token
        == access_tokens_response["access_tokens"][0]["token"]
    )

    assert sink2.pipeline_id == p.id
    assert (
        sink2.pipeline_access_token
        == access_tokens_response["access_tokens"][1]["token"]
    )


def test_get_logs_from_pipeline_ok(client, requests_mock, get_logs_response):
    pipeline_id = "test-pipeline-id"
    requests_mock.get(
        client.glassflow_config.server_url
        + f"/pipelines/{pipeline_id}/functions/main/logs",
        json=get_logs_response,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )
    pipeline = Pipeline(id=pipeline_id, personal_access_token="test-token")
    logs = pipeline.get_logs(page_size=50, severity_code=100)

    assert logs.next == get_logs_response["next"]
    for idx, log in enumerate(logs.logs):
        assert log.level == get_logs_response["logs"][idx]["level"]
        assert log.severity_code == get_logs_response["logs"][idx]["severity_code"]
        assert (
            log.payload.message == get_logs_response["logs"][idx]["payload"]["message"]
        )


def test_test_pipeline_ok(client, requests_mock, test_pipeline_response):
    pipeline_id = "test-pipeline-id"
    requests_mock.post(
        client.glassflow_config.server_url
        + f"/pipelines/{pipeline_id}/functions/main/test",
        json=test_pipeline_response,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )
    pipeline = Pipeline(id=pipeline_id, personal_access_token="test-token")
    response = pipeline.test(test_pipeline_response["payload"])

    assert (
        response.event_context.external_id
        == test_pipeline_response["event_context"]["external_id"]
    )
    assert response.status == test_pipeline_response["status"]
    assert response.response == test_pipeline_response["response"]
