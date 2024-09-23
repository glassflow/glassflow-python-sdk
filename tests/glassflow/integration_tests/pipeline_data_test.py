import pytest

from glassflow import errors


def test_using_staging_server(source, sink):
    assert source.glassflow_config.server_url == "https://staging.api.glassflow.dev/v1"
    assert sink.glassflow_config.server_url == "https://staging.api.glassflow.dev/v1"


def test_pipeline_data_source_validate_credentials_ok(source):
    try:
        source.validate_credentials()
    except Exception as e:
        pytest.fail(e)


def test_pipeline_data_source_validate_credentials_invalid_access_token(
    source_with_invalid_access_token,
):
    with pytest.raises(errors.PipelineAccessTokenInvalidError):
        source_with_invalid_access_token.validate_credentials()


def test_pipeline_data_source_validate_credentials_id_not_found(
    source_with_non_existing_id,
):
    with pytest.raises(errors.PipelineNotFoundError):
        source_with_non_existing_id.validate_credentials()


def test_pipeline_publish_and_consume(source, sink):
    publish_response = source.publish({"test-key": "test-value"})
    assert publish_response.status_code == 200
    while True:
        consume_response = sink.consume()
        assert consume_response.status_code in (200, 204)
        if consume_response.status_code == 200:
            assert consume_response.json() == {"test-key": "test-value"}
            break
