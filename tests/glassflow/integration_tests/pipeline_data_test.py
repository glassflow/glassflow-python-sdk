import pytest

from glassflow import errors


def test_using_staging_server(source, sink):
    assert source.glassflow_config.server_url == "https://staging.api.glassflow.dev/v1"
    assert sink.glassflow_config.server_url == "https://staging.api.glassflow.dev/v1"


def test_validate_credentials_from_pipeline_data_source_ok(source):
    try:
        source.validate_credentials()
    except Exception as e:
        pytest.fail(e)


def test_validate_credentials_from_pipeline_data_source_fail_with_invalid_access_token(
    source_with_invalid_access_token,
):
    with pytest.raises(errors.PipelineAccessTokenInvalidError):
        source_with_invalid_access_token.validate_credentials()


def test_validate_credentials_from_pipeline_data_source_fail_with_id_not_found(
    source_with_non_existing_id,
):
    with pytest.raises(errors.PipelineNotFoundError):
        source_with_non_existing_id.validate_credentials()


def test_publish_to_pipeline_data_source_ok(source):
    publish_response = source.publish({"test_field": "test_value"})
    assert publish_response.status_code == 200


def test_consume_from_pipeline_data_sink_ok(sink):
    while True:
        consume_response = sink.consume()
        assert consume_response.status_code in (200, 204)
        if consume_response.status_code == 200:
            assert consume_response.json() == {
                "test_field": "test_value",
                "new_field": "new_value",
            }
            break
