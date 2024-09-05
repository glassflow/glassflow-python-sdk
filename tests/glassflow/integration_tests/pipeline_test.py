import pytest

from glassflow.models.errors import ClientError


def test_pipeline_is_valid_access_token_ok(client, pipeline_credentials):
    pipeline = client.pipeline_client(**pipeline_credentials)

    is_valid = pipeline.is_valid_access_token()
    assert is_valid


def test_pipeline_is_valid_access_token_not_ok(
    client, pipeline_credentials_invalid_token
):
    pipeline = client.pipeline_client(**pipeline_credentials_invalid_token)

    is_valid = pipeline.is_valid_access_token()
    assert not is_valid


def test_pipeline_is_valid_access_token_with_invalid_credentials(
    client, pipeline_credentials_invalid_id
):
    pipeline = client.pipeline_client(**pipeline_credentials_invalid_id)

    with pytest.raises(ClientError) as exc_info:
        pipeline.is_valid_access_token()
    exc = exc_info.value
    assert exc.status_code == 404


def test_pipeline_publish_and_consume(client, pipeline_credentials):
    pipeline = client.pipeline_client(**pipeline_credentials)
    publish_response = pipeline.publish({"test-key": "test-value"})
    assert publish_response.status_code == 200
    while True:
        consume_response = pipeline.consume()
        assert consume_response.status_code in (200, 204)
        if consume_response.status_code == 200:
            assert consume_response.json() == {"test-key": "test-value"}
            break
