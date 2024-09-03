import pytest
from glassflow.models.errors import ClientError


def test_pipeline_get_with_invalid_pipeline_credentials(client_with_token, invalid_pipeline_credentials):
    pipeline = client_with_token.pipeline_client(**invalid_pipeline_credentials)
    with pytest.raises(ClientError) as error:
        pipeline.get()
    assert error.value.status_code == 400


def test_pipeline_get_without_token(client_without_token, invalid_pipeline_credentials):
    pipeline = client_without_token.pipeline_client(**invalid_pipeline_credentials)
    with pytest.raises(ClientError) as error:
        pipeline.get()
    assert error.value.status_code == 401


def test_pipeline_get_success(client_with_token, pipeline_credentials):
    pipeline = client_with_token.pipeline_client(**pipeline_credentials)
    response = pipeline.get()
    assert response.status_code == 200


def test_pipeline_publish_and_consume(client_without_token, pipeline_credentials):
    pipeline = client_without_token.pipeline_client(**pipeline_credentials)
    publish_response = pipeline.publish({"test-key": "test-value"})
    assert publish_response.status_code == 200
    while True:
        consume_response = pipeline.consume()
        assert consume_response.status_code in (200, 204)
        if consume_response.status_code == 200:
            assert consume_response.json() == {"test-key": "test-value"}
            break
