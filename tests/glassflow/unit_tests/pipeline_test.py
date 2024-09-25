import pytest

from glassflow.pipeline import Pipeline


def test_pipeline_transformation_file():
    try:
        p = Pipeline(
            transformation_file="tests/data/transformation.py",
            personal_access_token="test-token"
        )
        assert p.transformation_code is not None
    except Exception as e:
        pytest.fail(e)


def test_pipeline_transformation_file_not_found():
    with pytest.raises(FileNotFoundError):
        Pipeline(transformation_file="fake_file.py", personal_access_token="test-token")


def test_pipeline_delete_ok(requests_mock, client):
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


def test_pipeline_delete_missing_pipeline_id(client):
    pipeline = Pipeline(
        personal_access_token="test-token",
    )
    with pytest.raises(ValueError):
        pipeline.delete()


def test_pipeline_get_source_ok(client, pipeline_dict, requests_mock, access_tokens):
    requests_mock.get(
        client.glassflow_config.server_url + "/pipelines/test-id",
        json=pipeline_dict,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )
    requests_mock.get(
        client.glassflow_config.server_url + "/pipelines/test-id/access_tokens",
        json=access_tokens,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )
    p = client.get_pipeline("test-id")
    source = p.get_source()
    source2 = p.get_source(pipeline_access_token_name="token2")

    assert source.pipeline_id == p.id
    assert source.pipeline_access_token == access_tokens["access_tokens"][0]["token"]

    assert source2.pipeline_id == p.id
    assert source2.pipeline_access_token == access_tokens["access_tokens"][1]["token"]


def test_pipeline_get_sink_ok(client, pipeline_dict, requests_mock, access_tokens):
    requests_mock.get(
        client.glassflow_config.server_url + "/pipelines/test-id",
        json=pipeline_dict,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )
    requests_mock.get(
        client.glassflow_config.server_url + "/pipelines/test-id/access_tokens",
        json=access_tokens,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )
    p = client.get_pipeline("test-id")
    sink = p.get_sink()
    sink2 = p.get_sink(pipeline_access_token_name="token2")

    assert sink.pipeline_id == p.id
    assert sink.pipeline_access_token == access_tokens["access_tokens"][0]["token"]

    assert sink2.pipeline_id == p.id
    assert sink2.pipeline_access_token == access_tokens["access_tokens"][1]["token"]
