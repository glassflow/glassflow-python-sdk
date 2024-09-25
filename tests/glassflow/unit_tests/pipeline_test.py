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
