import pytest

from glassflow.models import errors
from glassflow.pipeline import Pipeline


def test_pipeline_with_transformation_file():
    try:
        p = Pipeline(
            transformation_file="tests/data/transformation.py",
            personal_access_token="test-token",
        )
        assert p.transformation_code is not None
    except Exception as e:
        pytest.fail(e)


def test_pipeline_fail_with_file_not_found():
    with pytest.raises(FileNotFoundError):
        Pipeline(transformation_file="fake_file.py", personal_access_token="test-token")


def test_pipeline_fail_with_missing_sink_data():
    with pytest.raises(ValueError) as e:
        Pipeline(
            transformation_file="tests/data/transformation.py",
            personal_access_token="test-token",
            sink_kind="google_pubsub",
        )
    assert str(e.value) == "Both sink_kind and sink_config must be provided"


def test_pipeline_fail_with_missing_source_data():
    with pytest.raises(ValueError) as e:
        Pipeline(
            transformation_file="tests/data/transformation.py",
            personal_access_token="test-token",
            source_kind="google_pubsub",
        )
    assert str(e.value) == "Both source_kind and source_config must be provided"


def test_fetch_pipeline_ok(requests_mock, pipeline_dict, access_tokens, client):
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

    pipeline = Pipeline(
        id=pipeline_dict["id"],
        personal_access_token="test-token",
    ).fetch()

    assert pipeline.name == pipeline_dict["name"]


def test_fetch_pipeline_fail_with_404(requests_mock, pipeline_dict, client):
    requests_mock.get(
        client.glassflow_config.server_url + "/pipelines/test-id",
        json=pipeline_dict,
        status_code=404,
        headers={"Content-Type": "application/json"},
    )

    with pytest.raises(errors.PipelineNotFoundError):
        Pipeline(
            id=pipeline_dict["id"],
            personal_access_token="test-token",
        ).fetch()


def test_fetch_pipeline_fail_with_401(requests_mock, pipeline_dict, client):
    requests_mock.get(
        client.glassflow_config.server_url + "/pipelines/test-id",
        json=pipeline_dict,
        status_code=401,
        headers={"Content-Type": "application/json"},
    )

    with pytest.raises(errors.UnauthorizedError):
        Pipeline(
            id=pipeline_dict["id"],
            personal_access_token="test-token",
        ).fetch()


def test_create_pipeline_ok(
    requests_mock, pipeline_dict, create_pipeline_response, client
):
    requests_mock.post(
        client.glassflow_config.server_url + "/pipelines",
        json=create_pipeline_response,
        status_code=200,
        headers={"Content-Type": "application/json"},
    )
    pipeline = Pipeline(
        name=pipeline_dict["name"],
        space_id=create_pipeline_response["space_id"],
        transformation_code="transformation code...",
        personal_access_token="test-token",
    ).create()

    assert pipeline.id == "test-id"
    assert pipeline.name == "test-name"


def test_create_pipeline_fail_with_missing_name(client):
    with pytest.raises(ValueError) as e:
        Pipeline(
            space_id="test-space-id",
            transformation_code="transformation code...",
            personal_access_token="test-token",
        ).create()

    assert e.value.__str__() == (
        "Name must be provided in order to " "create the pipeline"
    )


def test_create_pipeline_fail_with_missing_space_id(client):
    with pytest.raises(ValueError) as e:
        Pipeline(
            name="test-name",
            transformation_code="transformation code...",
            personal_access_token="test-token",
        ).create()

    assert e.value.__str__() == (
        "Space_id must be provided in order to " "create the pipeline"
    )


def test_create_pipeline_fail_with_missing_transformation(client):
    with pytest.raises(ValueError) as e:
        Pipeline(
            name="test-name",
            space_id="test-space-id",
            personal_access_token="test-token",
        ).create()

    assert e.value.__str__() == (
        "Either transformation_code or " "transformation_file must be provided"
    )


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
    client, pipeline_dict, requests_mock, access_tokens
):
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


def test_get_source_from_pipeline_fail_with_missing_id(client):
    pipeline = Pipeline(personal_access_token="test-token")
    with pytest.raises(ValueError) as e:
        pipeline.get_source()

    assert e.value.__str__() == "Pipeline id must be provided in the constructor"


def test_get_sink_from_pipeline_ok(client, pipeline_dict, requests_mock, access_tokens):
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
