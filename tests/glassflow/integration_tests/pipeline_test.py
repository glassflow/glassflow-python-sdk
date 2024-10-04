import pytest

from glassflow.models import api, errors


def test_create_pipeline_ok(creating_pipeline):
    assert creating_pipeline.name == "test_pipeline"
    assert creating_pipeline.id is not None


def test_fetch_pipeline_ok(creating_pipeline):
    creating_pipeline.fetch()
    assert creating_pipeline.name == "test_pipeline"
    assert creating_pipeline.id is not None
    assert creating_pipeline.created_at is not None


def test_fetch_pipeline_fail_with_404(pipeline_with_random_id):
    with pytest.raises(errors.PipelineNotFoundError):
        pipeline_with_random_id.fetch()


def test_fetch_pipeline_fail_with_401(pipeline_with_random_id_and_invalid_token):
    with pytest.raises(errors.UnauthorizedError):
        pipeline_with_random_id_and_invalid_token.fetch()


def test_update_pipeline_ok(creating_pipeline):
    updated_pipeline = creating_pipeline.update(
        name="new_name",
        sink_kind="webhook",
        state="paused",
        sink_config={
            "url": "www.test-url.com",
            "method": "GET",
            "headers": [{"name": "header1", "value": "header1"}],
        },
        transformation_file="tests/data/transformation_2.py",
        requirements="requests,pandas",
        env_vars=[
            {"name": "env1", "value": "env1"},
            {"name": "env2", "value": "env2"},
        ],
    )
    assert updated_pipeline.name == "new_name"
    assert updated_pipeline.sink_kind == "webhook"
    assert updated_pipeline.sink_config == {
        "url": "www.test-url.com",
        "method": "GET",
        "headers": [{"name": "header1", "value": "header1"}],
    }
    assert updated_pipeline.env_vars == [
        {"name": "env1", "value": "env1"},
        {"name": "env2", "value": "env2"},
    ]
    with open("tests/data/transformation_2.py") as f:
        assert updated_pipeline.transformation_code == f.read()

    assert updated_pipeline.source_kind == creating_pipeline.source_kind
    assert updated_pipeline.source_config == creating_pipeline.source_config
    assert updated_pipeline.state == api.PipelineState.paused


def test_delete_pipeline_fail_with_404(pipeline_with_random_id):
    with pytest.raises(errors.PipelineNotFoundError):
        pipeline_with_random_id.delete()


def test_delete_pipeline_fail_with_401(pipeline_with_random_id_and_invalid_token):
    with pytest.raises(errors.UnauthorizedError):
        pipeline_with_random_id_and_invalid_token.delete()


def test_get_logs_from_pipeline_ok(creating_pipeline):
    import time

    n_tries = 0
    max_tries = 10
    while True:
        if n_tries == max_tries:
            pytest.fail("Max tries reached")

        logs = creating_pipeline.get_logs()
        if len(logs.logs) >= 2:
            break
        else:
            n_tries += 1
            time.sleep(1)

    assert logs.status_code == 200
    assert logs.content_type == "application/json"
    assert logs.logs[0].payload.message == "Function is uploaded"
    assert logs.logs[0].level == "INFO"
    assert logs.logs[1].payload.message == "Pipeline is created"
    assert logs.logs[1].level == "INFO"
