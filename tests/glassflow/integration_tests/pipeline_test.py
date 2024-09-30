import pytest

from glassflow.models import errors


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
        sink_config={
            "url": "www.test-url.com",
            "method": "GET",
            "headers": [{"name": "header1", "value": "header1"}],
        },
    )
    assert updated_pipeline.name == "new_name"
    assert updated_pipeline.sink_kind == "webhook"
    assert updated_pipeline.sink_config == {
        "url": "www.test-url.com",
        "method": "GET",
        "headers": [{"name": "header1", "value": "header1"}],
    }


def test_delete_pipeline_fail_with_404(pipeline_with_random_id):
    with pytest.raises(errors.PipelineNotFoundError):
        pipeline_with_random_id.delete()


def test_delete_pipeline_fail_with_401(pipeline_with_random_id_and_invalid_token):
    with pytest.raises(errors.UnauthorizedError):
        pipeline_with_random_id_and_invalid_token.delete()
