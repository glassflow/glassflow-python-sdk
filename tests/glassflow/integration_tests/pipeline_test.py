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


def test_fetch_pipeline_fail_404(pipeline_with_id):
    with pytest.raises(errors.PipelineNotFoundError):
        pipeline_with_id.fetch()


def test_fetch_pipeline_fail_401(pipeline_with_id_and_invalid_token):
    with pytest.raises(errors.UnauthorizedError):
        pipeline_with_id_and_invalid_token.fetch()


def test_delete_pipeline_fail_404(pipeline_with_id):
    with pytest.raises(errors.PipelineNotFoundError):
        pipeline_with_id.delete()


def test_delete_pipeline_fail_401(pipeline_with_id_and_invalid_token):
    with pytest.raises(errors.UnauthorizedError):
        pipeline_with_id_and_invalid_token.delete()
