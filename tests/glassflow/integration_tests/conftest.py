import os
import uuid

import pytest

from glassflow import GlassFlowClient, PipelineDataSink, PipelineDataSource
from glassflow.pipeline import Pipeline


@pytest.fixture
def client():
    return GlassFlowClient(os.getenv("PERSONAL_ACCESS_TOKEN"))


@pytest.fixture
def pipeline(client):
    return Pipeline(
        name="test_pipeline",
        space_id=os.getenv("SPACE_ID"),
        transformation_file="tests/data/transformation.py",
        personal_access_token=client.personal_access_token,
    )


@pytest.fixture
def pipeline_with_id(client):
    return Pipeline(
        id=str(uuid.uuid4()),
        personal_access_token=client.personal_access_token,
    )


@pytest.fixture
def pipeline_with_id_and_invalid_token():
    return Pipeline(
        id=str(uuid.uuid4()),
        personal_access_token="invalid-token",
    )


@pytest.fixture
def creating_pipeline(pipeline):
    pipeline.create()
    yield pipeline
    pipeline.delete()


@pytest.fixture
def source(creating_pipeline):
    return PipelineDataSource(
        pipeline_id=creating_pipeline.id,
        pipeline_access_token=creating_pipeline.access_tokens[0]["token"],
    )


@pytest.fixture
def source_with_invalid_access_token(creating_pipeline):
    return PipelineDataSource(
        pipeline_id=creating_pipeline.id, pipeline_access_token="invalid-access-token"
    )


@pytest.fixture
def source_with_non_existing_id(creating_pipeline):
    return PipelineDataSource(
        pipeline_id=str(uuid.uuid4()),
        pipeline_access_token=creating_pipeline.access_tokens[0]["token"],
    )


@pytest.fixture
def source_with_published_events(source):
    source.publish({"test_field": "test_value"})
    yield source


@pytest.fixture
def sink(source_with_published_events):
    return PipelineDataSink(
        pipeline_id=source_with_published_events.pipeline_id,
        pipeline_access_token=source_with_published_events.pipeline_access_token,
    )