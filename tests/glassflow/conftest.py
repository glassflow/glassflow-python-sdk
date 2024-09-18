import os
import uuid

import pytest

from glassflow import GlassFlowClient, PipelineDataSink, PipelineDataSource
from glassflow.client import GlassFlowConfig


@pytest.fixture
def staging_config():
    config = GlassFlowConfig()
    config.server_url = "https://staging.api.glassflow.dev/v1"
    return config


@pytest.fixture
def client(staging_config):
    c = GlassFlowClient()
    c.glassflow_config = staging_config
    return c


@pytest.fixture
def source(pipeline_credentials, staging_config):
    source = PipelineDataSource(**pipeline_credentials)
    source.glassflow_config = staging_config
    return source


@pytest.fixture
def source_with_invalid_access_token(pipeline_credentials_invalid_token, staging_config):
    source = PipelineDataSource(**pipeline_credentials_invalid_token)
    source.glassflow_config = staging_config
    return source


@pytest.fixture
def source_with_non_existing_id(pipeline_credentials_invalid_id, staging_config):
    source = PipelineDataSource(**pipeline_credentials_invalid_id)
    source.glassflow_config = staging_config
    return source


@pytest.fixture
def sink(pipeline_credentials, staging_config):
    sink = PipelineDataSink(**pipeline_credentials)
    sink.glassflow_config = staging_config
    return sink


@pytest.fixture
def pipeline_credentials():
    return {
        "pipeline_id": os.getenv("PIPELINE_ID"),
        "pipeline_access_token": os.getenv("PIPELINE_ACCESS_TOKEN"),
    }


@pytest.fixture
def pipeline_credentials_invalid_token():
    return {
        "pipeline_id": os.getenv("PIPELINE_ID"),
        "pipeline_access_token": "invalid-pipeline-access-token",
    }


@pytest.fixture
def pipeline_credentials_invalid_id():
    return {
        "pipeline_id": str(uuid.uuid4()),
        "pipeline_access_token": os.getenv("PIPELINE_ACCESS_TOKEN"),
    }
