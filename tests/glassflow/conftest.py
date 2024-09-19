import os
import uuid

import pytest

from glassflow import PipelineDataSink, PipelineDataSource
from glassflow.client import APIClient

# Use staging api server
APIClient.glassflow_config.server_url = "https://staging.api.glassflow.dev/v1"


@pytest.fixture
def source(pipeline_credentials):
    return PipelineDataSource(**pipeline_credentials)


@pytest.fixture
def source_with_invalid_access_token(pipeline_credentials_invalid_token):
    return PipelineDataSource(**pipeline_credentials_invalid_token)


@pytest.fixture
def source_with_non_existing_id(pipeline_credentials_invalid_id):
    return PipelineDataSource(**pipeline_credentials_invalid_id)


@pytest.fixture
def sink(pipeline_credentials):
    return PipelineDataSink(**pipeline_credentials)


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
