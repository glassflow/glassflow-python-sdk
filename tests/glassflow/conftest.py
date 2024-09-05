import os
import uuid

import pytest

from glassflow.client import GlassFlowClient


@pytest.fixture
def client():
    return GlassFlowClient()


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
