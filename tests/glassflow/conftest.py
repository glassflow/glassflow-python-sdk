import pytest
import json
from glassflow.client import GlassFlowClient


@pytest.fixture
def glassflow_token():
    config = json.load(open("tests/glassflow/secrets/glassflow_token.json", "r"))
    return config.get("glassflow_token")


@pytest.fixture
def client_with_token(glassflow_token):
    return GlassFlowClient(glassflow_token)


@pytest.fixture
def client_without_token():
    return GlassFlowClient()


@pytest.fixture
def pipeline_credentials():
    return json.load(open("tests/glassflow/secrets/valid_pipeline_credentials.json", "r"))


@pytest.fixture
def invalid_pipeline_credentials():
    return json.load(open("tests/glassflow/sample_files/invalid_pipeline_credentials.json", "r"))
