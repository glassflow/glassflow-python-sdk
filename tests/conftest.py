from unittest.mock import patch

import pytest

from glassflow.etl.dlq import DLQ
from glassflow.etl.models import PipelineConfig
from glassflow.etl.pipeline import Pipeline
from tests.data import mock_responses, pipeline_configs


@pytest.fixture
def mock_track(autouse=True):
    """Mock the Mixpanel track method."""
    with patch("glassflow.etl.tracking.mixpanel.Mixpanel.track") as mock:
        yield mock


@pytest.fixture
def valid_config() -> dict:
    """Fixture for a valid pipeline configuration."""
    return pipeline_configs.get_valid_pipeline_config()


@pytest.fixture
def valid_config_without_joins() -> dict:
    """Fixture for a valid pipeline configuration without joins."""
    return pipeline_configs.get_valid_config_without_joins()


@pytest.fixture
def valid_config_with_dedup_disabled() -> dict:
    """Fixture for a valid pipeline configuration with deduplication disabled."""
    return pipeline_configs.get_valid_config_with_dedup_disabled()


@pytest.fixture
def valid_config_without_joins_and_dedup_disabled() -> dict:
    """Fixture for a valid pipeline configuration without joins and deduplication."""
    return pipeline_configs.get_valid_config_without_joins_and_dedup_disabled()


@pytest.fixture
def invalid_config() -> dict:
    """Fixture for an invalid pipeline configuration."""
    return pipeline_configs.get_invalid_config()


@pytest.fixture
def mock_success_response():
    """Fixture for a successful HTTP response."""
    return mock_responses.create_mock_response_factory()(
        status_code=200,
        json_data={"message": "Success"},
    )


@pytest.fixture
def mock_not_found_response():
    """Fixture for a 404 Not Found HTTP response."""
    return mock_responses.create_mock_response_factory()(
        status_code=404,
        json_data={"message": "Not Found"},
    )


@pytest.fixture
def mock_forbidden_response():
    """Fixture for a 403 Forbidden HTTP response."""
    return mock_responses.create_mock_response_factory()(
        status_code=403,
        json_data={"message": "Forbidden"},
    )


@pytest.fixture
def mock_bad_request_response():
    """Fixture for a 400 Bad Request HTTP response."""
    return mock_responses.create_mock_response_factory()(
        status_code=400,
        json_data={"message": "Bad Request"},
    )


@pytest.fixture
def mock_connection_error():
    """Fixture for a connection error."""
    return mock_responses.create_mock_connection_error()


@pytest.fixture
def mock_success_get_pipeline(valid_config):
    """Fixture for a successful GET pipeline response."""
    return mock_responses.create_mock_response_factory()(
        status_code=200,
        json_data=valid_config,
    )


@pytest.fixture
def pipeline_from_id(mock_success_get_pipeline):
    """Fixture for a successful GET request."""
    with patch("httpx.Client.request", return_value=mock_success_get_pipeline):
        return Pipeline(pipeline_id="test-pipeline-id").get()


@pytest.fixture
def dlq():
    """Fixture for a DLQ instance."""
    return DLQ(host="http://localhost:8080", pipeline_id="test-pipeline")


@pytest.fixture
def pipeline(valid_config):
    """Base pipeline fixture with valid config."""
    config = PipelineConfig(**valid_config)
    return Pipeline(host="http://localhost:8080", config=config)
