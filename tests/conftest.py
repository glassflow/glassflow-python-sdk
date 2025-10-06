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
def get_pipeline_response(valid_config) -> dict:
    """Fixture for a valid pipeline configuration with status."""
    config = valid_config
    config["status"] = "Running"
    return config


@pytest.fixture
def get_health_payload():
    """Factory to create a health endpoint payload for a pipeline id."""

    def factory(
        pipeline_id: str,
        name: str = "Test Pipeline",
        status: str = "Running",
    ) -> dict:
        return {
            "pipeline_id": pipeline_id,
            "pipeline_name": name,
            "overall_status": status,
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T00:00:00Z",
        }

    return factory


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
def mock_success():
    """Factory-context fixture that patches httpx and returns 200 with JSON.

    - Accepts either a single dict payload or a list of dict payloads via the
      optional argument to the returned context manager. If a list is provided,
      they are returned sequentially from response.json() to simulate multiple
      HTTP calls within the same test flow.
    - If no payload is provided, it defaults to {"message": "Success"}.

    Usage:
        with mock_success(payload_or_list) as mock_request:
            # invoke code under test
            assert mock_request.call_args_list == [...]
    """
    from contextlib import contextmanager

    @contextmanager
    def factory(json_payloads=None):
        if json_payloads is None:
            json_payloads = [{"message": "Success"}]
        payload_list = (
            list(json_payloads) if isinstance(json_payloads, list) else [json_payloads]
        )
        response = mock_responses.create_mock_response_factory()(
            status_code=200,
            json_data=payload_list[0] if payload_list else {},
        )
        with patch("httpx.Client.request", return_value=response) as mock:
            if payload_list:
                response.json.side_effect = payload_list
            yield mock

    return factory


@pytest.fixture
def pipeline_from_id(mock_success, get_pipeline_response, get_health_payload):
    """Fixture for a successful GET request."""
    with mock_success([get_pipeline_response, get_health_payload("test-pipeline-id")]):
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
