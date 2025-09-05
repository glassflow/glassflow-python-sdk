from unittest.mock import patch

import pytest
from pydantic import ValidationError

from glassflow.etl import errors
from glassflow.etl.models import PipelineConfig
from glassflow.etl.pipeline import Pipeline


def test_create_pipeline_success(valid_pipeline_config, mock_success_response):
    """Test successful pipeline creation."""
    config = PipelineConfig(**valid_pipeline_config)
    pipeline = Pipeline(config=config)

    with patch("httpx.Client.post", return_value=mock_success_response) as mock_post:
        pipeline.create()
        mock_post.assert_called_once_with(
            pipeline.ENDPOINT,
            json=config.model_dump(mode="json", by_alias=True),
        )


def test_create_pipeline_already_active(valid_pipeline_config, mock_forbidden_response):
    """Test pipeline creation when a pipeline is already active."""
    config = PipelineConfig(**valid_pipeline_config)
    pipeline = Pipeline(config=config)

    with patch("httpx.Client.post", return_value=mock_forbidden_response):
        with pytest.raises(errors.PipelineAlreadyExistsError):
            pipeline.create()


def test_create_pipeline_invalid_config(invalid_pipeline_config):
    """Test pipeline creation with invalid configuration."""
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        Pipeline(config=invalid_pipeline_config)
    assert "pipeline_id cannot be empty" in str(exc_info.value)


def test_create_pipeline_bad_request(valid_pipeline_config, mock_bad_request_response):
    """Test pipeline creation with bad request."""
    config = PipelineConfig(**valid_pipeline_config)
    pipeline = Pipeline(config=config)

    with patch("httpx.Client.post", return_value=mock_bad_request_response):
        with pytest.raises(ValueError) as exc_info:
            pipeline.create()
        assert "Bad request" in str(exc_info.value)


def test_create_pipeline_connection_error(valid_pipeline_config, mock_connection_error):
    """Test pipeline creation with connection error."""
    config = PipelineConfig(**valid_pipeline_config)
    pipeline = Pipeline(config=config)

    with patch("httpx.Client.post", side_effect=mock_connection_error):
        with pytest.raises(errors.ConnectionError) as exc_info:
            pipeline.create()
        assert "Failed to connect to pipeline service" in str(exc_info.value)


def test_delete_pipeline_success(valid_pipeline_config, mock_success_response):
    """Test successful pipeline shutdown."""
    config = PipelineConfig(**valid_pipeline_config)
    pipeline = Pipeline(config=config)

    with patch(
        "httpx.Client.delete", return_value=mock_success_response
    ) as mock_delete:
        pipeline.delete()
        mock_delete.assert_called_once_with(f"{pipeline.ENDPOINT}/shutdown")


def test_delete_pipeline_not_found(valid_pipeline_config, mock_not_found_response):
    """Test pipeline shutdown when no pipeline is active."""
    config = PipelineConfig(**valid_pipeline_config)
    pipeline = Pipeline(config=config)

    with patch("httpx.Client.delete", return_value=mock_not_found_response):
        with pytest.raises(errors.PipelineNotFoundError):
            pipeline.delete()


def test_delete_pipeline_error(valid_pipeline_config, mock_server_error_response):
    """Test pipeline shutdown with error."""
    config = PipelineConfig(**valid_pipeline_config)
    pipeline = Pipeline(config=config)

    with patch("httpx.Client.delete", return_value=mock_server_error_response):
        with pytest.raises(errors.InternalServerError) as exc_info:
            pipeline.delete()
        assert "Failed to shutdown pipeline" in str(exc_info.value)


def test_delete_pipeline_connection_error(valid_pipeline_config, mock_connection_error):
    """Test pipeline shutdown with connection error."""
    config = PipelineConfig(**valid_pipeline_config)
    pipeline = Pipeline(config=config)

    with patch("httpx.Client.delete", side_effect=mock_connection_error):
        with pytest.raises(errors.ConnectionError) as exc_info:
            pipeline.delete()
        assert "Failed to connect to pipeline service" in str(exc_info.value)


def test_validate_config_valid(valid_pipeline_config):
    """Test validation of a valid pipeline configuration."""
    config = PipelineConfig(**valid_pipeline_config)
    Pipeline.validate_config(config)
    # No exception should be raised


def test_validate_config_invalid(invalid_pipeline_config):
    """Test validation of an invalid pipeline configuration."""
    with pytest.raises((ValueError, ValidationError)) as exc_info:
        Pipeline.validate_config(invalid_pipeline_config)
    assert "pipeline_id cannot be empty" in str(exc_info.value)


def test_get_running_pipeline_success(valid_pipeline_config, mock_success_response):
    """Test getting a running pipeline successfully."""
    pipeline = Pipeline(config=valid_pipeline_config)
    mock_success_response.json.return_value = {
        "id": valid_pipeline_config["pipeline_id"]
    }
    with patch("httpx.Client.get", return_value=mock_success_response) as mock_get:
        pipeline_id = pipeline.get_running_pipeline()
        mock_get.assert_called_once_with(pipeline.ENDPOINT)
        assert pipeline_id == valid_pipeline_config["pipeline_id"]


def test_get_running_pipeline_not_found(mock_not_found_response):
    """Test getting a running pipeline when none exists."""
    pipeline = Pipeline()
    with patch("httpx.Client.get", return_value=mock_not_found_response) as mock_get:
        with pytest.raises(errors.PipelineNotFoundError):
            pipeline.get_running_pipeline()
            mock_get.assert_called_once_with(pipeline.ENDPOINT)


def test_get_running_pipeline_server_error(mock_server_error_response):
    """Test getting a running pipeline when server error occurs."""
    pipeline = Pipeline()
    with patch("httpx.Client.get", return_value=mock_server_error_response) as mock_get:
        with pytest.raises(errors.InternalServerError):
            pipeline.get_running_pipeline()
            mock_get.assert_called_once_with(pipeline.ENDPOINT)


def test_get_running_pipeline_connection_error(mock_connection_error):
    """Test getting a running pipeline when connection error occurs."""
    pipeline = Pipeline()
    with patch("httpx.Client.get", side_effect=mock_connection_error) as mock_get:
        with pytest.raises(errors.ConnectionError):
            pipeline.get_running_pipeline()
            mock_get.assert_called_once_with(pipeline.ENDPOINT)


def test_tracking_info(
    valid_pipeline_config,
    valid_pipeline_config_with_dedup_disabled,
    valid_pipeline_config_without_joins,
    valid_pipeline_config_without_joins_and_dedup_disabled,
):
    """Test tracking info."""
    pipeline = Pipeline(config=valid_pipeline_config)
    assert pipeline._tracking_info() == {
        "pipeline_id": valid_pipeline_config["pipeline_id"],
        "join_enabled": True,
        "deduplication_enabled": True,
        "source_auth_method": "SCRAM-SHA-256",
        "source_security_protocol": "SASL_SSL",
        "source_root_ca_provided": True,
        "source_skip_auth": False,
    }

    pipeline = Pipeline(config=valid_pipeline_config_with_dedup_disabled)
    assert pipeline._tracking_info() == {
        "pipeline_id": valid_pipeline_config_with_dedup_disabled["pipeline_id"],
        "join_enabled": True,
        "deduplication_enabled": False,
        "source_auth_method": "SCRAM-SHA-256",
        "source_security_protocol": "SASL_SSL",
        "source_root_ca_provided": True,
        "source_skip_auth": False,
    }

    pipeline = Pipeline(config=valid_pipeline_config_without_joins)
    assert pipeline._tracking_info() == {
        "pipeline_id": valid_pipeline_config_without_joins["pipeline_id"],
        "join_enabled": False,
        "deduplication_enabled": True,
        "source_auth_method": "SCRAM-SHA-256",
        "source_security_protocol": "SASL_SSL",
        "source_root_ca_provided": True,
        "source_skip_auth": False,
    }

    pipeline = Pipeline(config=valid_pipeline_config_without_joins_and_dedup_disabled)
    pipeline_id = valid_pipeline_config_without_joins_and_dedup_disabled["pipeline_id"]
    assert pipeline._tracking_info() == {
        "pipeline_id": pipeline_id,
        "join_enabled": False,
        "deduplication_enabled": False,
        "source_auth_method": "SCRAM-SHA-256",
        "source_security_protocol": "SASL_SSL",
        "source_root_ca_provided": True,
        "source_skip_auth": False,
    }
