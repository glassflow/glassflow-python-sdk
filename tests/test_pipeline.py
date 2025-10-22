import os
import tempfile
from unittest.mock import call, patch

import pytest
from pydantic import ValidationError

from glassflow.etl import errors, models
from glassflow.etl.models import PipelineConfig
from glassflow.etl.pipeline import Pipeline
from tests.data import error_scenarios, mock_responses


class TestPipelineCreation:
    """Tests for pipeline creation operations."""

    def test_create_success(self, pipeline, mock_success):
        """Test successful pipeline creation."""
        with mock_success() as mock_request:
            result = pipeline.create()
            mock_request.assert_called_once_with(
                "POST",
                pipeline.ENDPOINT,
                json=pipeline.config.model_dump(mode="json", by_alias=True),
            )
            assert result == pipeline
            assert pipeline.status == models.PipelineStatus.CREATED

    def test_create_invalid_config(self, invalid_config):
        """Test pipeline creation with invalid configuration."""
        with pytest.raises((ValueError, ValidationError)) as exc_info:
            Pipeline(host="http://localhost:8080", config=invalid_config)
        assert "pipeline_id cannot be empty" in str(exc_info.value)

    def test_create_value_error(self, valid_config):
        """Test pipeline creation with value error."""
        with pytest.raises(ValueError):
            Pipeline(host="http://localhost:8080")

        with pytest.raises(ValueError):
            Pipeline(config=valid_config, pipeline_id="test-pipeline")

        with pytest.raises(ValueError):
            Pipeline(host="http://localhost:8080", pipeline_id="test-pipeline").create()

    def test_create_connection_error(self, pipeline, mock_connection_error):
        """Test pipeline creation with connection error."""
        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline.create()
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)

    @pytest.mark.parametrize(
        "scenario",
        error_scenarios.get_http_error_scenarios(),
        ids=lambda s: s["name"],
    )
    def test_create_http_error_scenarios(self, pipeline, scenario):
        """Test pipeline creation with various HTTP error scenarios."""
        mock_response = mock_responses.create_mock_response_factory()(
            status_code=scenario["status_code"],
            json_data=scenario["json_data"],
            text=scenario["json_data"]["message"],
        )

        with patch(
            "httpx.Client.request",
            side_effect=mock_response.raise_for_status.side_effect,
        ):
            with pytest.raises(scenario["expected_error"]) as exc_info:
                pipeline.create()
            assert scenario["error_message"] in str(exc_info.value)


class TestPipelineLifecycle:
    """Tests for resume, stop, terminate, delete operations."""

    @pytest.mark.parametrize(
        "operation,method,endpoint,params,status",
        [
            ("get", "GET", "", {}, models.PipelineStatus.RUNNING),
            ("resume", "POST", "/resume", {}, models.PipelineStatus.RESUMING),
            ("delete", "DELETE", "", {}, models.PipelineStatus.DELETED),
            (
                "stop",
                "POST",
                "/stop",
                {"terminate": False},
                models.PipelineStatus.STOPPING,
            ),
            (
                "stop",
                "POST",
                "/terminate",
                {"terminate": True},
                models.PipelineStatus.TERMINATING,
            ),
        ],
    )
    def test_lifecycle_operations(
        self,
        pipeline,
        mock_success,
        get_pipeline_response,
        get_health_payload,
        operation,
        method,
        endpoint,
        params,
        status,
    ):
        """Test common pipeline lifecycle operations."""
        if operation == "get":
            mocked = mock_success(
                [get_pipeline_response, get_health_payload(pipeline.pipeline_id)]
            )
        else:
            mocked = mock_success()
        with mocked as mock_request:
            result = getattr(pipeline, operation)(**params)
            expected_endpoint = f"{pipeline.ENDPOINT}/{pipeline.pipeline_id}{endpoint}"
            if operation == "get":
                assert mock_request.call_args_list == [
                    call("GET", expected_endpoint),
                    call(
                        "GET",
                        f"{pipeline.ENDPOINT}/{pipeline.pipeline_id}/health",
                    ),
                ]
            else:
                mock_request.assert_called_once_with(method, expected_endpoint)

            if operation == "delete":
                assert result is None
            else:
                assert result == pipeline
            assert pipeline.status == status

    @pytest.mark.parametrize("operation", ["get", "delete", "resume", "stop"])
    def test_lifecycle_not_found(self, pipeline, mock_not_found_response, operation):
        """Test lifecycle operations when pipeline is not found."""
        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError):
                getattr(pipeline, operation)()

    @pytest.mark.parametrize("operation", ["get", "delete", "resume", "stop"])
    def test_lifecycle_connection_error(
        self, pipeline, mock_connection_error, operation
    ):
        """Test lifecycle operations with connection error."""
        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                getattr(pipeline, operation)()
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)


class TestPipelineModification:
    """Tests for update, rename operations."""

    def test_rename_success(self, pipeline, mock_success):
        """Test successful pipeline rename."""
        new_name = "renamed-pipeline"
        with mock_success() as mock_request:
            result = pipeline.rename(new_name)
            mock_request.assert_called_once_with(
                "PATCH",
                f"{pipeline.ENDPOINT}/{pipeline.pipeline_id}",
                json={"name": new_name},
            )
            assert result == pipeline
            # After rename, the config should be loaded and name should be updated

    def test_rename_not_found(self, pipeline, mock_not_found_response):
        """Test pipeline rename when pipeline is not found."""
        new_name = "renamed-pipeline"
        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError):
                pipeline.rename(new_name)

    def test_rename_connection_error(self, pipeline, mock_connection_error):
        """Test pipeline rename with connection error."""
        new_name = "renamed-pipeline"
        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline.rename(new_name)
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)


class TestPipelineValidation:
    """Tests for config validation."""

    def test_validate_config_valid(self, valid_config):
        """Test validation of a valid pipeline configuration."""
        config = PipelineConfig(**valid_config)
        Pipeline.validate_config(config)
        # No exception should be raised

    @pytest.mark.parametrize(
        "scenario",
        error_scenarios.get_validation_error_scenarios(),
        ids=lambda s: s["name"],
    )
    def test_pipeline_id_validation_scenarios(self, scenario):
        """Test pipeline ID validation with various error scenarios."""
        # Create a minimal valid config and override pipeline_id
        from tests.data.pipeline_configs import get_valid_pipeline_config

        config_data = get_valid_pipeline_config()
        config_data.update(scenario["config"])

        with pytest.raises(scenario["expected_error"]) as exc_info:
            PipelineConfig(**config_data)

        assert scenario["error_message"] in str(exc_info.value)


class TestPipelineTracking:
    """Tests for tracking functionality."""

    def test_tracking_info(
        self,
        valid_config,
        valid_config_with_dedup_disabled,
        valid_config_without_joins,
        valid_config_without_joins_and_dedup_disabled,
    ):
        """Test tracking info."""
        pipeline = Pipeline(host="http://localhost:8080", config=valid_config)
        assert pipeline._tracking_info() == {
            "pipeline_id": valid_config["pipeline_id"],
            "join_enabled": True,
            "deduplication_enabled": True,
            "source_auth_method": "SCRAM-SHA-256",
            "source_security_protocol": "SASL_SSL",
            "source_root_ca_provided": True,
            "source_skip_auth": False,
        }

        pipeline = Pipeline(
            host="http://localhost:8080",
            config=valid_config_with_dedup_disabled,
        )
        assert pipeline._tracking_info() == {
            "pipeline_id": valid_config_with_dedup_disabled["pipeline_id"],
            "join_enabled": True,
            "deduplication_enabled": False,
            "source_auth_method": "SCRAM-SHA-256",
            "source_security_protocol": "SASL_SSL",
            "source_root_ca_provided": True,
            "source_skip_auth": False,
        }

        pipeline = Pipeline(
            host="http://localhost:8080", config=valid_config_without_joins
        )
        assert pipeline._tracking_info() == {
            "pipeline_id": valid_config_without_joins["pipeline_id"],
            "join_enabled": False,
            "deduplication_enabled": True,
            "source_auth_method": "SCRAM-SHA-256",
            "source_security_protocol": "SASL_SSL",
            "source_root_ca_provided": True,
            "source_skip_auth": False,
        }

        pipeline = Pipeline(
            host="http://localhost:8080",
            config=valid_config_without_joins_and_dedup_disabled,
        )
        pipeline_id = valid_config_without_joins_and_dedup_disabled["pipeline_id"]
        assert pipeline._tracking_info() == {
            "pipeline_id": pipeline_id,
            "join_enabled": False,
            "deduplication_enabled": False,
            "source_auth_method": "SCRAM-SHA-256",
            "source_security_protocol": "SASL_SSL",
            "source_root_ca_provided": True,
            "source_skip_auth": False,
        }


class TestPipelineIO:
    """Tests for file operations."""

    def test_to_yaml(self, pipeline):
        """Test pipeline to YAML file."""
        # Use a temporary file that will be automatically cleaned up
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as temp_file:
            temp_path = temp_file.name
        try:
            pipeline.to_yaml(temp_path)
            assert os.path.exists(temp_path)
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_to_json(self, pipeline):
        """Test pipeline to JSON file."""
        # Use a temporary file that will be automatically cleaned up
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as temp_file:
            temp_path = temp_file.name
        try:
            pipeline.to_json(temp_path)
            assert os.path.exists(temp_path)
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_from_yaml(self, pipeline):
        """Test pipeline from YAML file."""
        pipeline = Pipeline.from_yaml("tests/data/valid_pipeline.yaml")
        assert pipeline.pipeline_id == "test-pipeline"

    def test_from_json(self, pipeline):
        """Test pipeline from JSON file."""
        pipeline = Pipeline.from_json("tests/data/valid_pipeline.json")
        assert pipeline.pipeline_id == "test-pipeline"

    def test_to_dict(self, pipeline):
        """Test pipeline to dictionary."""
        assert pipeline.to_dict() == pipeline.config.model_dump(
            mode="json", by_alias=True
        )

        pipeline = Pipeline(host="http://localhost:8080", pipeline_id="test-pipeline")
        assert pipeline.to_dict() == {"pipeline_id": "test-pipeline"}


class TestPipelineHealth:
    """Tests for pipeline health endpoint."""

    def test_health_success(self, pipeline, mock_success):
        """Test successful health fetch returns expected payload."""
        expected = {
            "pipeline_id": "test-pipeline",
            "pipeline_name": "Test Pipeline",
            "overall_status": "Terminating",
            "created_at": "2025-08-31T16:05:09.163872763Z",
            "updated_at": "2025-08-31T16:05:10.638243216Z",
        }
        with mock_success(expected) as mock_request:
            result = pipeline.health()
            mock_request.assert_called_once_with(
                "GET",
                f"{pipeline.ENDPOINT}/{pipeline.pipeline_id}/health",
            )
            assert result == expected
            assert pipeline.status == models.PipelineStatus.TERMINATING
