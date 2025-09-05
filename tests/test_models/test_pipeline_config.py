import pytest

from glassflow.etl import models


class TestPipelineConfig:
    """Tests for PipelineConfig model."""

    def test_pipeline_config_creation(self, valid_config):
        """Test successful PipelineConfig creation."""
        pipeline_config = models.PipelineConfig(**valid_config)
        assert pipeline_config.pipeline_id == "test-pipeline"
        assert pipeline_config.source.type == "kafka"
        assert pipeline_config.sink.type == "clickhouse"

    def test_invalid_pipeline_config(self, invalid_config):
        """Test PipelineConfig creation with invalid configuration."""
        with pytest.raises(ValueError):
            models.PipelineConfig(**invalid_config)

    def test_pipeline_config_pipeline_id_validation(self, valid_config):
        """Test PipelineConfig validation for pipeline_id."""
        # Test with valid configuration
        config = models.PipelineConfig(
            pipeline_id="test-pipeline-123a",
            source=valid_config["source"],
            join=valid_config["join"],
            sink=valid_config["sink"],
        )
        assert config.pipeline_id == "test-pipeline-123a"

        # Test with invalid configuration
        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="",
                source=valid_config["source"],
                join=valid_config["join"],
                sink=valid_config["sink"],
            )
        assert "pipeline_id cannot be empty" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="Test_Pipeline",
                source=valid_config["source"],
                join=valid_config["join"],
                sink=valid_config["sink"],
            )
        assert (
            "pipeline_id can only contain lowercase letters, numbers, and hyphens"
            in str(exc_info.value)
        )

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="test-pipeline-1234567890123456789012345678901234567890",
                source=valid_config["source"],
                join=valid_config["join"],
                sink=valid_config["sink"],
            )
        assert "pipeline_id cannot be longer than 40 characters" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="-test-pipeline",
                source=valid_config["source"],
                join=valid_config["join"],
                sink=valid_config["sink"],
            )
        assert "pipeline_id must start with a lowercase alphanumeric" in str(
            exc_info.value
        )

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="test-pipeline-",
                source=valid_config["source"],
                join=valid_config["join"],
                sink=valid_config["sink"],
            )
        assert "pipeline_id must end with a lowercase alphanumeric" in str(
            exc_info.value
        )

    def test_pipeline_config_pipeline_name_provided(self, valid_config):
        """Test PipelineConfig when pipeline_name is explicitly provided."""
        config = models.PipelineConfig(
            pipeline_id="test-pipeline",
            name="My Custom Pipeline Name",
            source=valid_config["source"],
            join=valid_config["join"],
            sink=valid_config["sink"],
        )
        assert config.pipeline_id == "test-pipeline"
        assert config.name == "My Custom Pipeline Name"

    def test_pipeline_config_pipeline_name_not_provided(self, valid_config):
        """Test PipelineConfig when pipeline_name is not provided (default behavior)."""
        config = models.PipelineConfig(
            pipeline_id="test-pipeline",
            source=valid_config["source"],
            join=valid_config["join"],
            sink=valid_config["sink"],
        )
        assert config.pipeline_id == "test-pipeline"
        assert config.name == "Test Pipeline"
