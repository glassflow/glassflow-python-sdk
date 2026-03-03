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
            schema=valid_config["schema"],
            stateless_transformation=valid_config["stateless_transformation"],
        )
        assert config.pipeline_id == "test-pipeline-123a"

        # Test with invalid configuration
        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="",
                source=valid_config["source"],
                join=valid_config["join"],
                sink=valid_config["sink"],
                schema=valid_config["schema"],
            )
        assert "pipeline_id cannot be empty" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="Test_Pipeline",
                source=valid_config["source"],
                join=valid_config["join"],
                sink=valid_config["sink"],
                schema=valid_config["schema"],
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
                schema=valid_config["schema"],
            )
        assert "pipeline_id cannot be longer than 40 characters" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="-test-pipeline",
                source=valid_config["source"],
                join=valid_config["join"],
                sink=valid_config["sink"],
                schema=valid_config["schema"],
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
                schema=valid_config["schema"],
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
            schema=valid_config["schema"],
            stateless_transformation=valid_config["stateless_transformation"],
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
            schema=valid_config["schema"],
            stateless_transformation=valid_config["stateless_transformation"],
        )
        assert config.pipeline_id == "test-pipeline"
        assert config.name == "Test Pipeline"

    def test_pipeline_config_creation_with_pipeline_resources(
        self, valid_config_with_pipeline_resources
    ):
        """Test PipelineConfig creation and validation with pipeline_resources."""
        config = models.PipelineConfig(**valid_config_with_pipeline_resources)
        assert config.pipeline_id == "test-pipeline"
        assert config.pipeline_resources is not None

        resources = config.pipeline_resources
        # NATS / JetStream
        assert resources.nats is not None
        assert resources.nats.stream is not None
        assert resources.nats.stream.max_age == "72h"
        assert resources.nats.stream.max_bytes == "1GB"

        # Sink
        assert resources.sink is not None
        assert resources.sink.replicas == 2
        assert resources.sink.requests is not None
        assert resources.sink.requests.memory == "256Mi"
        assert resources.sink.requests.cpu == "100m"
        assert resources.sink.limits is not None
        assert resources.sink.limits.memory == "512Mi"
        assert resources.sink.limits.cpu == "500m"

        # Transform
        assert resources.transform is not None
        assert resources.transform.storage is not None
        assert resources.transform.storage.size == "10Gi"
        assert resources.transform.replicas == 1
        assert resources.transform.requests.memory == "128Mi"
        assert resources.transform.limits.cpu == "200m"

        # Join
        assert resources.join is not None
        assert resources.join.replicas == 1
        assert resources.join.requests.memory == "64Mi"
        assert resources.join.limits.cpu == "100m"

        # Ingestor
        assert resources.ingestor is not None
        assert resources.ingestor.base is not None
        assert resources.ingestor.base.replicas == 2
        assert resources.ingestor.base.requests.memory == "128Mi"

    def test_pipeline_config_pipeline_resources_optional(self, valid_config):
        """Test that pipeline_resources is optional and defaults to None."""
        config = models.PipelineConfig(**valid_config)
        assert config.pipeline_id == "test-pipeline"
        assert config.pipeline_resources is None

    def test_pipeline_config_pipeline_resources_partial(self, valid_config):
        """Test PipelineConfig with partial pipeline_resources (only some sections)."""
        config_data = {**valid_config, "pipeline_resources": {"sink": {"replicas": 3}}}
        config = models.PipelineConfig(**config_data)
        assert config.pipeline_resources is not None
        assert config.pipeline_resources.sink is not None
        assert config.pipeline_resources.sink.replicas == 3
        assert config.pipeline_resources.nats is None
        assert config.pipeline_resources.transform is None
