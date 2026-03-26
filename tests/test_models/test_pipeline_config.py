import pytest

from glassflow.etl import models
from glassflow.etl.errors import ImmutableResourceError
from glassflow.etl.models.sources import OTLPSource


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
        assert resources.nats.stream.max_bytes == "1Gi"

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

    def test_pipeline_config_resources_update_transform_replicas_immutable_raises(
        self, valid_config
    ):
        """Updating pipeline_resources with transform.replicas raises."""
        config = models.PipelineConfig(**valid_config)
        patch = models.PipelineConfigPatch(
            pipeline_resources=models.PipelineResourcesConfig(
                transform=models.TransformResources(replicas=2)
            )
        )
        with pytest.raises(ImmutableResourceError) as _:
            config.update(patch)


class TestPipelineConfigOTLP:
    """Tests for PipelineConfig with an OTLP source."""

    def test_otlp_logs_pipeline_creation(self, valid_otlp_logs_config):
        config = models.PipelineConfig(**valid_otlp_logs_config)
        assert config.pipeline_id == "test-otlp-logs"
        assert isinstance(config.source, OTLPSource)
        assert config.source.type == "otlp.logs"
        assert config.source.id == "otlp-src"

    def test_otlp_metrics_pipeline_creation(self, valid_otlp_metrics_config):
        config = models.PipelineConfig(**valid_otlp_metrics_config)
        assert isinstance(config.source, OTLPSource)
        assert config.source.type == "otlp.metrics"

    def test_otlp_traces_pipeline_creation(self, valid_otlp_traces_config):
        config = models.PipelineConfig(**valid_otlp_traces_config)
        assert isinstance(config.source, OTLPSource)
        assert config.source.type == "otlp.traces"

    def test_otlp_pipeline_join_must_be_disabled(self, valid_otlp_logs_config):
        """OTLP pipelines must have join.enabled = False."""
        config_data = {
            **valid_otlp_logs_config,
            "join": {
                "enabled": True,
                "type": "temporal",
                "sources": [
                    {
                        "source_id": "a",
                        "key": "id",
                        "time_window": "1h",
                        "orientation": "left",
                    },
                    {
                        "source_id": "b",
                        "key": "id",
                        "time_window": "1h",
                        "orientation": "right",
                    },
                ],
            },
        }
        with pytest.raises(ValueError, match="join.enabled must be False"):
            models.PipelineConfig(**config_data)

    def test_otlp_pipeline_name_defaults_from_id(self, valid_otlp_logs_config):
        config = models.PipelineConfig(**valid_otlp_logs_config)
        assert config.name == "Test Otlp Logs"

    def test_otlp_pipeline_with_transformation(
        self, valid_otlp_with_transformation_config
    ):
        config = models.PipelineConfig(**valid_otlp_with_transformation_config)
        assert config.stateless_transformation.enabled is True
        assert config.stateless_transformation.id == "log_transform"
        assert config.stateless_transformation.source_id == "otlp-src"

    def test_otlp_pipeline_invalid_source_id(self, valid_otlp_logs_config):
        """Sink source_id must reference the OTLP source id."""
        config_data = {
            **valid_otlp_logs_config,
            "sink": {
                **valid_otlp_logs_config["sink"],
                "source_id": "non-existent-source",
            },
        }
        with pytest.raises(ValueError, match="does not match any known source"):
            models.PipelineConfig(**config_data)


class TestPipelineConfigKafka:
    """Tests for PipelineConfig with Kafka source features."""

    def test_pipeline_config_creation(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        assert config.pipeline_id == "test-v3-pipeline"
        assert isinstance(config.source, models.KafkaSource)

    def test_topic_id_field(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        topics = config.source.topics
        assert topics[0].id == "src-logins"
        assert topics[1].id == "src-orders"

    def test_topic_schema_registry(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        topic = config.source.topics[0]
        assert topic.schema_registry is not None
        assert topic.schema_registry.url == "https://schema-registry.example.com"

    def test_topic_schema_version(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        assert config.source.topics[0].schema_version == "1"

    def test_topic_deduplication_key(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        assert config.source.topics[0].deduplication.key == "session_id"

    def test_join_source_key(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        assert config.join.enabled is True
        for join_src in config.join.sources:
            assert join_src.key == "user_id"

    def test_join_fields(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        assert config.join.fields is not None
        assert len(config.join.fields) == 2
        assert config.join.fields[0].output_name == "login_session_id"
        assert config.join.fields[1].output_name is None

    def test_stateless_transformation_source_id(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        assert config.stateless_transformation.source_id == "src-logins"

    def test_sink_source_id_references_transformation(self, valid_v3_config):
        """Sink source_id references the stateless transformation id."""
        config = models.PipelineConfig(**valid_v3_config)
        assert config.sink.source_id == "my_transformation"

    def test_topic_schema_fields(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        topic = config.source.topics[0]
        assert topic.schema_fields is not None
        field_names = {f.name for f in topic.schema_fields}
        assert "session_id" in field_names
        assert "user_id" in field_names

    def test_sink_connection_params(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        assert config.sink.connection_params.http_port == "12754"
        assert config.sink.connection_params.password == "plaintext-password"

    def test_pipeline_deduplication_key_set(self, valid_config):
        """Deduplication key is correctly populated from config."""
        config = models.PipelineConfig(**valid_config)
        for topic in config.source.topics:
            if topic.deduplication and topic.deduplication.enabled:
                assert topic.deduplication.key is not None

    def test_pipeline_topic_without_id_falls_back_to_name(self, valid_config):
        """Topics without an explicit id use name as effective_id."""
        config = models.PipelineConfig(**valid_config)
        for topic in config.source.topics:
            assert topic.effective_id == topic.name

    def test_pipeline_sink_source_id_references_known_component(self, valid_config):
        """Sink source_id references a known upstream component."""
        config = models.PipelineConfig(**valid_config)
        topic_names = {t.name for t in config.source.topics}
        transform_id = (
            config.stateless_transformation.id
            if config.stateless_transformation
            else None
        )
        valid_ids = topic_names | ({transform_id} if transform_id else set())
        assert config.sink.source_id in valid_ids
