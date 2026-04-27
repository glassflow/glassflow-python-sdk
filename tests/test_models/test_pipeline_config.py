import pytest

from glassflow.etl import models
from glassflow.etl.models.sources import OTLPSource


class TestPipelineConfig:
    """Tests for PipelineConfig model."""

    def test_pipeline_config_creation(self, valid_config):
        """Test successful PipelineConfig creation."""
        pipeline_config = models.PipelineConfig(**valid_config)
        assert pipeline_config.pipeline_id == "test-pipeline"
        assert len(pipeline_config.sources) == 2
        assert pipeline_config.sources[0].type == "kafka"
        assert pipeline_config.sink.type == "clickhouse"

    def test_invalid_pipeline_config(self, invalid_config):
        """Test PipelineConfig creation with invalid configuration."""
        with pytest.raises(ValueError):
            models.PipelineConfig(**invalid_config)

    def test_pipeline_config_pipeline_id_validation(self, valid_config):
        """Test PipelineConfig validation for pipeline_id."""
        config = models.PipelineConfig(
            pipeline_id="test-pipeline-123a",
            sources=valid_config["sources"],
            join=valid_config["join"],
            sink=valid_config["sink"],
            transforms=valid_config["transforms"],
        )
        assert config.pipeline_id == "test-pipeline-123a"

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="",
                sources=valid_config["sources"],
                join=valid_config["join"],
                sink=valid_config["sink"],
            )
        assert "pipeline_id cannot be empty" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="Test_Pipeline",
                sources=valid_config["sources"],
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
                sources=valid_config["sources"],
                join=valid_config["join"],
                sink=valid_config["sink"],
            )
        assert "pipeline_id cannot be longer than 40 characters" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="-test-pipeline",
                sources=valid_config["sources"],
                join=valid_config["join"],
                sink=valid_config["sink"],
            )
        assert "pipeline_id must start with a lowercase alphanumeric" in str(
            exc_info.value
        )

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="test-pipeline-",
                sources=valid_config["sources"],
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
            sources=valid_config["sources"],
            join=valid_config["join"],
            sink=valid_config["sink"],
            transforms=valid_config["transforms"],
        )
        assert config.pipeline_id == "test-pipeline"
        assert config.name == "My Custom Pipeline Name"

    def test_pipeline_config_pipeline_name_not_provided(self, valid_config):
        """Test PipelineConfig when pipeline_name is not provided (default behavior)."""
        config = models.PipelineConfig(
            pipeline_id="test-pipeline",
            sources=valid_config["sources"],
            join=valid_config["join"],
            sink=valid_config["sink"],
            transforms=valid_config["transforms"],
        )
        assert config.pipeline_id == "test-pipeline"
        assert config.name == "Test Pipeline"

    def test_pipeline_config_creation_with_resources(
        self, valid_config_with_pipeline_resources
    ):
        """Test PipelineConfig creation and validation with resources."""
        config = models.PipelineConfig(**valid_config_with_pipeline_resources)
        assert config.pipeline_id == "test-pipeline"
        assert config.resources is not None

        resources = config.resources
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

        # Sources
        assert resources.sources is not None
        assert len(resources.sources) == 1
        assert resources.sources[0].source_id == "user-logins"
        assert resources.sources[0].replicas == 2

        # Transform
        assert resources.transform is not None
        assert len(resources.transform) == 1
        assert resources.transform[0].storage is not None
        assert resources.transform[0].storage.size == "10Gi"
        assert resources.transform[0].replicas == 1

    def test_pipeline_config_resources_optional(self, valid_config):
        """Test that resources is optional and defaults to None."""
        config = models.PipelineConfig(**valid_config)
        assert config.pipeline_id == "test-pipeline"
        assert config.resources is None

    def test_pipeline_config_resources_partial(self, valid_config):
        """Test PipelineConfig with partial resources (only some sections)."""
        config_data = {**valid_config, "resources": {"sink": {"replicas": 3}}}
        config = models.PipelineConfig(**config_data)
        assert config.resources is not None
        assert config.resources.sink is not None
        assert config.resources.sink.replicas == 3
        assert config.resources.nats is None
        assert config.resources.transform is None


class TestPipelineConfigOTLP:
    """Tests for PipelineConfig with an OTLP source."""

    def test_otlp_logs_pipeline_creation(self, valid_otlp_logs_config):
        config = models.PipelineConfig(**valid_otlp_logs_config)
        assert config.pipeline_id == "test-otlp-logs"
        assert isinstance(config.sources[0], OTLPSource)
        assert config.sources[0].type == "otlp.logs"
        assert config.sources[0].source_id == "otlp-src"

    def test_otlp_metrics_pipeline_creation(self, valid_otlp_metrics_config):
        config = models.PipelineConfig(**valid_otlp_metrics_config)
        assert isinstance(config.sources[0], OTLPSource)
        assert config.sources[0].type == "otlp.metrics"

    def test_otlp_traces_pipeline_creation(self, valid_otlp_traces_config):
        config = models.PipelineConfig(**valid_otlp_traces_config)
        assert isinstance(config.sources[0], OTLPSource)
        assert config.sources[0].type == "otlp.traces"

    def test_otlp_pipeline_join_must_be_disabled(self, valid_otlp_logs_config):
        """OTLP pipelines must have join.enabled = False."""
        config_data = {
            **valid_otlp_logs_config,
            "join": {
                "enabled": True,
                "type": "temporal",
                "left_source": {
                    "source_id": "a",
                    "key": "id",
                    "time_window": "1h",
                },
                "right_source": {
                    "source_id": "b",
                    "key": "id",
                    "time_window": "1h",
                },
                "output_fields": [
                    {"source_id": "a", "name": "f1"},
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
        assert config.transforms is not None
        stateless = [
            t for t in config.transforms if t.type == models.TransformType.STATELESS
        ]
        assert len(stateless) == 1
        assert stateless[0].source_id == "otlp-src"

    def test_otlp_pipeline_sink_source_id_optional(self, valid_otlp_logs_config):
        """Sink source_id is optional."""
        import copy

        config_data = copy.deepcopy(valid_otlp_logs_config)
        config_data["sink"].pop("source_id", None)
        config = models.PipelineConfig(**config_data)
        assert config.sink.source_id is None


class TestPipelineConfigKafka:
    """Tests for PipelineConfig with Kafka source features."""

    def test_pipeline_config_creation(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        assert config.pipeline_id == "test-v3-pipeline"
        assert isinstance(config.sources[0], models.KafkaSource)

    def test_source_ids(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        source_ids = [s.source_id for s in config.sources]
        assert "src-logins" in source_ids
        assert "src-orders" in source_ids

    def test_source_schema_registry(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        logins_src = next(s for s in config.sources if s.source_id == "src-logins")
        assert logins_src.schema_registry is not None
        assert logins_src.schema_registry.url == "https://schema-registry.example.com"

    def test_source_schema_version(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        logins_src = next(s for s in config.sources if s.source_id == "src-logins")
        assert logins_src.schema_version == "1"

    def test_dedup_transforms(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        dedup_transforms = [
            t for t in config.transforms if t.type == models.TransformType.DEDUP
        ]
        assert len(dedup_transforms) == 2
        assert dedup_transforms[0].config.key == "session_id"

    def test_join_left_right_sources(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        assert config.join.enabled is True
        assert config.join.left_source.source_id == "src-logins"
        assert config.join.left_source.key == "user_id"
        assert config.join.right_source.source_id == "src-orders"
        assert config.join.right_source.key == "user_id"

    def test_join_output_fields(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        assert config.join.output_fields is not None
        assert len(config.join.output_fields) == 2
        assert config.join.output_fields[0].output_name == "login_session_id"
        assert config.join.output_fields[1].output_name is None

    def test_stateless_transform(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        stateless = [
            t for t in config.transforms if t.type == models.TransformType.STATELESS
        ]
        assert len(stateless) == 1
        assert stateless[0].source_id == "src-logins"

    def test_source_schema_fields(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        logins_src = next(s for s in config.sources if s.source_id == "src-logins")
        assert logins_src.schema_fields is not None
        field_names = {f.name for f in logins_src.schema_fields}
        assert "session_id" in field_names
        assert "user_id" in field_names

    def test_sink_connection_params(self, valid_v3_config):
        config = models.PipelineConfig(**valid_v3_config)
        assert config.sink.connection_params.http_port == "12754"
        assert config.sink.connection_params.password == "plaintext-password"

    def test_pipeline_has_dedup(self, valid_config):
        """Deduplication transforms are present."""
        config = models.PipelineConfig(**valid_config)
        dedup = [t for t in config.transforms if t.type == models.TransformType.DEDUP]
        assert len(dedup) > 0

    def test_pipeline_sink_source_id_references_known_source(
        self, valid_config_without_joins
    ):
        """Sink source_id references a known source."""
        config = models.PipelineConfig(**valid_config_without_joins)
        source_ids = {s.source_id for s in config.sources}
        assert config.sink.source_id in source_ids
