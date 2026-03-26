import pytest

from glassflow.etl import models
from glassflow.etl.models.sources import SchemaRegistry, TopicConfig


class TestSchemaRegistry:
    """Tests for the SchemaRegistry model."""

    def test_schema_registry_creation(self):
        reg = SchemaRegistry(
            url="https://schema-registry.example.com",
            api_key="key123",
            api_secret="secret456",
        )
        assert reg.url == "https://schema-registry.example.com"
        assert reg.api_key == "key123"
        assert reg.api_secret == "secret456"

    def test_schema_registry_from_dict(self):
        reg = SchemaRegistry.model_validate(
            {
                "url": "https://sr.example.com",
                "api_key": "k",
                "api_secret": "s",
            }
        )
        assert reg.url == "https://sr.example.com"

    def test_schema_registry_missing_required_field(self):
        with pytest.raises(ValueError):
            SchemaRegistry(url="https://sr.example.com", api_key="k")


class TestTopicConfigExtended:
    """Tests for extended TopicConfig fields (id, schema_registry, schema_fields)."""

    def test_topic_config_with_id(self):
        topic = TopicConfig(name="user_events", id="src-events")
        assert topic.id == "src-events"
        assert topic.effective_id == "src-events"

    def test_topic_config_without_id_effective_id_falls_back_to_name(self):
        topic = TopicConfig(name="user_events")
        assert topic.id is None
        assert topic.effective_id == "user_events"

    def test_topic_config_with_schema_registry(self):
        topic = TopicConfig(
            name="events",
            schema_registry=SchemaRegistry(
                url="https://sr.example.com",
                api_key="k",
                api_secret="s",
            ),
            schema_version="1",
        )
        assert topic.schema_registry is not None
        assert topic.schema_registry.url == "https://sr.example.com"

    def test_topic_config_schema_registry_requires_version(self):
        """schema_version is required when schema_registry is provided."""
        with pytest.raises(ValueError, match="schema_version is required"):
            TopicConfig(
                name="events",
                schema_registry=SchemaRegistry(
                    url="https://sr.example.com",
                    api_key="k",
                    api_secret="s",
                ),
            )

    def test_topic_config_with_schema_version(self):
        topic = TopicConfig(name="events", schema_version="2")
        assert topic.schema_version == "2"

    def test_topic_config_schema_registry_and_version_optional(self):
        topic = TopicConfig(name="events")
        assert topic.schema_registry is None
        assert topic.schema_version is None

    def test_topic_config_with_schema_registry_from_dict(self):
        data = {
            "name": "events",
            "id": "ev-src",
            "schema_registry": {
                "url": "https://sr.example.com",
                "api_key": "key",
                "api_secret": "secret",
            },
            "schema_version": "3",
        }
        topic = TopicConfig.model_validate(data)
        assert topic.id == "ev-src"
        assert topic.schema_registry.api_key == "key"
        assert topic.schema_version == "3"

    def test_topic_config_with_schema_fields(self):
        topic = TopicConfig(
            name="events",
            schema_fields=[
                models.KafkaField(name="id", type=models.KafkaDataType.STRING),
                models.KafkaField(name="value", type=models.KafkaDataType.INT64),
            ],
        )
        assert topic.schema_fields is not None
        assert len(topic.schema_fields) == 2
        assert topic.schema_fields[0].name == "id"
        assert topic.schema_fields[0].type == models.KafkaDataType.STRING

    def test_topic_config_schema_fields_optional(self):
        topic = TopicConfig(name="events")
        assert topic.schema_fields is None

    def test_topic_config_schema_fields_from_dict(self):
        data = {
            "name": "events",
            "schema_fields": [
                {"name": "session_id", "type": "string"},
                {"name": "user_id", "type": "string"},
            ],
        }
        topic = TopicConfig.model_validate(data)
        assert topic.schema_fields is not None
        assert len(topic.schema_fields) == 2
        assert topic.schema_fields[0].name == "session_id"


class TestTopicConfig:
    """Tests for TopicConfig."""

    def test_topic_config_creation(self):
        """Test TopicConfig creation."""
        config = models.TopicConfig(
            name="test-topic",
            consumer_group_initial_offset=models.ConsumerGroupOffset.EARLIEST,
            deduplication=models.DeduplicationConfig(
                enabled=True,
                key="id",
                time_window="1h",
            ),
        )
        assert config.name == "test-topic"
        assert config.deduplication.key == "id"

    def test_topic_config_with_disabled_deduplication(self):
        """Test TopicConfig with disabled deduplication."""
        config = models.TopicConfig(
            name="test-topic",
            consumer_group_initial_offset=models.ConsumerGroupOffset.EARLIEST,
            deduplication=models.DeduplicationConfig(
                enabled=False,
            ),
        )
        assert config.deduplication.enabled is False

    def test_topic_config_replicas_validation(self):
        """Test TopicConfig validation for replicas."""
        config = models.TopicConfig(
            name="test-topic",
            consumer_group_initial_offset=models.ConsumerGroupOffset.EARLIEST,
            replicas=3,
        )
        assert config.replicas == 3

        with pytest.raises(ValueError) as exc_info:
            models.TopicConfig(
                name="test-topic",
                consumer_group_initial_offset=models.ConsumerGroupOffset.EARLIEST,
                replicas=0,
            )
        assert "Replicas must be at least 1" in str(exc_info.value)


class TestPipelineConfigDeduplicationValidation:
    """Tests for PipelineConfig deduplication key field validation."""

    def test_deduplication_key_field_validation_valid_with_fields(self):
        """
        Test PipelineConfig validation for deduplication key with topic fields defined
        and the key present.
        """
        source = models.KafkaSource(
            type=models.SourceType.KAFKA,
            connection_params=models.KafkaConnectionParams(
                brokers=["localhost:9092"],
                protocol=models.KafkaProtocol.PLAINTEXT,
            ),
            topics=[
                models.TopicConfig(
                    name="test-topic",
                    deduplication=models.DeduplicationConfig(
                        enabled=True,
                        key="id",
                        time_window="1h",
                    ),
                    schema_fields=[
                        models.KafkaField(name="id", type=models.KafkaDataType.STRING),
                    ],
                ),
            ],
        )
        sink = models.SinkConfig(
            type=models.SinkType.CLICKHOUSE,
            connection_params=models.ClickhouseConnectionParams(
                host="localhost",
                port="9000",
                database="test",
                username="default",
                password="",
            ),
            table="test_table",
        )

        # Should not raise an error
        config = models.PipelineConfig(
            pipeline_id="test-pipeline",
            source=source,
            sink=sink,
        )
        assert config.pipeline_id == "test-pipeline"

    def test_deduplication_key_field_validation_valid_without_fields(self):
        """
        Test PipelineConfig validation for deduplication key when topic has no fields
        defined — validation is skipped.
        """
        source = models.KafkaSource(
            type=models.SourceType.KAFKA,
            connection_params=models.KafkaConnectionParams(
                brokers=["localhost:9092"],
                protocol=models.KafkaProtocol.PLAINTEXT,
            ),
            topics=[
                models.TopicConfig(
                    name="test-topic",
                    deduplication=models.DeduplicationConfig(
                        enabled=True,
                        key="non-existent-field",
                        time_window="1h",
                    ),
                    # No schema_fields — validation skipped
                ),
            ],
        )
        sink = models.SinkConfig(
            type=models.SinkType.CLICKHOUSE,
            connection_params=models.ClickhouseConnectionParams(
                host="localhost",
                port="9000",
                database="test",
                username="default",
                password="",
            ),
            table="test_table",
        )

        # Should not raise because no schema_fields defined
        config = models.PipelineConfig(
            pipeline_id="test-pipeline",
            source=source,
            sink=sink,
        )
        assert config.pipeline_id == "test-pipeline"

    def test_deduplication_key_field_validation_invalid(self):
        """
        Test PipelineConfig validation for deduplication key with invalid field
        (schema_fields defined but key not in them).
        """
        source = models.KafkaSource(
            type=models.SourceType.KAFKA,
            connection_params=models.KafkaConnectionParams(
                brokers=["localhost:9092"],
                protocol=models.KafkaProtocol.PLAINTEXT,
            ),
            topics=[
                models.TopicConfig(
                    name="test-topic",
                    deduplication=models.DeduplicationConfig(
                        enabled=True,
                        key="non-existent-field",
                        time_window="1h",
                    ),
                    schema_fields=[
                        models.KafkaField(
                            name="name", type=models.KafkaDataType.STRING
                        ),
                    ],
                ),
            ],
        )
        sink = models.SinkConfig(
            type=models.SinkType.CLICKHOUSE,
            connection_params=models.ClickhouseConnectionParams(
                host="localhost",
                port="9000",
                database="test",
                username="default",
                password="",
            ),
            table="test_table",
        )

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="test-pipeline",
                source=source,
                sink=sink,
            )
        assert "not found in fields" in str(exc_info.value)
