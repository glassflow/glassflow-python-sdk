import pytest

from glassflow.etl import models


class TestTopicConfig:
    """Tests for TopicConfig."""

    def test_topic_config_creation(self):
        """Test TopicConfig creation."""
        config = models.TopicConfig(
            name="test-topic",
            consumer_group_initial_offset=models.ConsumerGroupOffset.EARLIEST,
            deduplication=models.DeduplicationConfig(
                enabled=True,
                id_field="id",
                id_field_type=models.KafkaDataType.STRING,
                time_window="1h",
            ),
        )
        assert config.name == "test-topic"
        assert config.deduplication.id_field == "id"
        assert config.deduplication.id_field_type == models.KafkaDataType.STRING

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
    """Tests for PipelineConfig deduplication ID field validation."""

    def test_deduplication_id_field_validation_valid(self):
        """
        Test PipelineConfig validation for deduplication ID field with valid field.
        """
        schema = models.Schema(
            fields=[
                models.SchemaField(
                    source_id="test-topic",
                    name="id",
                    type=models.KafkaDataType.STRING,
                    column_name="id",
                    column_type=models.ClickhouseDataType.STRING,
                ),
            ]
        )
        source = models.SourceConfig(
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
                        id_field="id",
                        id_field_type=models.KafkaDataType.STRING,
                        time_window="1h",
                    ),
                ),
            ],
        )
        sink = models.SinkConfig(
            type=models.SinkType.CLICKHOUSE,
            host="localhost",
            port="9000",
            database="test",
            username="default",
            password="",
            table="test_table",
        )

        # Should not raise an error
        config = models.PipelineConfig(
            pipeline_id="test-pipeline",
            source=source,
            sink=sink,
            schema=schema,
        )
        assert config.pipeline_id == "test-pipeline"

    def test_deduplication_id_field_validation_invalid(self):
        """
        Test PipelineConfig validation for deduplication ID field with invalid field.
        """
        schema = models.Schema(
            fields=[
                models.SchemaField(
                    source_id="test-topic",
                    name="name",
                    type=models.KafkaDataType.STRING,
                    column_name="name",
                    column_type=models.ClickhouseDataType.STRING,
                ),
            ]
        )
        source = models.SourceConfig(
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
                        id_field="non-existent-field",
                        id_field_type=models.KafkaDataType.STRING,
                        time_window="1h",
                    ),
                ),
            ],
        )
        sink = models.SinkConfig(
            type=models.SinkType.CLICKHOUSE,
            host="localhost",
            port="9000",
            database="test",
            username="default",
            password="",
            table="test_table",
        )

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="test-pipeline",
                source=source,
                sink=sink,
                schema=schema,
            )
        assert "not found in schema" in str(exc_info.value)
