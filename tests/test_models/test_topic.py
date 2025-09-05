import pytest

from glassflow.etl import models


class TestTopicConfig:
    """Tests for TopicConfig deduplication validation."""

    def test_topic_config_deduplication_id_field_validation(self):
        """Test TopicConfig validation for deduplication ID field."""
        # Test with valid configuration
        config = models.TopicConfig(
            name="test-topic",
            consumer_group_initial_offset=models.ConsumerGroupOffset.EARLIEST,
            schema=models.Schema(
                type=models.SchemaType.JSON,
                fields=[
                    models.SchemaField(name="id", type=models.KafkaDataType.STRING),
                    models.SchemaField(name="name", type=models.KafkaDataType.STRING),
                ],
            ),
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

        # Test with non-existent ID field
        with pytest.raises(ValueError) as exc_info:
            models.TopicConfig(
                name="test-topic",
                consumer_group_initial_offset=models.ConsumerGroupOffset.EARLIEST,
                schema=models.Schema(
                    type=models.SchemaType.JSON,
                    fields=[
                        models.SchemaField(
                            name="name",
                            type=models.KafkaDataType.STRING,
                        ),
                    ],
                ),
                deduplication=models.DeduplicationConfig(
                    enabled=True,
                    id_field="non-existent-field",
                    id_field_type=models.KafkaDataType.STRING,
                    time_window="1h",
                ),
            )
        assert "does not exist in the event schema" in str(exc_info.value)

        # Test with mismatched field type
        with pytest.raises(ValueError) as exc_info:
            models.TopicConfig(
                name="test-topic",
                consumer_group_initial_offset=models.ConsumerGroupOffset.EARLIEST,
                schema=models.Schema(
                    type=models.SchemaType.JSON,
                    fields=[
                        models.SchemaField(name="id", type=models.KafkaDataType.INT64),
                    ],
                ),
                deduplication=models.DeduplicationConfig(
                    enabled=True,
                    id_field="id",
                    id_field_type=models.KafkaDataType.STRING,
                    time_window="1h",
                ),
            )
        assert "does not match schema field type" in str(exc_info.value)

        # Test with disabled deduplication (should not validate)
        config = models.TopicConfig(
            name="test-topic",
            consumer_group_initial_offset=models.ConsumerGroupOffset.EARLIEST,
            schema=models.Schema(
                type=models.SchemaType.JSON,
                fields=[
                    models.SchemaField(name="name", type=models.KafkaDataType.STRING),
                ],
            ),
            deduplication=models.DeduplicationConfig(
                enabled=False,
                id_field="non-existent-field",
                id_field_type=models.KafkaDataType.STRING,
                time_window="1h",
            ),
        )
        assert config.deduplication.enabled is False
