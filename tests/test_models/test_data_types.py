import pytest

from glassflow.etl import models


class TestDataTypeCompatibility:
    """Tests for data type compatibility in models."""

    def test_sink_field_mapping_valid_types(self, valid_config):
        """Test that valid Clickhouse types are accepted in SinkFieldMapping."""
        config = models.PipelineConfig(**valid_config)
        # Verify that mapping has valid types
        for mapping in config.sink.mapping:
            assert isinstance(mapping.column_type, models.ClickhouseDataType)

    def test_sink_field_mapping_invalid_column_type(self):
        """Test that invalid Clickhouse column type raises a validation error."""
        with pytest.raises(ValueError):
            models.SinkFieldMapping(
                name="id",
                column_name="id",
                column_type="NotAValidType",
            )

    def test_kafka_field_valid_types(self):
        """Test that valid Kafka types are accepted in KafkaField."""
        field = models.KafkaField(name="ts", type=models.KafkaDataType.INT64)
        assert field.type == models.KafkaDataType.INT64

    def test_kafka_field_invalid_type(self):
        """Test that invalid Kafka type raises a validation error."""
        with pytest.raises(ValueError):
            models.KafkaField(name="ts", type="not_a_type")
