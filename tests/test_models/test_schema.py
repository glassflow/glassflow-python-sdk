import pytest

from glassflow.etl import models


class TestSinkFieldMapping:
    """Tests for SinkFieldMapping model."""

    def test_sink_field_mapping_creation(self):
        """Test SinkFieldMapping creation."""
        mapping = models.SinkFieldMapping(
            name="session_id",
            column_name="session_id",
            column_type=models.ClickhouseDataType.STRING,
        )
        assert mapping.name == "session_id"
        assert mapping.column_name == "session_id"
        assert mapping.column_type == models.ClickhouseDataType.STRING

    def test_sink_field_mapping_from_dict(self):
        """Test SinkFieldMapping creation from dict."""
        data = {
            "name": "ts",
            "column_name": "event_time",
            "column_type": "DateTime",
        }
        mapping = models.SinkFieldMapping.model_validate(data)
        assert mapping.column_type == models.ClickhouseDataType.DATETIME

    def test_sink_config_with_mapping(self):
        """Test SinkConfig with mapping."""
        sink = models.SinkConfig(
            connection_params=models.ClickhouseConnectionParams(
                host="localhost",
                port="9000",
                database="test",
                username="default",
                password="",
            ),
            table="test_table",
            mapping=[
                models.SinkFieldMapping(
                    name="id",
                    column_name="id",
                    column_type=models.ClickhouseDataType.STRING,
                ),
            ],
        )
        assert sink.mapping is not None
        assert len(sink.mapping) == 1
        assert sink.mapping[0].name == "id"

    def test_sink_config_mapping_optional(self):
        """Test SinkConfig without mapping."""
        sink = models.SinkConfig(
            connection_params=models.ClickhouseConnectionParams(
                host="localhost",
                port="9000",
                database="test",
                username="default",
                password="",
            ),
            table="test_table",
        )
        assert sink.mapping is None


class TestSinkSourceIdValidation:
    """Tests for sink source_id validation in PipelineConfig."""

    def test_sink_invalid_source_id(self, valid_config):
        """sink.source_id must match a known topic or transformation id."""
        import copy

        config_data = copy.deepcopy(valid_config)
        config_data["sink"]["source_id"] = "non-existent-topic"

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(**config_data)
        assert "does not match any known source" in str(exc_info.value)

    def test_sink_valid_source_id(self, valid_config):
        """sink.source_id matching a known component should not raise."""
        config = models.PipelineConfig(**valid_config)
        # Verify the source_id is valid
        topic_names = {t.name for t in config.source.topics}
        valid_ids = topic_names | {config.stateless_transformation.id}
        assert config.sink.source_id in valid_ids
