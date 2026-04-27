import copy

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
    """Tests for sink source_id in PipelineConfig."""

    def test_sink_source_id_optional(self, valid_config):
        """sink.source_id is optional."""
        config_data = copy.deepcopy(valid_config)
        config_data["sink"].pop("source_id", None)
        config = models.PipelineConfig(**config_data)
        assert config.sink.source_id is None

    def test_sink_valid_source_id(self, valid_config_without_joins):
        """sink.source_id matching a known source should not raise."""
        config = models.PipelineConfig(**valid_config_without_joins)
        source_ids = {s.source_id for s in config.sources}
        assert config.sink.source_id in source_ids
