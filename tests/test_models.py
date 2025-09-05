import pytest

from glassflow.etl import errors, models


def test_pipeline_config_creation(valid_pipeline_config):
    pipeline_config = models.PipelineConfig(**valid_pipeline_config)
    assert pipeline_config.pipeline_id == "test-pipeline"
    assert pipeline_config.source.type == "kafka"
    assert pipeline_config.sink.type == "clickhouse"


def test_invalid_pipeline_config(invalid_pipeline_config):
    with pytest.raises(ValueError):
        models.PipelineConfig(**invalid_pipeline_config)


def test_deduplication_config_enabled_true():
    """Test DeduplicationConfig when enabled is True."""
    with pytest.raises(ValueError) as exc_info:
        models.DeduplicationConfig(
            enabled=True,
            id_field=None,
            id_field_type=None,
            time_window=None,
        )
    assert "is required when deduplication is enabled" in str(exc_info.value)

    # All fields should be required when enabled is True
    config = models.DeduplicationConfig(
        enabled=True,
        id_field="id",
        id_field_type="string",
        time_window="1h",
    )
    assert config.enabled is True
    assert config.id_field == "id"
    assert config.id_field_type == "string"
    assert config.time_window == "1h"


def test_deduplication_config_enabled_false():
    """Test DeduplicationConfig when enabled is False."""
    # All fields should be optional when enabled is False
    config = models.DeduplicationConfig(
        enabled=False,
        id_field=None,
        id_field_type=None,
        time_window=None,
    )
    assert config.enabled is False
    assert config.id_field is None
    assert config.id_field_type is None
    assert config.time_window is None


def test_join_config_enabled_true():
    """Test JoinConfig when enabled is True."""
    with pytest.raises(ValueError) as exc_info:
        models.JoinConfig(
            enabled=True,
            type=None,
            sources=None,
        )
    assert "type is required when join is enabled" in str(exc_info.value)

    # Test with only one source
    with pytest.raises(ValueError) as exc_info:
        models.JoinConfig(
            enabled=True,
            type=models.JoinType.TEMPORAL,
            sources=[
                models.JoinSourceConfig(
                    source_id="test-topic",
                    join_key="id",
                    time_window="1h",
                    orientation=models.JoinOrientation.LEFT,
                )
            ],
        )
    assert "join must have exactly two sources when enabled" in str(exc_info.value)

    # Test with two sources but same orientation
    with pytest.raises(ValueError) as exc_info:
        models.JoinConfig(
            enabled=True,
            type=models.JoinType.TEMPORAL,
            sources=[
                models.JoinSourceConfig(
                    source_id="test-topic-1",
                    join_key="id",
                    time_window="1h",
                    orientation=models.JoinOrientation.LEFT,
                ),
                models.JoinSourceConfig(
                    source_id="test-topic-2",
                    join_key="id",
                    time_window="1h",
                    orientation=models.JoinOrientation.LEFT,
                ),
            ],
        )
    assert "join sources must have opposite orientations" in str(exc_info.value)

    # Test with valid configuration
    config = models.JoinConfig(
        enabled=True,
        type=models.JoinType.TEMPORAL,
        sources=[
            models.JoinSourceConfig(
                source_id="test-topic-1",
                join_key="id",
                time_window="1h",
                orientation=models.JoinOrientation.LEFT,
            ),
            models.JoinSourceConfig(
                source_id="test-topic-2",
                join_key="id",
                time_window="1h",
                orientation=models.JoinOrientation.RIGHT,
            ),
        ],
    )
    assert config.enabled is True
    assert config.type == models.JoinType.TEMPORAL
    assert len(config.sources) == 2
    assert config.sources[0].orientation == models.JoinOrientation.LEFT
    assert config.sources[1].orientation == models.JoinOrientation.RIGHT


def test_join_config_enabled_false():
    """Test JoinConfig when enabled is False."""
    # All fields should be optional when enabled is False
    config = models.JoinConfig(
        enabled=False,
        type=None,
        sources=None,
    )
    assert config.enabled is False
    assert config.type is None
    assert config.sources is None


def test_validate_join_config_source_id_not_found(valid_pipeline_config):
    """Test join config validation when source_id does not exist in topics."""
    join = models.JoinConfig(
        enabled=True,
        type=models.JoinType.TEMPORAL,
        sources=[
            models.JoinSourceConfig(
                source_id="non-existent-topic",  # This topic doesn't exist
                join_key="id",
                time_window="1h",
                orientation=models.JoinOrientation.LEFT,
            ),
            models.JoinSourceConfig(
                source_id=valid_pipeline_config["source"]["topics"][1]["name"],
                join_key="id",
                time_window="1h",
                orientation=models.JoinOrientation.RIGHT,
            ),
        ],
    )

    with pytest.raises(ValueError) as exc_info:
        models.PipelineConfig(
            pipeline_id="test-pipeline",
            source=valid_pipeline_config["source"],
            join=join,
            sink=valid_pipeline_config["sink"],
        )
    assert "does not exist in any topic" in str(exc_info.value)


def test_validate_join_config_join_key_not_found(valid_pipeline_config):
    """Test join config validation when join_key does not exist in schema."""
    join = models.JoinConfig(
        enabled=True,
        type=models.JoinType.TEMPORAL,
        sources=[
            models.JoinSourceConfig(
                source_id=valid_pipeline_config["source"]["topics"][0]["name"],
                join_key="non-existent-field",  # This field doesn't exist
                time_window="1h",
                orientation=models.JoinOrientation.LEFT,
            ),
            models.JoinSourceConfig(
                source_id=valid_pipeline_config["source"]["topics"][1]["name"],
                join_key="id",
                time_window="1h",
                orientation=models.JoinOrientation.RIGHT,
            ),
        ],
    )

    with pytest.raises(ValueError) as exc_info:
        models.PipelineConfig(
            pipeline_id="test-pipeline",
            source=valid_pipeline_config["source"],
            join=join,
            sink=valid_pipeline_config["sink"],
        )
    assert "does not exist in source" in str(exc_info.value)
    assert "schema" in str(exc_info.value)


def test_validate_sink_config_source_id_not_found(valid_pipeline_config):
    """Test sink config validation when source_id does not exist in topics."""

    sink = valid_pipeline_config["sink"]
    sink["table_mapping"] = [
        models.TableMapping(
            source_id="non-existent-topic",  # This topic doesn't exist
            field_name="id",
            column_name="id",
            column_type="String",
        )
    ]
    with pytest.raises(ValueError) as exc_info:
        models.PipelineConfig(
            pipeline_id="test-pipeline",
            source=valid_pipeline_config["source"],
            sink=sink,
        )
    assert "does not exist in any topic" in str(exc_info.value)


def test_validate_sink_config_field_name_not_found(valid_pipeline_config):
    """Test sink config validation when field_name does not exist in schema."""
    sink = valid_pipeline_config["sink"]
    sink["table_mapping"] = [
        models.TableMapping(
            source_id=valid_pipeline_config["source"]["topics"][0]["name"],
            field_name="non-existent-field",  # This field doesn't exist
            column_name="id",
            column_type="String",
        )
    ]
    with pytest.raises(ValueError) as exc_info:
        models.PipelineConfig(
            pipeline_id="test-pipeline",
            source=valid_pipeline_config["source"],
            sink=sink,
        )
    assert "does not exist in source" in str(exc_info.value)
    assert "event schema" in str(exc_info.value)


def test_topic_config_deduplication_id_field_validation():
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
                    models.SchemaField(name="name", type=models.KafkaDataType.STRING),
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


def test_validate_data_type_compatibility_invalid_mapping(valid_pipeline_config):
    """Test data type compatibility validation with invalid type mappings."""
    # Modify the sink configuration to have an invalid type mapping
    valid_pipeline_config["sink"]["table_mapping"][0]["column_type"] = (
        models.ClickhouseDataType.INT32
    )

    with pytest.raises(errors.InvalidDataTypeMappingError):
        models.PipelineConfig(**valid_pipeline_config)
