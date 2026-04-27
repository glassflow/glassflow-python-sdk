import pytest

from glassflow.etl import models
from glassflow.etl.models.sources import KafkaSource, SchemaRegistry


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


class TestKafkaSourceExtended:
    """Tests for KafkaSource fields (source_id, schema_registry, schema_fields)."""

    def test_kafka_source_with_source_id(self):
        src = KafkaSource(
            source_id="src-events",
            connection_params={
                "brokers": ["localhost:9092"],
                "protocol": "PLAINTEXT",
            },
            topic="user_events",
        )
        assert src.source_id == "src-events"
        assert src.topic == "user_events"

    def test_kafka_source_with_schema_registry(self):
        src = KafkaSource(
            source_id="events",
            connection_params={
                "brokers": ["localhost:9092"],
                "protocol": "PLAINTEXT",
            },
            topic="events",
            schema_registry=SchemaRegistry(
                url="https://sr.example.com",
                api_key="k",
                api_secret="s",
            ),
            schema_version="1",
        )
        assert src.schema_registry is not None
        assert src.schema_registry.url == "https://sr.example.com"

    def test_kafka_source_schema_registry_requires_version(self):
        """schema_version is required when schema_registry is provided."""
        with pytest.raises(ValueError, match="schema_version is required"):
            KafkaSource(
                source_id="events",
                connection_params={
                    "brokers": ["localhost:9092"],
                    "protocol": "PLAINTEXT",
                },
                topic="events",
                schema_registry=SchemaRegistry(
                    url="https://sr.example.com",
                    api_key="k",
                    api_secret="s",
                ),
            )

    def test_kafka_source_with_schema_version(self):
        src = KafkaSource(
            source_id="events",
            connection_params={
                "brokers": ["localhost:9092"],
                "protocol": "PLAINTEXT",
            },
            topic="events",
            schema_version="2",
        )
        assert src.schema_version == "2"

    def test_kafka_source_schema_registry_and_version_optional(self):
        src = KafkaSource(
            source_id="events",
            connection_params={
                "brokers": ["localhost:9092"],
                "protocol": "PLAINTEXT",
            },
            topic="events",
        )
        assert src.schema_registry is None
        assert src.schema_version is None

    def test_kafka_source_from_dict(self):
        data = {
            "type": "kafka",
            "source_id": "ev-src",
            "connection_params": {
                "brokers": ["localhost:9092"],
                "protocol": "PLAINTEXT",
            },
            "topic": "events",
            "schema_registry": {
                "url": "https://sr.example.com",
                "api_key": "key",
                "api_secret": "secret",
            },
            "schema_version": "3",
        }
        src = KafkaSource.model_validate(data)
        assert src.source_id == "ev-src"
        assert src.schema_registry.api_key == "key"
        assert src.schema_version == "3"

    def test_kafka_source_with_schema_fields(self):
        src = KafkaSource(
            source_id="events",
            connection_params={
                "brokers": ["localhost:9092"],
                "protocol": "PLAINTEXT",
            },
            topic="events",
            schema_fields=[
                models.KafkaField(name="id", type=models.KafkaDataType.STRING),
                models.KafkaField(name="value", type=models.KafkaDataType.INT64),
            ],
        )
        assert src.schema_fields is not None
        assert len(src.schema_fields) == 2
        assert src.schema_fields[0].name == "id"
        assert src.schema_fields[0].type == models.KafkaDataType.STRING

    def test_kafka_source_schema_fields_optional(self):
        src = KafkaSource(
            source_id="events",
            connection_params={
                "brokers": ["localhost:9092"],
                "protocol": "PLAINTEXT",
            },
            topic="events",
        )
        assert src.schema_fields is None

    def test_kafka_source_schema_fields_from_dict(self):
        data = {
            "type": "kafka",
            "source_id": "events",
            "connection_params": {
                "brokers": ["localhost:9092"],
                "protocol": "PLAINTEXT",
            },
            "topic": "events",
            "schema_fields": [
                {"name": "session_id", "type": "string"},
                {"name": "user_id", "type": "string"},
            ],
        }
        src = KafkaSource.model_validate(data)
        assert src.schema_fields is not None
        assert len(src.schema_fields) == 2
        assert src.schema_fields[0].name == "session_id"


class TestPipelineConfigDeduplicationValidation:
    """Tests for PipelineConfig dedup key validation against source schema_fields."""

    def test_dedup_key_validation_valid_with_fields(self):
        """Dedup key is valid when present in schema_fields."""
        config = models.PipelineConfig(
            pipeline_id="test-pipeline",
            sources=[
                models.KafkaSource(
                    source_id="test-src",
                    connection_params=models.KafkaConnectionParams(
                        brokers=["localhost:9092"],
                        protocol=models.KafkaProtocol.PLAINTEXT,
                    ),
                    topic="test-topic",
                    schema_fields=[
                        models.KafkaField(name="id", type=models.KafkaDataType.STRING),
                    ],
                ),
            ],
            transforms=[
                models.DedupTransform(
                    source_id="test-src",
                    config=models.DedupTransformConfig(key="id", time_window="1h"),
                ),
            ],
            sink=models.SinkConfig(
                connection_params=models.ClickhouseConnectionParams(
                    host="localhost",
                    port="9000",
                    database="test",
                    username="default",
                    password="",
                ),
                table="test_table",
            ),
        )
        assert config.pipeline_id == "test-pipeline"

    def test_dedup_key_validation_valid_without_fields(self):
        """Dedup key validation is skipped when source has no schema_fields."""
        config = models.PipelineConfig(
            pipeline_id="test-pipeline",
            sources=[
                models.KafkaSource(
                    source_id="test-src",
                    connection_params=models.KafkaConnectionParams(
                        brokers=["localhost:9092"],
                        protocol=models.KafkaProtocol.PLAINTEXT,
                    ),
                    topic="test-topic",
                ),
            ],
            transforms=[
                models.DedupTransform(
                    source_id="test-src",
                    config=models.DedupTransformConfig(
                        key="non-existent-field", time_window="1h"
                    ),
                ),
            ],
            sink=models.SinkConfig(
                connection_params=models.ClickhouseConnectionParams(
                    host="localhost",
                    port="9000",
                    database="test",
                    username="default",
                    password="",
                ),
                table="test_table",
            ),
        )
        assert config.pipeline_id == "test-pipeline"

    def test_dedup_key_validation_invalid(self):
        """Dedup key not found in schema_fields raises error."""
        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="test-pipeline",
                sources=[
                    models.KafkaSource(
                        source_id="test-src",
                        connection_params=models.KafkaConnectionParams(
                            brokers=["localhost:9092"],
                            protocol=models.KafkaProtocol.PLAINTEXT,
                        ),
                        topic="test-topic",
                        schema_fields=[
                            models.KafkaField(
                                name="name", type=models.KafkaDataType.STRING
                            ),
                        ],
                    ),
                ],
                transforms=[
                    models.DedupTransform(
                        source_id="test-src",
                        config=models.DedupTransformConfig(
                            key="non-existent-field", time_window="1h"
                        ),
                    ),
                ],
                sink=models.SinkConfig(
                    connection_params=models.ClickhouseConnectionParams(
                        host="localhost",
                        port="9000",
                        database="test",
                        username="default",
                        password="",
                    ),
                    table="test_table",
                ),
            )
        assert "not found" in str(exc_info.value)
