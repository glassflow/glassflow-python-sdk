"""Tests for source formats and the source/format registries.

These prove the open-ended extension mechanism: an out-of-tree format or source
type (standing in for what the Enterprise SDK ships) plugs in via the registry
with no change to the OSS models, and SerializeAsAny preserves its fields on
dump.
"""

from typing import Literal, Optional

import pytest
from pydantic import BaseModel, model_validator

from glassflow.etl import models
from glassflow.etl.models import registry
from glassflow.etl.models.base import CaseInsensitiveStrEnum
from glassflow.etl.models.sources.formats import SourceFormat

# --- Out-of-tree EE-like definitions (not part of OSS) ----------------------


class SchemaSource(CaseInsensitiveStrEnum):
    INLINE = "inline"
    REGISTRY = "registry"


class ProtobufConfig(BaseModel):
    schema_source: SchemaSource
    proto_text: Optional[str] = None

    @model_validator(mode="after")
    def _require_inline_text(self):
        if self.schema_source == SchemaSource.INLINE and not self.proto_text:
            raise ValueError("proto_text is required when schema_source is 'inline'")
        return self


class ProtobufFormat(SourceFormat):
    type: Literal["protobuf"] = "protobuf"
    protobuf: ProtobufConfig

    def validate_against_registry(self, has_schema_registry: bool) -> None:
        if self.protobuf.schema_source == SchemaSource.REGISTRY and not (
            has_schema_registry
        ):
            raise ValueError(
                "protobuf with schema_source 'registry' requires a schema registry "
                "on the source"
            )


class KinesisSource(models.SourceBaseConfig):
    type: Literal["kinesis"] = "kinesis"
    stream_name: str
    region: str


@pytest.fixture
def register_ee_types():
    """Register the out-of-tree types and clean up afterward."""
    registry.register_format(ProtobufFormat)
    registry.register_source(KinesisSource)
    yield
    registry._FORMAT_CLASSES.pop("protobuf", None)
    registry._SOURCE_CLASSES.pop("kinesis", None)


# The exact payload from the ticket discussion.
PROTOBUF_SOURCE = {
    "type": "kafka",
    "source_id": "events",
    "connection_params": {
        "brokers": ["kafka-controller-0.staging-cluster.glassflow.xyz:9094"],
        "mechanism": "PLAIN",
        "protocol": "SASL_PLAINTEXT",
        "username": "glassflow",
        "password": "secret",
    },
    "topic": "test_proto_events_dedup",
    "format": {
        "type": "protobuf",
        "protobuf": {
            "schema_source": "inline",
            "proto_text": 'syntax = "proto3";\nmessage Event {\n  string id = 1;\n}',
        },
    },
    "consumer_group_initial_offset": "earliest",
}


class TestJsonFormat:
    """OSS default format behaviour."""

    def test_format_defaults_to_none(self):
        src = models.KafkaSource.model_validate(
            {
                "type": "kafka",
                "source_id": "s1",
                "connection_params": {"brokers": ["b:9092"], "protocol": "PLAINTEXT"},
                "topic": "t",
            }
        )
        assert src.format is None
        # Omitted from the serialized config so OSS payloads are unchanged.
        assert "format" not in src.model_dump(by_alias=True, exclude_none=True)

    def test_explicit_json_format_roundtrips(self):
        src = models.KafkaSource.model_validate(
            {
                "type": "kafka",
                "source_id": "s1",
                "connection_params": {"brokers": ["b:9092"], "protocol": "PLAINTEXT"},
                "topic": "t",
                "format": {"type": "json"},
            }
        )
        assert isinstance(src.format, models.JsonFormat)
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        assert dumped["format"] == {"type": "json"}

    def test_unknown_format_raises(self):
        with pytest.raises(ValueError, match="Unknown format type 'avro'"):
            models.KafkaSource.model_validate(
                {
                    "type": "kafka",
                    "source_id": "s1",
                    "connection_params": {
                        "brokers": ["b:9092"],
                        "protocol": "PLAINTEXT",
                    },
                    "topic": "t",
                    "format": {"type": "avro"},
                }
            )


class TestRegisteredFormat:
    """An out-of-tree format plugs in via the registry."""

    def test_protobuf_payload_roundtrips(self, register_ee_types):
        src = models.KafkaSource.model_validate(PROTOBUF_SOURCE)

        assert isinstance(src.format, ProtobufFormat)
        assert src.format.protobuf.schema_source == SchemaSource.INLINE

        # SerializeAsAny preserves the subclass-only nested config on dump,
        # round-tripping to the exact wire shape.
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        assert dumped["format"] == PROTOBUF_SOURCE["format"]

    def test_protobuf_inline_requires_text(self, register_ee_types):
        bad = {**PROTOBUF_SOURCE}
        bad["format"] = {"type": "protobuf", "protobuf": {"schema_source": "inline"}}
        with pytest.raises(ValueError, match="proto_text is required"):
            models.KafkaSource.model_validate(bad)

    def test_protobuf_registry_source_requires_schema_registry(self, register_ee_types):
        # schema_source 'registry' but no schema_registry on the source -> hook fires.
        bad = {**PROTOBUF_SOURCE}
        bad["format"] = {"type": "protobuf", "protobuf": {"schema_source": "registry"}}
        with pytest.raises(ValueError, match="requires a schema registry"):
            models.KafkaSource.model_validate(bad)


class TestRegisteredSourceType:
    """An out-of-tree source type plugs in via the registry."""

    def _config(self, source: dict) -> dict:
        return {
            "pipeline_id": "p1",
            "sources": [source],
            "sink": {
                "type": "clickhouse",
                "connection_params": {
                    "host": "h",
                    "port": "9000",
                    "database": "db",
                    "username": "u",
                    "password": "p",
                    "secure": True,
                },
                "table": "t",
                "max_batch_size": 1,
                "mapping": [
                    {"name": "id", "column_name": "id", "column_type": "String"}
                ],
            },
        }

    def test_kinesis_source_dispatches_and_roundtrips(self, register_ee_types):
        kinesis = {
            "type": "kinesis",
            "source_id": "k1",
            "stream_name": "events",
            "region": "eu-central-1",
        }
        cfg = models.PipelineConfig.model_validate(self._config(kinesis))

        assert isinstance(cfg.sources[0], KinesisSource)
        # SerializeAsAny keeps kinesis-only fields through the base-typed field.
        dumped = cfg.model_dump(by_alias=True, exclude_none=True)
        assert dumped["sources"][0]["stream_name"] == "events"
        assert dumped["sources"][0]["region"] == "eu-central-1"

    def test_unknown_source_type_raises(self):
        with pytest.raises(ValueError, match="Unknown source type 'pubsub'"):
            models.PipelineConfig.model_validate(
                self._config({"type": "pubsub", "source_id": "x"})
            )
