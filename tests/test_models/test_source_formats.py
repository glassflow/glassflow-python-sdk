"""Tests for Kafka source formats (json/avro/protobuf) and the source registry.

The unified ``source_schema`` (wire key ``schema``) holds all schema config:
``fields`` (json), ``file`` (the inline avsc/proto text), ``message_type``
(protobuf), and read-only ``parsed_fields`` (returned on GET). The legacy
top-level ``schema_fields`` is still accepted on input and upgraded to
``schema.fields``. The source registry lets an out-of-tree source type plug in
without changing OSS models.
"""

from typing import Literal
from unittest.mock import patch

import pytest

from glassflow.etl import errors, models
from glassflow.etl.models import registry
from tests.data import mock_responses


def _kafka(**overrides) -> dict:
    base = {
        "type": "kafka",
        "source_id": "events",
        "connection_params": {"brokers": ["b:9092"], "protocol": "PLAINTEXT"},
        "topic": "events",
        "consumer_group_initial_offset": "earliest",
    }
    base.update(overrides)
    return base


AVSC_TEXT = (
    '{"type": "record", "name": "Event", "fields": [{"name": "id", "type": "string"}]}'
)
PROTO_TEXT = 'syntax = "proto3";\npackage test;\nmessage Event {\n  string id = 1;\n}'


class TestJsonFormat:
    def test_format_defaults_to_none_and_is_omitted(self):
        src = models.KafkaSource.model_validate(_kafka())
        assert src.format is None
        assert "format" not in src.model_dump(by_alias=True, exclude_none=True)

    def test_legacy_schema_fields_accepted_and_upgraded(self):
        # The deprecated top-level schema_fields is folded into schema.fields.
        wire = _kafka(format="json", schema_fields=[{"name": "id", "type": "string"}])
        src = models.KafkaSource.model_validate(wire)
        assert src.source_schema.fields[0].name == "id"
        assert src.schema_fields[0].name == "id"  # compat accessor
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        assert dumped["schema"] == {"fields": [{"name": "id", "type": "string"}]}
        assert "schema_fields" not in dumped  # upgraded to the unified schema

    def test_schema_fields_round_trips(self):
        wire = _kafka(
            format="json", schema={"fields": [{"name": "id", "type": "string"}]}
        )
        src = models.KafkaSource.model_validate(wire)
        assert src.schema_fields[0].name == "id"
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        assert dumped["schema"] == {"fields": [{"name": "id", "type": "string"}]}


class TestAvroFormat:
    def test_avro_round_trips(self):
        src = models.KafkaSource.model_validate(
            _kafka(format="avro", schema={"file": AVSC_TEXT})
        )
        assert src.format == models.KafkaFormat.AVRO
        assert src.source_schema.file == AVSC_TEXT
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        assert dumped["schema"] == {"file": AVSC_TEXT}

    def test_avro_requires_file(self):
        with pytest.raises(ValueError, match="avro format requires schema.file"):
            models.KafkaSource.model_validate(_kafka(format="avro"))


class TestProtobufFormat:
    def test_protobuf_round_trips(self):
        src = models.KafkaSource.model_validate(
            _kafka(
                format="protobuf",
                schema={"file": PROTO_TEXT, "message_type": "Event"},
            )
        )
        assert src.format == models.KafkaFormat.PROTOBUF
        assert src.source_schema.file == PROTO_TEXT
        assert src.source_schema.message_type == "Event"
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        assert dumped["schema"] == {"file": PROTO_TEXT, "message_type": "Event"}

    def test_protobuf_requires_file_and_message_type(self):
        with pytest.raises(ValueError, match="schema.file and schema.message_type"):
            models.KafkaSource.model_validate(
                _kafka(format="protobuf", schema={"file": PROTO_TEXT})
            )


class TestParsedFields:
    def test_parsed_fields_read_only(self):
        # Shape the backend returns on GET for an avro source.
        src = models.KafkaSource.model_validate(
            _kafka(
                format="avro",
                schema={
                    "file": AVSC_TEXT,
                    "parsed_fields": [{"name": "id", "type": "string"}],
                },
            )
        )
        # Available as informational backend output...
        assert src.source_schema.parsed_fields[0].name == "id"
        # ...but not surfaced via the schema_fields compat accessor (json only),
        assert src.schema_fields is None
        # ...and not emitted back on dump.
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        assert dumped["schema"] == {"file": AVSC_TEXT}
        assert "parsed_fields" not in dumped["schema"]


class TestSchemaRegistryRelaxation:
    """With a schema_registry configured the backend resolves the schema, so
    schema.file (and message_type) are optional. The backend omits them on GET
    for schema versions resolved at runtime, so requiring them would reject an
    otherwise-valid response."""

    _SR = {
        "url": "https://schema-registry.example.com",
        "api_key": "key",
        "api_secret": "secret",
    }

    def test_registry_avro_without_file_ok(self):
        # The shape the backend returns on GET after a runtime schema-version
        # resolution: format=avro with an empty schema block.
        src = models.KafkaSource.model_validate(
            _kafka(format="avro", schema_registry=self._SR, schema_version="1")
        )
        assert src.format == models.KafkaFormat.AVRO
        assert src.schema_registry is not None

    def test_registry_protobuf_without_file_ok(self):
        # GET shape after evolution: file dropped, only message_type survives.
        src = models.KafkaSource.model_validate(
            _kafka(
                format="protobuf",
                schema_registry=self._SR,
                schema_version="1",
                schema={"message_type": "Event"},
            )
        )
        assert src.format == models.KafkaFormat.PROTOBUF


class TestSchemaErrorSurfacing:
    """A backend 422 puts the specific cause in details.error; the SDK surfaces
    it on the create/edit path instead of the generic message."""

    def test_invalid_schema_surfaces_backend_detail(self, pipeline, mock_track):
        resp = mock_responses.create_mock_response_factory()(
            status_code=422,
            json_data={
                "status": 422,
                "code": "unprocessable_entity",
                "message": "failed to convert request to pipeline model",
                "details": {"error": 'source "events": proto compilation error: boom'},
            },
        )
        with patch(
            "httpx.Client.request", side_effect=resp.raise_for_status.side_effect
        ):
            with pytest.raises(errors.PipelineInvalidConfigurationError) as exc:
                pipeline.create()

        assert "proto compilation error: boom" in str(exc.value)
        assert exc.value.details["error"].startswith('source "events"')


# --- Source registry: an out-of-tree source type plugs in -------------------


class KinesisSource(models.SourceBaseConfig):
    type: Literal["kinesis"] = "kinesis"
    stream_name: str
    region: str


@pytest.fixture
def register_kinesis():
    registry.register_source(KinesisSource)
    yield
    registry._SOURCE_CLASSES.pop("kinesis", None)


class TestRegisteredSourceType:
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

    def test_kinesis_dispatches_and_roundtrips(self, register_kinesis):
        kinesis = {
            "type": "kinesis",
            "source_id": "k1",
            "stream_name": "events",
            "region": "eu-central-1",
        }
        cfg = models.PipelineConfig.model_validate(self._config(kinesis))
        assert isinstance(cfg.sources[0], KinesisSource)
        dumped = cfg.model_dump(by_alias=True, exclude_none=True)
        assert dumped["sources"][0]["stream_name"] == "events"

    def test_unknown_source_type_raises(self):
        with pytest.raises(ValueError, match="Unknown source type 'pubsub'"):
            models.PipelineConfig.model_validate(
                self._config({"type": "pubsub", "source_id": "x"})
            )
