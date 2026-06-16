"""Tests for Kafka source formats (json/avro/protobuf) and the source registry.

The unified ``source_schema`` holds all schema config. It serializes json to a
top-level ``schema_fields`` (compatible with Open Source and Enterprise) and
avro/protobuf to the ``schema`` object (``avsc`` / ``proto``+``message``). On
reads the backend may also return ``schema.parsed_fields``.
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


AVSC = {
    "type": "record",
    "name": "Event",
    "namespace": "test",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "ts_ms", "type": "long"},
    ],
}
PROTO_TEXT = 'syntax = "proto3";\npackage test;\nmessage Event {\n  string id = 1;\n}'


class TestJsonFormat:
    def test_format_defaults_to_none_and_is_omitted(self):
        src = models.KafkaSource.model_validate(_kafka())
        assert src.format is None
        assert "format" not in src.model_dump(by_alias=True, exclude_none=True)

    def test_top_level_schema_fields_round_trips(self):
        wire = _kafka(
            format="json",
            schema_fields=[
                {"name": "id", "type": "string"},
                {"name": "ts_ms", "type": "int"},
            ],
        )
        src = models.KafkaSource.model_validate(wire)
        assert src.source_schema.fields[0].name == "id"
        assert src.schema_fields[0].name == "id"  # compat accessor
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        # json always serializes to top-level schema_fields (OSS + EE compat)
        assert dumped["schema_fields"] == wire["schema_fields"]
        assert "schema" not in dumped
        assert "source_schema" not in dumped

    def test_unified_schema_fields_input_also_accepted(self):
        # EE may return json fields under the unified schema object.
        src = models.KafkaSource.model_validate(
            _kafka(format="json", schema={"fields": [{"name": "id", "type": "string"}]})
        )
        assert src.schema_fields[0].name == "id"
        # still emits the compatible top-level form
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        assert dumped["schema_fields"] == [{"name": "id", "type": "string"}]


class TestAvroFormat:
    def test_avro_round_trips(self):
        src = models.KafkaSource.model_validate(
            _kafka(format="avro", schema={"avsc": AVSC})
        )
        assert src.format == models.KafkaFormat.AVRO
        assert src.source_schema.avsc.name == "Event"
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        assert dumped["schema"] == {"avsc": AVSC}
        assert "schema_fields" not in dumped

    def test_avro_requires_avsc(self):
        with pytest.raises(ValueError, match="avro format requires schema.avsc"):
            models.KafkaSource.model_validate(_kafka(format="avro"))

    def test_avsc_must_be_a_record_with_fields(self):
        with pytest.raises(ValueError):
            models.KafkaSource.model_validate(
                _kafka(format="avro", schema={"avsc": {"type": "string", "name": "x"}})
            )
        with pytest.raises(ValueError):
            models.KafkaSource.model_validate(
                _kafka(format="avro", schema={"avsc": {"type": "record", "name": "E"}})
            )

    def test_avsc_preserves_extra_keys(self):
        avsc = {
            "type": "record",
            "name": "Event",
            "namespace": "test",
            "doc": "an event",
            "fields": [{"name": "meta", "type": {"type": "map", "values": "string"}}],
        }
        src = models.KafkaSource.model_validate(
            _kafka(format="avro", schema={"avsc": avsc})
        )
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        assert dumped["schema"]["avsc"] == avsc


class TestProtobufFormat:
    def test_protobuf_round_trips(self):
        src = models.KafkaSource.model_validate(
            _kafka(format="protobuf", schema={"proto": PROTO_TEXT, "message": "Event"})
        )
        assert src.format == models.KafkaFormat.PROTOBUF
        assert src.source_schema.proto == PROTO_TEXT
        assert src.source_schema.message == "Event"
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        assert dumped["schema"] == {"proto": PROTO_TEXT, "message": "Event"}

    def test_protobuf_requires_proto_and_message(self):
        with pytest.raises(ValueError, match="schema.proto and schema.message"):
            models.KafkaSource.model_validate(
                _kafka(format="protobuf", schema={"proto": PROTO_TEXT})
            )


class TestParsedFields:
    def test_parsed_fields_read_only_not_surfaced_or_emitted(self):
        # Shape the backend returns for an avro source on GET.
        src = models.KafkaSource.model_validate(
            _kafka(
                format="avro",
                schema={
                    "avsc": AVSC,
                    "parsed_fields": [
                        {"name": "id", "type": "string"},
                        {"name": "ts_ms", "type": "int"},
                    ],
                },
            )
        )
        # Available as informational backend output...
        assert src.source_schema.parsed_fields[0].name == "id"
        # ...but not surfaced via the schema_fields compat accessor (json only),
        assert src.schema_fields is None
        # ...and not emitted back on dump.
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        assert dumped["schema"] == {"avsc": AVSC}
        assert "parsed_fields" not in dumped["schema"]

    def test_parsed_fields_on_json_get(self):
        # GET of a json source: schema_fields (or schema.fields) plus parsed_fields.
        src = models.KafkaSource.model_validate(
            _kafka(
                format="json",
                schema_fields=[{"name": "id", "type": "string"}],
                schema={"parsed_fields": [{"name": "id", "type": "string"}]},
            )
        )
        assert src.schema_fields[0].name == "id"
        assert src.source_schema.parsed_fields[0].name == "id"
        dumped = src.model_dump(by_alias=True, exclude_none=True)
        assert dumped["schema_fields"] == [{"name": "id", "type": "string"}]
        assert "schema" not in dumped  # parsed_fields not echoed back


class TestSchemaFormatConsistency:
    def test_avsc_without_avro_format_rejected(self):
        with pytest.raises(ValueError, match="require format 'avro' or 'protobuf'"):
            models.KafkaSource.model_validate(
                _kafka(format="json", schema={"avsc": AVSC})
            )


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
