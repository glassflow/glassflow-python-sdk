"""Kafka source models."""

from typing import Any, Dict, List, Literal, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_serializer,
    model_validator,
)

from ..base import CaseInsensitiveStrEnum
from ..data_types import KafkaDataType
from ..source import SourceBaseConfig, SourceBaseConfigPatch, SourceType


class KafkaFormat(CaseInsensitiveStrEnum):
    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"


class KafkaProtocol(CaseInsensitiveStrEnum):
    SSL = "SSL"
    SASL_SSL = "SASL_SSL"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    PLAINTEXT = "PLAINTEXT"


class KafkaMechanism(CaseInsensitiveStrEnum):
    SCRAM_SHA_256 = "SCRAM-SHA-256"
    SCRAM_SHA_512 = "SCRAM-SHA-512"
    PLAIN = "PLAIN"
    GSSAPI = "GSSAPI"
    NO_AUTH = "NO_AUTH"


class SchemaRegistry(BaseModel):
    """Schema registry configuration for a Kafka source."""

    url: str
    api_key: str
    api_secret: str


class ConsumerGroupOffset(CaseInsensitiveStrEnum):
    LATEST = "latest"
    EARLIEST = "earliest"


class KafkaField(BaseModel):
    """A field in a Kafka source schema."""

    name: str
    type: KafkaDataType


class AvroSchema(BaseModel):
    """The Avro schema record (``schema.avsc``) for an Avro Kafka source.

    GlassFlow maps the record's root-level fields to ClickHouse columns, so the
    top-level schema must be an Avro ``record`` with a ``name`` and a non-empty
    ``fields`` list. Individual field ``type`` values may themselves be nested
    Avro schemas. Other Avro keys (``namespace``, ``doc``, ``aliases``, ...) are
    accepted and preserved on round-trip.
    """

    model_config = ConfigDict(extra="allow")

    type: Literal["record"]
    name: str
    fields: List[Dict[str, Any]] = Field(min_length=1)
    namespace: Optional[str] = Field(default=None)


class KafkaSchema(BaseModel):
    """Unified schema for a Kafka source. The shape used is selected by the
    source's :class:`KafkaFormat`:

    - ``json``     -> ``fields`` (GlassFlow field declarations)
    - ``avro``     -> ``avsc`` (the Avro schema record)
    - ``protobuf`` -> ``proto`` (the ``.proto`` text) and ``message`` (the
      message name within it)

    On reads the backend also returns ``parsed_fields``: the field list parsed
    from the avsc/proto. It is read-only and is not sent back on create/edit.
    """

    fields: Optional[List[KafkaField]] = Field(default=None)
    avsc: Optional[AvroSchema] = Field(default=None)
    proto: Optional[str] = Field(default=None)
    message: Optional[str] = Field(default=None)
    parsed_fields: Optional[List[KafkaField]] = Field(default=None)


class KafkaConnectionParams(BaseModel):
    brokers: List[str]
    protocol: KafkaProtocol
    mechanism: Optional[KafkaMechanism] = Field(default=None)
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    root_ca: Optional[str] = Field(default=None)
    kerberos_service_name: Optional[str] = Field(default=None)
    kerberos_keytab: Optional[str] = Field(default=None)
    kerberos_realm: Optional[str] = Field(default=None)
    kerberos_config: Optional[str] = Field(default=None)
    skip_tls_verification: bool = Field(default=False)

    @model_validator(mode="before")
    @classmethod
    def empty_str_to_none(cls, values):
        if values.get("mechanism", None) == "":
            values["mechanism"] = None
        return values

    def update(self, patch: "KafkaConnectionParamsPatch") -> "KafkaConnectionParams":
        """Apply a patch to this connection params config."""
        current_dict = self.model_dump()
        patch_dict = patch.model_dump(exclude_none=True)
        merged_dict = {**current_dict, **patch_dict}
        return KafkaConnectionParams.model_validate(merged_dict)


def _parse_source_schema(data: Any) -> Any:
    """Fold both wire schema shapes into the unified ``source_schema``:

    - top-level ``schema_fields`` (Open Source, and Enterprise for compatibility)
    - the unified ``schema`` object (Enterprise: avsc / proto+message /
      parsed_fields, and optionally fields)

    Also drops an empty ``schema_registry``. Left untouched if ``source_schema``
    is already supplied directly.
    """
    if not isinstance(data, dict) or "source_schema" in data:
        return data
    data = dict(data)
    if data.get("schema_registry", None) == {}:
        data.pop("schema_registry", None)

    schema: Dict[str, Any] = {}
    if "schema_fields" in data:
        schema["fields"] = data.pop("schema_fields")
    if "schema" in data:
        wire = data.pop("schema")
        if isinstance(wire, dict):
            schema.update(wire)
        else:
            data["schema"] = wire  # let validation reject a non-object schema
    if schema:
        data["source_schema"] = schema
    return data


class KafkaSource(SourceBaseConfig):
    """Kafka source configuration.

    In V3, each source is a flat object with its own connection_params
    and a single topic string.
    """

    model_config = ConfigDict(populate_by_name=True)

    type: Literal[SourceType.KAFKA] = SourceType.KAFKA
    connection_params: KafkaConnectionParams
    topic: str
    consumer_group_initial_offset: ConsumerGroupOffset = ConsumerGroupOffset.LATEST
    schema_registry: Optional[SchemaRegistry] = Field(default=None)
    schema_version: Optional[str] = Field(default=None)
    # Payload wire format. ``None`` means JSON (the backend default) and is
    # omitted from the serialized config. ``avro`` and ``protobuf`` are
    # Enterprise features and are rejected by an unlicensed backend.
    format: Optional[KafkaFormat] = Field(default=None)
    # All schema-related config in one place. Serialized back to the wire as
    # top-level ``schema_fields`` (json, for Open Source / Enterprise compat) or
    # the ``schema`` object (avro/protobuf); see the serializer below.
    source_schema: Optional[KafkaSchema] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def parse_schema(cls, data: Any) -> Any:
        return _parse_source_schema(data)

    @model_serializer(mode="wrap")
    def serialize_schema(self, handler: Any) -> Any:
        """Emit json fields as top-level ``schema_fields`` (compatible with both
        editions) and avro/protobuf as the ``schema`` object. ``parsed_fields``
        is read-only and is not emitted."""
        data = handler(self)
        schema = data.pop("source_schema", None)
        if schema:
            if schema.get("fields") is not None:
                data["schema_fields"] = schema["fields"]
            inner = {
                k: schema[k]
                for k in ("avsc", "proto", "message")
                if schema.get(k) is not None
            }
            if inner:
                data["schema"] = inner
        return data

    @property
    def schema_fields(self) -> Optional[List[KafkaField]]:
        """Backward-compatible accessor for the JSON field declarations, held
        under ``schema.fields``. (``schema.parsed_fields`` is read-only backend
        info and is intentionally not surfaced here.)"""
        return self.source_schema.fields if self.source_schema else None

    @model_validator(mode="after")
    def validate_schema_registry_requires_version(self) -> "KafkaSource":
        """Validate that schema_version is set when schema_registry is provided."""
        if self.schema_registry is not None and self.schema_version is None:
            raise ValueError(
                "schema_version is required when schema_registry is provided"
            )
        return self

    @model_validator(mode="after")
    def validate_format_schema(self) -> "KafkaSource":
        """The schema shape must match the declared format."""
        schema = self.source_schema
        if self.format == KafkaFormat.AVRO:
            if schema is None or schema.avsc is None:
                raise ValueError("avro format requires schema.avsc")
        elif self.format == KafkaFormat.PROTOBUF:
            if schema is None or not schema.proto or not schema.message:
                raise ValueError(
                    "protobuf format requires schema.proto and schema.message"
                )
        elif schema is not None and (
            schema.avsc is not None or schema.proto or schema.message
        ):
            raise ValueError(
                "schema.avsc / proto / message require format 'avro' or 'protobuf'"
            )
        return self

    def update(self, patch: "KafkaSourcePatch") -> "KafkaSource":
        """Apply a patch to this source config."""
        update_dict = self.model_copy(deep=True)

        if patch.connection_params is not None:
            update_dict.connection_params = self.connection_params.update(
                patch.connection_params
            )

        if patch.topic is not None:
            update_dict.topic = patch.topic

        if patch.format is not None:
            update_dict.format = patch.format

        if patch.source_schema is not None:
            update_dict.source_schema = patch.source_schema

        return update_dict


# Patch models


class KafkaConnectionParamsPatch(BaseModel):
    brokers: Optional[List[str]] = Field(default=None)
    protocol: Optional[KafkaProtocol] = Field(default=None)
    mechanism: Optional[KafkaMechanism] = Field(default=None)
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    root_ca: Optional[str] = Field(default=None)
    kerberos_service_name: Optional[str] = Field(default=None)
    kerberos_keytab: Optional[str] = Field(default=None)
    kerberos_realm: Optional[str] = Field(default=None)
    kerberos_config: Optional[str] = Field(default=None)
    skip_tls_verification: Optional[bool] = Field(default=None)


class KafkaSourcePatch(SourceBaseConfigPatch):
    """Patch model for KafkaSource."""

    model_config = ConfigDict(populate_by_name=True)

    connection_params: Optional[KafkaConnectionParamsPatch] = Field(default=None)
    topic: Optional[str] = Field(default=None)
    format: Optional[KafkaFormat] = Field(default=None)
    source_schema: Optional[KafkaSchema] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def parse_schema(cls, data: Any) -> Any:
        return _parse_source_schema(data)

    @property
    def schema_fields(self) -> Optional[List[KafkaField]]:
        """Backward-compatible accessor for ``schema.fields``."""
        return self.source_schema.fields if self.source_schema else None
