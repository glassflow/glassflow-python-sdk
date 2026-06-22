"""Kafka source models."""

from typing import Any, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

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


class KafkaSchema(BaseModel):
    """Unified schema for a Kafka source. The shape used is selected by the
    source's :class:`KafkaFormat`:

    - ``json``     -> ``fields`` (GlassFlow field declarations)
    - ``avro``     -> ``file`` (the inline ``.avsc`` schema text)
    - ``protobuf`` -> ``file`` (the inline ``.proto`` text) and ``message_type``
      (the message within it to decode)

    On reads the backend also returns ``parsed_fields``: the field list parsed
    from the avsc/proto. It is read-only, exposed for inspection, and never sent
    back on create/edit.
    """

    fields: Optional[List[KafkaField]] = Field(default=None)
    file: Optional[str] = Field(default=None)
    message_type: Optional[str] = Field(default=None)
    parsed_fields: Optional[List[KafkaField]] = Field(default=None, exclude=True)


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
    """Accept the legacy top-level ``schema_fields`` (deprecated) by folding it
    into the unified ``schema`` object's ``fields``. The ``schema`` object itself
    maps directly to ``source_schema`` via its alias. Also drops an empty
    ``schema_registry``. Left untouched if ``source_schema`` is supplied by name.
    """
    if not isinstance(data, dict) or "source_schema" in data:
        return data
    data = dict(data)
    if data.get("schema_registry", None) == {}:
        data.pop("schema_registry", None)
    if "schema_fields" in data:
        fields = data.pop("schema_fields")
        schema = data.get("schema")
        schema = dict(schema) if isinstance(schema, dict) else {}
        schema.setdefault("fields", fields)
        data["schema"] = schema
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
    # All schema-related config in one place, serialized as the ``schema``
    # object (``fields`` for json, ``file`` [+ ``message_type``] for
    # avro/protobuf). The legacy top-level ``schema_fields`` is still accepted on
    # input (see _parse_source_schema).
    source_schema: Optional[KafkaSchema] = Field(default=None, alias="schema")

    @model_validator(mode="before")
    @classmethod
    def parse_schema(cls, data: Any) -> Any:
        return _parse_source_schema(data)

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
        """The schema shape must match the declared format.

        When a ``schema_registry`` is configured the schema is resolved from the
        registry, so ``schema.file`` (and ``message_type``) are optional: the
        backend omits them on reads for schema versions resolved at runtime, and
        requiring them would reject an otherwise-valid GET response.
        """
        schema = self.source_schema
        using_registry = self.schema_registry is not None
        if self.format == KafkaFormat.AVRO:
            if not using_registry and (schema is None or not schema.file):
                raise ValueError("avro format requires schema.file")
        elif self.format == KafkaFormat.PROTOBUF:
            if not using_registry and (
                schema is None or not schema.file or not schema.message_type
            ):
                raise ValueError(
                    "protobuf format requires schema.file and schema.message_type"
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
    source_schema: Optional[KafkaSchema] = Field(default=None, alias="schema")

    @model_validator(mode="before")
    @classmethod
    def parse_schema(cls, data: Any) -> Any:
        return _parse_source_schema(data)

    @property
    def schema_fields(self) -> Optional[List[KafkaField]]:
        """Backward-compatible accessor for ``schema.fields``."""
        return self.source_schema.fields if self.source_schema else None
