"""Kafka source models."""

from typing import Any, List, Literal, Optional

from pydantic import BaseModel, Field, SerializeAsAny, field_validator, model_validator

from ..base import CaseInsensitiveStrEnum
from ..data_types import KafkaDataType
from ..registry import resolve_format
from ..source import SourceBaseConfig, SourceBaseConfigPatch, SourceType
from .formats import SourceFormat


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


class KafkaSource(SourceBaseConfig):
    """Kafka source configuration.

    In V3, each source is a flat object with its own connection_params
    and a single topic string.
    """

    type: Literal[SourceType.KAFKA] = SourceType.KAFKA
    connection_params: KafkaConnectionParams
    topic: str
    consumer_group_initial_offset: ConsumerGroupOffset = ConsumerGroupOffset.LATEST
    schema_registry: Optional[SchemaRegistry] = Field(default=None)
    schema_version: Optional[str] = Field(default=None)
    schema_fields: Optional[List[KafkaField]] = Field(default=None)
    # Payload format. ``None`` means JSON (default) and is omitted from the
    # serialized config. Concrete subclass is resolved from the format registry
    # by its ``type``; SerializeAsAny preserves subclass-only fields on dump.
    format: Optional[SerializeAsAny[SourceFormat]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def validate_empty_schema_registry(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if data.get("schema_registry", None) == {}:
                data.pop("schema_registry", None)
        return data

    @field_validator("format", mode="before")
    @classmethod
    def resolve_format_field(cls, value: Any) -> Any:
        """Dispatch a raw format dict to the concrete registered format class."""
        return resolve_format(value)

    @model_validator(mode="after")
    def validate_schema_registry_requires_version(self) -> "KafkaSource":
        """Validate that schema_version is set when schema_registry is provided."""
        if self.schema_registry is not None and self.schema_version is None:
            raise ValueError(
                "schema_version is required when schema_registry is provided"
            )
        return self

    @model_validator(mode="after")
    def validate_format(self) -> "KafkaSource":
        """Let the format enforce schema-source rules against this source's
        schema registry configuration."""
        if self.format is not None:
            self.format.validate_against_registry(self.schema_registry is not None)
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

        if patch.schema_fields is not None:
            update_dict.schema_fields = patch.schema_fields

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

    connection_params: Optional[KafkaConnectionParamsPatch] = Field(default=None)
    topic: Optional[str] = Field(default=None)
    schema_fields: Optional[List[KafkaField]] = Field(default=None)
