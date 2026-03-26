"""Kafka source models."""

from typing import List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from ..base import CaseInsensitiveStrEnum
from ..data_types import KafkaDataType
from ..source import SourceBaseConfig, SourceBaseConfigPatch, SourceType
from ..transforms.deduplication import DeduplicationConfig  # noqa: F401 — re-exported


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
    """Schema registry configuration for a Kafka topic (V3)."""

    url: str
    api_key: str
    api_secret: str


class ConsumerGroupOffset(CaseInsensitiveStrEnum):
    LATEST = "latest"
    EARLIEST = "earliest"


class KafkaField(BaseModel):
    """A field in a Kafka topic schema."""

    name: str
    type: KafkaDataType


class TopicConfig(BaseModel):
    consumer_group_initial_offset: ConsumerGroupOffset = ConsumerGroupOffset.LATEST
    name: str
    # V3: each topic has an explicit id (referenced by downstream source_id fields)
    id: Optional[str] = Field(default=None)
    deduplication: Optional[DeduplicationConfig] = Field(default=DeduplicationConfig())
    replicas: Optional[int] = Field(
        default=1,
        deprecated="Use pipeline_resources.ingestor.<base|left|right>.replicas instead",
    )
    schema_registry: Optional[SchemaRegistry] = Field(default=None)
    schema_version: Optional[str] = Field(default=None)
    schema_fields: Optional[List[KafkaField]] = Field(default=None)

    @field_validator("replicas")
    @classmethod
    def validate_replicas(cls, v: int) -> int:
        if v < 1:
            raise ValueError("Replicas must be at least 1")
        return v

    @model_validator(mode="after")
    def validate_schema_registry_requires_version(self) -> "TopicConfig":
        """Validate that schema_version is set when schema_registry is provided."""
        if self.schema_registry is not None and self.schema_version is None:
            raise ValueError(
                "schema_version is required when schema_registry is provided"
            )
        return self

    @property
    def effective_id(self) -> str:
        """Return the topic id if explicitly set, otherwise fall back to name."""
        return self.id if self.id is not None else self.name


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
    def empty_str_to_none(values):
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
    """Kafka source configuration."""

    type: Literal[SourceType.KAFKA] = SourceType.KAFKA
    provider: Optional[str] = Field(default=None)
    connection_params: KafkaConnectionParams
    topics: List[TopicConfig]

    def update(self, patch: "KafkaSourcePatch") -> "KafkaSource":
        """Apply a patch to this source config."""
        update_dict = self.model_copy(deep=True)

        if patch.provider is not None:
            update_dict.provider = patch.provider

        if patch.connection_params is not None:
            update_dict.connection_params = self.connection_params.update(
                patch.connection_params
            )

        if patch.topics is not None:
            update_dict.topics = patch.topics

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

    provider: Optional[str] = Field(default=None)
    connection_params: Optional[KafkaConnectionParamsPatch] = Field(default=None)
    # Full replacement only; users must provide complete TopicConfig entries
    topics: Optional[List[TopicConfig]] = Field(default=None)
