from typing import List, Optional

from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator

from .base import CaseInsensitiveStrEnum
from .data_types import KafkaDataType


class KafkaProtocol(CaseInsensitiveStrEnum):
    SSL = "SSL"
    SASL_SSL = "SASL_SSL"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    PLAINTEXT = "PLAINTEXT"


class KafkaMechanism(CaseInsensitiveStrEnum):
    SCRAM_SHA_256 = "SCRAM-SHA-256"
    SCRAM_SHA_512 = "SCRAM-SHA-512"
    PLAIN = "PLAIN"


class SchemaField(BaseModel):
    name: str
    type: KafkaDataType


class SchemaType(CaseInsensitiveStrEnum):
    JSON = "json"


class Schema(BaseModel):
    type: SchemaType = SchemaType.JSON
    fields: List[SchemaField]


class DeduplicationConfig(BaseModel):
    enabled: bool = False
    id_field: Optional[str] = Field(default=None)
    id_field_type: Optional[KafkaDataType] = Field(default=None)
    time_window: Optional[str] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def validate_deduplication_fields(cls, values):
        """Validate deduplication fields based on enabled status."""
        if isinstance(values, dict):
            enabled = values.get("enabled", False)

            # If deduplication is disabled, allow empty strings
            if not enabled:
                # Convert empty strings to None for enum fields
                if values.get("id_field_type") == "":
                    values["id_field_type"] = None
                if values.get("id_field") == "":
                    values["id_field"] = None
                if values.get("time_window") == "":
                    values["time_window"] = None
            else:
                # If enabled, ensure required fields are present and not empty
                for field_name in ["id_field", "id_field_type", "time_window"]:
                    field_value = values.get(field_name)
                    if field_value is None or field_value == "":
                        raise ValueError(
                            f"{field_name} is required when deduplication is enabled"
                        )

                # Validate id_field_type is a valid type when enabled
                id_field_type = values.get("id_field_type")
                if id_field_type not in [
                    KafkaDataType.STRING,
                    KafkaDataType.INT32,
                    KafkaDataType.INT64,
                ]:
                    raise ValueError(
                        "id_field_type must be a string, int32, or int64 when "
                        "deduplication is enabled"
                    )

        return values


class ConsumerGroupOffset(CaseInsensitiveStrEnum):
    LATEST = "latest"
    EARLIEST = "earliest"


class TopicConfig(BaseModel):
    consumer_group_initial_offset: ConsumerGroupOffset = ConsumerGroupOffset.EARLIEST
    name: str
    event_schema: Schema = Field(alias="schema")
    deduplication: Optional[DeduplicationConfig] = Field(default=DeduplicationConfig())

    @field_validator("deduplication")
    @classmethod
    def validate_deduplication_id_field(
        cls, v: DeduplicationConfig, info: ValidationInfo
    ) -> DeduplicationConfig:
        """
        Validate that the deduplication ID field exists in the
        schema and has matching type.
        """
        if v is None or not v.enabled:
            return v

        # Skip validation if id_field is empty when deduplication is disabled
        if not v.id_field or v.id_field == "":
            return v

        # Get the schema from the parent model's data
        schema = info.data.get("event_schema", {})
        if isinstance(schema, dict):
            fields = schema.get("fields", [])
        else:
            fields = schema.fields

        # Find the field in the schema
        field = next((f for f in fields if f.name == v.id_field), None)
        if not field:
            raise ValueError(
                f"Deduplication ID field '{v.id_field}' does not exist in "
                "the event schema"
            )

        # Check if the field type matches the deduplication ID field type
        if field.type.value != v.id_field_type.value:
            raise ValueError(
                f"Deduplication ID field type '{v.id_field_type.value}' does not match "
                f"schema field type '{field.type.value}' for field '{v.id_field}'"
            )

        return v


class KafkaConnectionParams(BaseModel):
    brokers: List[str]
    protocol: KafkaProtocol
    mechanism: Optional[KafkaMechanism] = Field(default=None)
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    root_ca: Optional[str] = Field(default=None)
    skip_auth: bool = Field(default=False)

    @model_validator(mode="before")
    def empty_str_to_none(values):
        if values.get("mechanism", None) == "":
            values["mechanism"] = None
        return values


class SourceType(CaseInsensitiveStrEnum):
    KAFKA = "kafka"


class SourceConfig(BaseModel):
    type: SourceType = SourceType.KAFKA
    provider: Optional[str] = Field(default=None)
    connection_params: KafkaConnectionParams
    topics: List[TopicConfig]


class DeduplicationConfigPatch(BaseModel):
    enabled: Optional[bool] = Field(default=None)
    id_field: Optional[str] = Field(default=None)
    id_field_type: Optional[KafkaDataType] = Field(default=None)
    time_window: Optional[str] = Field(default=None)


class TopicConfigPatch(BaseModel):
    consumer_group_initial_offset: Optional[ConsumerGroupOffset] = Field(default=None)
    name: Optional[str] = Field(default=None)
    event_schema: Optional[Schema] = Field(default=None)
    deduplication: Optional[DeduplicationConfigPatch] = Field(default=None)


class KafkaConnectionParamsPatch(BaseModel):
    brokers: Optional[List[str]] = Field(default=None)
    protocol: Optional[KafkaProtocol] = Field(default=None)
    mechanism: Optional[KafkaMechanism] = Field(default=None)
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    root_ca: Optional[str] = Field(default=None)
    skip_auth: Optional[bool] = Field(default=None)


class SourceConfigPatch(BaseModel):
    provider: Optional[str] = Field(default=None)
    connection_params: Optional[KafkaConnectionParamsPatch] = Field(default=None)
    topics: Optional[List[TopicConfigPatch]] = Field(default=None)
