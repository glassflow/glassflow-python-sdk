import re
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from ..errors import InvalidDataTypeMappingError
from .data_types import kafka_to_clickhouse_data_type_mappings
from .join import JoinConfig, JoinConfigPatch
from .sink import SinkConfig, SinkConfigPatch
from .source import SourceConfig, SourceConfigPatch


class PipelineConfig(BaseModel):
    pipeline_id: str
    name: Optional[str] = Field(default=None)
    source: SourceConfig
    join: Optional[JoinConfig] = Field(default=JoinConfig())
    sink: SinkConfig

    @field_validator("pipeline_id")
    @classmethod
    def validate_pipeline_id(cls, v: str) -> str:
        if not v:
            raise ValueError("pipeline_id cannot be empty")
        if len(v) > 40:
            raise ValueError("pipeline_id cannot be longer than 40 characters")
        if not re.match(r"^[a-z0-9-]+$", v):
            raise ValueError(
                "pipeline_id can only contain lowercase letters, numbers, and hyphens"
            )
        if not re.match(r"^[a-z0-9]", v):
            raise ValueError("pipeline_id must start with a lowercase alphanumeric")
        if not re.match(r".*[a-z0-9]$", v):
            raise ValueError("pipeline_id must end with a lowercase alphanumeric")
        return v

    @model_validator(mode="after")
    def set_pipeline_name(self) -> "PipelineConfig":
        """
        If name is not provided, use the pipeline_id and replace hyphens
        with spaces.
        """
        if self.name is None:
            self.name = self.pipeline_id.replace("-", " ").title()
        return self

    @field_validator("join")
    @classmethod
    def validate_join_config(
        cls,
        v: Optional[JoinConfig],
        info: Any,
    ) -> Optional[JoinConfig]:
        if not v or not v.enabled:
            return v

        # Get the source topics from the parent model's data
        source = info.data.get("source", {})
        if isinstance(source, dict):
            source_topics = source.get("topics", [])
        else:
            source_topics = source.topics
        if not source_topics:
            return v

        # Validate each source in the join config
        for source in v.sources:
            # Check if source_id exists in any topic
            source_exists = any(
                topic.name == source.source_id for topic in source_topics
            )
            if not source_exists:
                raise ValueError(
                    f"Source ID '{source.source_id}' does not exist in any topic"
                )

            # Find the topic and check if join_key exists in its schema
            topic = next((t for t in source_topics if t.name == source.source_id), None)
            if not topic:
                continue

            field_exists = any(
                field.name == source.join_key for field in topic.event_schema.fields
            )
            if not field_exists:
                raise ValueError(
                    f"Join key '{source.join_key}' does not exist in source "
                    f"'{source.source_id}' schema"
                )

        return v

    @field_validator("sink")
    @classmethod
    def validate_sink_config(cls, v: SinkConfig, info: Any) -> SinkConfig:
        # Get the source topics from the parent model's data
        source = info.data.get("source", {})
        if isinstance(source, dict):
            source_topics = source.get("topics", [])
        else:
            source_topics = source.topics
        if not source_topics:
            return v

        # Validate each table mapping
        for mapping in v.table_mapping:
            # Check if source_id exists in any topic
            source_exists = any(
                topic.name == mapping.source_id for topic in source_topics
            )
            if not source_exists:
                raise ValueError(
                    f"Source ID '{mapping.source_id}' does not exist in any topic"
                )

            # Find the topic and check if field_name exists in its schema
            topic = next(
                (t for t in source_topics if t.name == mapping.source_id), None
            )
            if not topic:
                continue

            field_exists = any(
                field.name == mapping.field_name for field in topic.event_schema.fields
            )
            if not field_exists:
                raise ValueError(
                    f"Field '{mapping.field_name}' does not exist in source "
                    f"'{mapping.source_id}' event schema"
                )

        return v

    @field_validator("sink")
    @classmethod
    def validate_data_type_compatibility(cls, v: SinkConfig, info: Any) -> SinkConfig:
        # Get the source topics from the parent model's data
        source = info.data.get("source", {})
        if isinstance(source, dict):
            source_topics = source.get("topics", [])
        else:
            source_topics = source.topics
        if not source_topics:
            return v

        # Validate each table mapping
        for mapping in v.table_mapping:
            # Find the topic
            topic = next(
                (t for t in source_topics if t.name == mapping.source_id), None
            )
            if not topic:
                continue

            # Find the source field
            source_field = next(
                (f for f in topic.event_schema.fields if f.name == mapping.field_name),
                None,
            )
            if not source_field:
                continue

            # Get the source and target data types
            source_type = source_field.type
            target_type = mapping.column_type

            # Check if the target type is compatible with the source type
            compatible_types = kafka_to_clickhouse_data_type_mappings.get(
                source_type, []
            )
            if target_type not in compatible_types:
                raise InvalidDataTypeMappingError(
                    f"Data type '{target_type}' is not compatible with source type "
                    f"'{source_type}' for field '{mapping.field_name}' in source "
                    f"'{mapping.source_id}'"
                )

        return v


class PipelineConfigPatch(BaseModel):
    name: Optional[str] = Field(default=None)
    source: Optional[SourceConfigPatch] = Field(default=None)
    join: Optional[JoinConfigPatch] = Field(default=None)
    sink: Optional[SinkConfigPatch] = Field(default=None)
