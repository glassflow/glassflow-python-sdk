import re
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .base import CaseInsensitiveStrEnum
from .filter import FilterConfig, FilterConfigPatch
from .join import JoinConfig, JoinConfigPatch
from .metadata import MetadataConfig
from .schema import Schema
from .sink import SinkConfig, SinkConfigPatch
from .source import SourceConfig, SourceConfigPatch


class PipelineStatus(CaseInsensitiveStrEnum):
    CREATED = "Created"
    RUNNING = "Running"
    RESUMING = "Resuming"
    STOPPING = "Stopping"
    STOPPED = "Stopped"
    TERMINATING = "Terminating"
    TERMINATED = "Terminated"
    FAILED = "Failed"
    DELETED = "Deleted"


class PipelineConfig(BaseModel):
    version: str = Field(default="v2")
    pipeline_id: str
    name: Optional[str] = Field(default=None)
    source: SourceConfig
    join: Optional[JoinConfig] = Field(default=JoinConfig())
    filter: Optional[FilterConfig] = Field(default=FilterConfig())
    metadata: Optional[MetadataConfig] = Field(default=MetadataConfig())
    sink: SinkConfig
    pipeline_schema: Schema = Field(alias="schema")

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
    def validate_config(self) -> "PipelineConfig":
        """
        Set pipeline name if not provided and validate configuration.
        """
        # Set pipeline name if not provided
        if self.name is None:
            self.name = self.pipeline_id.replace("-", " ").title()

        # Validate schema
        topic_names = [topic.name for topic in self.source.topics]
        for field in self.pipeline_schema.fields:
            if field.source_id not in topic_names:
                raise ValueError(
                    f"Source '{field.source_id}' does not exist in any topic"
                )

        # Validate deduplication ID fields
        for topic in self.source.topics:
            if topic.deduplication is None or not topic.deduplication.enabled:
                continue

            if not self.pipeline_schema.is_field_in_schema(
                topic.deduplication.id_field, topic.name
            ):
                raise ValueError(
                    f"Deduplication id_field '{topic.deduplication.id_field}' "
                    f"not found in schema from source '{topic.name}'"
                )

        # Validate join configuration
        if self.join and self.join.enabled:
            # Validate each source in the join config
            for join_source in self.join.sources:
                if join_source.source_id not in topic_names:
                    raise ValueError(
                        f"Join source '{join_source.source_id}' does not exist in any "
                        "topic"
                    )

                if not self.pipeline_schema.is_field_in_schema(
                    join_source.join_key,
                    join_source.source_id,
                ):
                    raise ValueError(
                        f"Join key '{join_source.join_key}' does not exist in source "
                        f"'{join_source.source_id}' schema"
                    )

        return self

    def update(self, config_patch: "PipelineConfigPatch") -> "PipelineConfig":
        """
        Apply a patch configuration to this pipeline configuration.

        Args:
            config_patch: The patch configuration (PipelineConfigPatch or dict)

        Returns:
            PipelineConfig: A new PipelineConfig instance with the patch applied
        """
        # Start with a deep copy of the current config
        updated_config = self.model_copy(deep=True)

        # Update name if provided
        if config_patch.name is not None:
            updated_config.name = config_patch.name

        # Update source if provided
        if config_patch.source is not None:
            updated_config.source = updated_config.source.update(config_patch.source)

        # Update join if provided
        if config_patch.join is not None:
            updated_config.join = (updated_config.join or JoinConfig()).update(
                config_patch.join
            )

        # Update filter if provided
        if config_patch.filter is not None:
            updated_config.filter = (updated_config.filter or FilterConfig()).update(
                config_patch.filter
            )

        # Update sink if provided
        if config_patch.sink is not None:
            updated_config.sink = updated_config.sink.update(config_patch.sink)

        # Update schema if provided
        if config_patch.pipeline_schema is not None:
            updated_config.pipeline_schema = config_patch.pipeline_schema

        if config_patch.metadata is not None:
            updated_config.metadata = config_patch.metadata

        return updated_config


class PipelineConfigPatch(BaseModel):
    name: Optional[str] = Field(default=None)
    join: Optional[JoinConfigPatch] = Field(default=None)
    filter: Optional[FilterConfigPatch] = Field(default=None)
    metadata: Optional[MetadataConfig] = Field(default=None)
    pipeline_schema: Optional[Schema] = Field(default=None, alias="schema")
    sink: Optional[SinkConfigPatch] = Field(default=None)
    source: Optional[SourceConfigPatch] = Field(default=None)
