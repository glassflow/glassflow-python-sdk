import re
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from ..errors import ImmutableResourceError
from .base import CaseInsensitiveStrEnum
from .metadata import MetadataConfig
from .resources import PipelineResourcesConfig
from .sink import SinkConfig, SinkConfigPatch
from .sources import KafkaSourcePatch, OTLPSource, SourceConfig, SourceConfigPatch
from .transforms import (
    FilterConfig,
    FilterConfigPatch,
    JoinConfig,
    JoinConfigPatch,
    StatelessTransformationConfig,
    StatelessTransformationConfigPatch,
)


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


class PipelineVersion(CaseInsensitiveStrEnum):
    V1 = "v1"
    V2 = "v2"
    V3 = "v3"


class PipelineConfig(BaseModel):
    version: PipelineVersion = Field(default=PipelineVersion.V3)
    pipeline_id: str
    name: Optional[str] = Field(default=None)
    source: SourceConfig
    join: Optional[JoinConfig] = Field(default=JoinConfig())
    filter: Optional[FilterConfig] = Field(default=FilterConfig())
    metadata: Optional[MetadataConfig] = Field(default=MetadataConfig())
    sink: SinkConfig
    stateless_transformation: Optional[StatelessTransformationConfig] = Field(
        default=StatelessTransformationConfig()
    )
    pipeline_resources: Optional[PipelineResourcesConfig] = Field(default=None)

    @field_validator("version")
    @classmethod
    def validate_version(cls, v: PipelineVersion) -> PipelineVersion:
        if v in (PipelineVersion.V1, PipelineVersion.V2):
            raise ValueError(
                f"Pipeline version {v} is no longer supported by this SDK. "
                "Please use glassflow-python-sdk<2.0.0 for v1 pipelines, "
                "glassflow-python-sdk<4.0.0 for v2 pipelines, "
                "or migrate your pipeline configuration to v3."
            )
        return v

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
        if self.name is None:
            self.name = self.pipeline_id.replace("-", " ").title()

        # Build the set of valid source identifiers
        if isinstance(self.source, OTLPSource):
            if self.join and self.join.enabled:
                raise ValueError("join.enabled must be False for OTLP pipelines")
            valid_source_ids = {self.source.id}
            if self.stateless_transformation and self.stateless_transformation.enabled:
                valid_source_ids.add(self.stateless_transformation.id)
        else:
            # Kafka: prefer explicit id, fall back to name
            topic_ids = [topic.effective_id for topic in self.source.topics]
            topic_names = [topic.name for topic in self.source.topics]
            valid_source_ids = set(dict.fromkeys(topic_ids + topic_names))
            if self.stateless_transformation and self.stateless_transformation.enabled:
                valid_source_ids.add(self.stateless_transformation.id)
            if self.join and self.join.enabled and self.join.id:
                valid_source_ids.add(self.join.id)

        # Validate sink.source_id if provided
        if self.sink.source_id is not None:
            if self.sink.source_id not in valid_source_ids:
                raise ValueError(
                    f"Sink source_id '{self.sink.source_id}' does not match"
                    " any known source"
                )

        if isinstance(self.source, OTLPSource):
            return self

        # Kafka-specific validation
        # Build a lookup: topic effective_id -> topic for field-level validation
        topic_by_id = {topic.effective_id: topic for topic in self.source.topics}
        topic_by_name = {topic.name: topic for topic in self.source.topics}

        # Validate deduplication keys against topic fields (only if fields defined)
        for topic in self.source.topics:
            if topic.deduplication is None or not topic.deduplication.enabled:
                continue
            if topic.schema_fields is None:
                continue
            field_names = {f.name for f in topic.schema_fields}
            if topic.deduplication.key not in field_names:
                raise ValueError(
                    f"Deduplication key '{topic.deduplication.key}' "
                    f"not found in fields of topic '{topic.name}'"
                )

        # Validate join keys against topic fields (only if fields defined)
        if self.join and self.join.enabled:
            all_topic_identifiers = set(
                dict.fromkeys(
                    [t.effective_id for t in self.source.topics]
                    + [t.name for t in self.source.topics]
                )
            )
            for join_source in self.join.sources:
                if join_source.source_id not in all_topic_identifiers:
                    raise ValueError(
                        f"Join source '{join_source.source_id}' does not exist"
                        " in any topic"
                    )
                # Find the topic
                topic = topic_by_id.get(join_source.source_id) or topic_by_name.get(
                    join_source.source_id
                )
                if topic is not None and topic.schema_fields is not None:
                    field_names = {f.name for f in topic.schema_fields}
                    if join_source.key not in field_names:
                        raise ValueError(
                            f"Join key '{join_source.key}' does not exist in source "
                            f"'{join_source.source_id}' fields"
                        )

        return self

    def _has_deduplication_enabled(self) -> bool:
        """
        Check if the pipeline has deduplication enabled.
        """
        if isinstance(self.source, OTLPSource):
            return (
                self.source.deduplication is not None
                and self.source.deduplication.enabled
            )
        return any(
            topic.deduplication and topic.deduplication.enabled
            for topic in self.source.topics
            if topic.deduplication is not None
        )

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

        # Update pipeline resources if provided
        if config_patch.pipeline_resources is not None:
            if (
                config_patch.pipeline_resources.transform is not None
                and config_patch.pipeline_resources.transform.replicas is not None
                and self._has_deduplication_enabled()
                and not config_patch._has_deduplication_disabled()
            ):
                raise ImmutableResourceError(
                    "Cannot update pipeline resources of a transform component if the "
                    "pipeline has deduplication enabled"
                )

            updated_config.pipeline_resources = (
                updated_config.pipeline_resources or PipelineResourcesConfig()
            ).update(config_patch.pipeline_resources)

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

        if config_patch.metadata is not None:
            updated_config.metadata = config_patch.metadata

        # Update stateless transformation if provided
        if config_patch.stateless_transformation is not None:
            updated_config.stateless_transformation = (
                updated_config.stateless_transformation
                or StatelessTransformationConfig()
            ).update(config_patch.stateless_transformation)

        return updated_config


class PipelineConfigPatch(BaseModel):
    name: Optional[str] = Field(default=None)
    join: Optional[JoinConfigPatch] = Field(default=None)
    filter: Optional[FilterConfigPatch] = Field(default=None)
    metadata: Optional[MetadataConfig] = Field(default=None)
    sink: Optional[SinkConfigPatch] = Field(default=None)
    source: Optional[SourceConfigPatch] = Field(default=None)
    stateless_transformation: Optional[StatelessTransformationConfigPatch] = Field(
        default=None
    )
    pipeline_resources: Optional[PipelineResourcesConfig] = Field(default=None)
    version: Optional[str] = Field(default=None)

    def _has_deduplication_disabled(self) -> bool:
        """
        Check if the pipeline has deduplication disabled.
        """
        if not isinstance(self.source, KafkaSourcePatch) or self.source.topics is None:
            return False
        return any(
            topic.deduplication and not topic.deduplication.enabled
            for topic in self.source.topics
        )
