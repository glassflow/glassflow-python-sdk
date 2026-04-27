import re
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .base import CaseInsensitiveStrEnum
from .metadata import MetadataConfig
from .resources import PipelineResourcesConfig
from .sink import SinkConfig, SinkConfigPatch
from .sources import KafkaSource, OTLPSource, SourceConfig
from .transforms import (
    DedupTransform,
    JoinConfig,
    JoinConfigPatch,
    TransformEntry,
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
    sources: List[SourceConfig]
    transforms: Optional[List[TransformEntry]] = Field(default=None)
    join: Optional[JoinConfig] = Field(default=None)
    sink: SinkConfig
    metadata: Optional[MetadataConfig] = Field(default=MetadataConfig())
    resources: Optional[PipelineResourcesConfig] = Field(default=None)

    @field_validator("version")
    @classmethod
    def validate_version(cls, v: PipelineVersion) -> PipelineVersion:
        if v == PipelineVersion.V1:
            raise ValueError(
                "Pipeline version v1 is no longer supported by this SDK. "
                "Please use glassflow-python-sdk<2.0.0 to work with v1 pipelines."
            )
        if v == PipelineVersion.V2:
            raise ValueError(
                "Pipeline version v2 is no longer supported by this SDK. "
                "Convert your v2 configuration to v3 by calling "
                "`glassflow.etl.Client().migrate_pipeline_v2_to_v3(config)`, "
                "then pass the returned config to create_pipeline(). "
                "Alternatively, pin glassflow-python-sdk<4.0.0 to keep "
                "working with v2 pipelines directly."
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

        # Validate sources list is non-empty
        if not self.sources:
            raise ValueError("At least one source is required")

        # Validate source_id uniqueness
        source_id_list = [src.source_id for src in self.sources]
        if len(source_id_list) != len(set(source_id_list)):
            duplicates = {
                sid for sid in source_id_list if source_id_list.count(sid) > 1
            }
            raise ValueError(f"Duplicate source_id(s) found: {duplicates}")

        # Build the set of valid source identifiers from sources list
        source_ids = set(source_id_list)
        has_otlp = any(isinstance(src, OTLPSource) for src in self.sources)

        if has_otlp and self.join and self.join.enabled:
            raise ValueError("join.enabled must be False for OTLP pipelines")

        # Validate join source references
        if self.join and self.join.enabled:
            if self.join.left_source:
                if self.join.left_source.source_id not in source_ids:
                    raise ValueError(
                        f"Join left_source '{self.join.left_source.source_id}' "
                        "does not match any source"
                    )
            if self.join.right_source:
                if self.join.right_source.source_id not in source_ids:
                    raise ValueError(
                        f"Join right_source '{self.join.right_source.source_id}' "
                        "does not match any source"
                    )

        # Validate transform source_id references
        if self.transforms:
            for transform in self.transforms:
                if transform.source_id not in source_ids:
                    raise ValueError(
                        f"Transform source_id '{transform.source_id}' "
                        "does not match any source"
                    )

        # Validate dedup keys against source schema_fields (Kafka only)
        if self.transforms:
            source_by_id = {src.source_id: src for src in self.sources}
            for transform in self.transforms:
                if isinstance(transform, DedupTransform):
                    src = source_by_id.get(transform.source_id)
                    if isinstance(src, KafkaSource) and src.schema_fields is not None:
                        field_names = {f.name for f in src.schema_fields}
                        if transform.config.key not in field_names:
                            raise ValueError(
                                f"Dedup key '{transform.config.key}' not found "
                                f"in schema_fields of source "
                                f"'{transform.source_id}'"
                            )

        # Validate join keys against source schema_fields (Kafka only)
        if self.join and self.join.enabled:
            source_by_id = {src.source_id: src for src in self.sources}
            for join_src in [self.join.left_source, self.join.right_source]:
                if join_src is None:
                    continue
                src = source_by_id.get(join_src.source_id)
                if isinstance(src, KafkaSource) and src.schema_fields is not None:
                    field_names = {f.name for f in src.schema_fields}
                    if join_src.key not in field_names:
                        raise ValueError(
                            f"Join key '{join_src.key}' does not exist in source "
                            f"'{join_src.source_id}' schema_fields"
                        )

        return self

    def _has_deduplication_enabled(self) -> bool:
        """Check if the pipeline has any dedup transforms."""
        if not self.transforms:
            return False
        return any(isinstance(t, DedupTransform) for t in self.transforms)

    def update(self, config_patch: "PipelineConfigPatch") -> "PipelineConfig":
        """Apply a patch configuration to this pipeline configuration."""
        updated_config = self.model_copy(deep=True)

        if config_patch.name is not None:
            updated_config.name = config_patch.name

        if config_patch.resources is not None:
            updated_config.resources = (
                updated_config.resources or PipelineResourcesConfig()
            ).update(config_patch.resources)

        if config_patch.join is not None:
            updated_config.join = (updated_config.join or JoinConfig()).update(
                config_patch.join
            )

        if config_patch.sink is not None:
            updated_config.sink = updated_config.sink.update(config_patch.sink)

        if config_patch.metadata is not None:
            updated_config.metadata = config_patch.metadata

        if config_patch.transforms is not None:
            updated_config.transforms = config_patch.transforms

        if config_patch.sources is not None:
            updated_config.sources = config_patch.sources

        return updated_config


class PipelineConfigPatch(BaseModel):
    name: Optional[str] = Field(default=None)
    sources: Optional[List[SourceConfig]] = Field(default=None)
    transforms: Optional[List[TransformEntry]] = Field(default=None)
    join: Optional[JoinConfigPatch] = Field(default=None)
    metadata: Optional[MetadataConfig] = Field(default=None)
    sink: Optional[SinkConfigPatch] = Field(default=None)
    resources: Optional[PipelineResourcesConfig] = Field(default=None)
    version: Optional[PipelineVersion] = Field(default=None)
