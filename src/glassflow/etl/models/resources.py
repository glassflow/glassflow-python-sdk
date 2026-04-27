from typing import List, Optional

from pydantic import BaseModel, Field

from glassflow.etl.errors import ImmutableResourceError


class JetStreamResources(BaseModel):
    max_age: Optional[str] = Field(default=None, frozen=True, alias="maxAge")
    max_bytes: Optional[str] = Field(default=None, frozen=True, alias="maxBytes")

    def update(self, patch: "JetStreamResources") -> "JetStreamResources":
        """Apply a patch to this jetstream resources config."""
        if patch.max_age is not None or patch.max_bytes is not None:
            raise ImmutableResourceError(
                "Cannot update pipeline resources: 'maxAge' and 'maxBytes' in "
                "nats.stream are immutable and cannot be changed after pipeline "
                "creation."
            )
        return self.model_copy(deep=True)


class NATSResources(BaseModel):
    stream: Optional[JetStreamResources] = Field(default=None)

    def update(self, patch: "NATSResources") -> "NATSResources":
        """Apply a patch to this jetstream resources config."""
        updated_config = self.model_copy(deep=True)
        if patch.stream is not None:
            updated_config.stream = (
                updated_config.stream or JetStreamResources()
            ).update(patch.stream)
        return updated_config


class Resources(BaseModel):
    memory: Optional[str] = Field(default=None)
    cpu: Optional[str] = Field(default=None)

    def update(self, patch: "Resources") -> "Resources":
        """Apply a patch to this resources config."""
        updated_config = self.model_copy(deep=True)
        if patch.memory is not None:
            updated_config.memory = patch.memory
        if patch.cpu is not None:
            updated_config.cpu = patch.cpu
        return updated_config


class StorageResources(BaseModel):
    size: Optional[str] = Field(default=None, frozen=True)

    def update(self, patch: "StorageResources") -> "StorageResources":
        """Apply a patch to this storage resources config."""
        if patch.size is not None:
            raise ImmutableResourceError(
                "Cannot update pipeline resources: 'size' in transform.storage is "
                "immutable and cannot be changed after pipeline creation."
            )
        return self.model_copy(deep=True)


class SourceResourceEntry(BaseModel):
    """Resource allocation for a single source (replaces the old ingestor model)."""

    source_id: str
    replicas: Optional[int] = Field(default=None)
    requests: Optional[Resources] = Field(default=None)
    limits: Optional[Resources] = Field(default=None)


class TransformResourceEntry(BaseModel):
    """Resource allocation for a single transform (per source_id)."""

    source_id: str
    replicas: Optional[int] = Field(default=None)
    storage: Optional[StorageResources] = Field(default=None)
    requests: Optional[Resources] = Field(default=None)
    limits: Optional[Resources] = Field(default=None)


class SinkResources(BaseModel):
    replicas: Optional[int] = Field(default=None)
    requests: Optional[Resources] = Field(default=None)
    limits: Optional[Resources] = Field(default=None)

    def update(self, patch: "SinkResources") -> "SinkResources":
        """Apply a patch to this sink resources config."""
        updated_config = self.model_copy(deep=True)
        if patch.replicas is not None:
            updated_config.replicas = patch.replicas
        if patch.requests is not None:
            updated_config.requests = (updated_config.requests or Resources()).update(
                patch.requests
            )
        if patch.limits is not None:
            updated_config.limits = (updated_config.limits or Resources()).update(
                patch.limits
            )
        return updated_config


class PipelineResourcesConfig(BaseModel):
    nats: Optional[NATSResources] = Field(default=None)
    sources: Optional[List[SourceResourceEntry]] = Field(default=None)
    transform: Optional[List[TransformResourceEntry]] = Field(default=None)
    sink: Optional[SinkResources] = Field(default=None)

    def update(self, patch: "PipelineResourcesConfig") -> "PipelineResourcesConfig":
        """Apply a patch to this pipeline resources config."""
        updated_config = self.model_copy(deep=True)

        if patch.nats is not None:
            updated_config.nats = (updated_config.nats or NATSResources()).update(
                patch.nats
            )
        if patch.sink is not None:
            updated_config.sink = (updated_config.sink or SinkResources()).update(
                patch.sink
            )
        if patch.sources is not None:
            updated_config.sources = patch.sources
        if patch.transform is not None:
            updated_config.transform = patch.transform
        return updated_config
