from typing import Optional

from pydantic import BaseModel, Field


class JetStreamResources(BaseModel):
    max_age: Optional[str] = Field(default=None, frozen=True)
    max_bytes: Optional[str] = Field(default=None, frozen=True)

    def update(self, patch: "JetStreamResources") -> "JetStreamResources":
        """Apply a patch to this jetstream resources config."""
        updated_config = self.model_copy(deep=True)
        if patch.max_age is not None:
            updated_config.max_age = patch.max_age
        if patch.max_bytes is not None:
            updated_config.max_bytes = patch.max_bytes
        return updated_config


class NATSResources(BaseModel):
    stream: Optional[JetStreamResources] = Field(default=None)

    def update(self, patch: "NATSResources") -> "NATSResources":
        """Apply a patch to this jetstream resources config."""
        updated_config = self.model_copy(deep=True)
        if patch.stream is not None:
            updated_config.stream = updated_config.stream.update(patch.stream)
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
        updated_config = self.model_copy(deep=True)
        if patch.size is not None:
            updated_config.size = patch.size
        return updated_config


class TransformResources(BaseModel):
    storage: Optional[StorageResources] = Field(default=None)
    replicas: Optional[int] = Field(default=None, frozen=True)
    requests: Optional[Resources] = Field(default=None)
    limits: Optional[Resources] = Field(default=None)

    def update(self, patch: "TransformResources") -> "TransformResources":
        """Apply a patch to this transform resources config."""
        updated_config = self.model_copy(deep=True)
        if patch.storage is not None:
            updated_config.storage = updated_config.storage.update(patch.storage)
        if patch.replicas is not None:
            updated_config.replicas = patch.replicas
        if patch.requests is not None:
            updated_config.requests = updated_config.requests.update(patch.requests)
        if patch.limits is not None:
            updated_config.limits = updated_config.limits.update(patch.limits)
        return updated_config


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
            updated_config.requests = updated_config.requests.update(patch.requests)
        if patch.limits is not None:
            updated_config.limits = updated_config.limits.update(patch.limits)
        return updated_config


class JoinResources(BaseModel):
    limits: Optional[Resources] = Field(default=None)
    requests: Optional[Resources] = Field(default=None)
    replicas: Optional[int] = Field(default=None, frozen=True)

    def update(self, patch: "JoinResources") -> "JoinResources":
        """Apply a patch to this join resources config."""
        updated_config = self.model_copy(deep=True)
        if patch.limits is not None:
            updated_config.limits = updated_config.limits.update(patch.limits)
        if patch.requests is not None:
            updated_config.requests = updated_config.requests.update(patch.requests)
        if patch.replicas is not None:
            updated_config.replicas = patch.replicas


class IngestorPodResources(BaseModel):
    replicas: Optional[int] = Field(default=None)
    requests: Optional[Resources] = Field(default=None)
    limits: Optional[Resources] = Field(default=None)

    def update(self, patch: "IngestorPodResources") -> "IngestorPodResources":
        """Apply a patch to this ingestor pod resources config."""
        updated_config = self.model_copy(deep=True)
        if patch.replicas is not None:
            updated_config.replicas = patch.replicas
        if patch.requests is not None:
            updated_config.requests = updated_config.requests.update(patch.requests)
        if patch.limits is not None:
            updated_config.limits = updated_config.limits.update(patch.limits)
        return updated_config


class IngestorResources(BaseModel):
    base: Optional[IngestorPodResources] = Field(default=None)
    left: Optional[IngestorPodResources] = Field(default=None)
    right: Optional[IngestorPodResources] = Field(default=None)

    def update(self, patch: "IngestorResources") -> "IngestorResources":
        """Apply a patch to this ingestor resources config."""
        updated_config = self.model_copy(deep=True)

        if patch.base is not None:
            updated_config.base = updated_config.base.update(patch.base)
        if patch.left is not None:
            updated_config.left = updated_config.left.update(patch.left)
        if patch.right is not None:
            updated_config.right = updated_config.right.update(patch.right)
        return updated_config


class PipelineResourcesConfig(BaseModel):
    nats: Optional[NATSResources] = Field(default=None)
    sink: Optional[SinkResources] = Field(default=None)
    ingestor: Optional[IngestorResources] = Field(default=None)
    transform: Optional[TransformResources] = Field(default=None)
    join: Optional[JoinResources] = Field(default=None)

    def update(self, patch: "PipelineResourcesConfig") -> "PipelineResourcesConfig":
        """Apply a patch to this pipeline resources config."""
        updated_config = self.model_copy(deep=True)

        if patch.nats is not None:
            updated_config.nats = updated_config.nats.update(patch.nats)
        if patch.sink is not None:
            updated_config.sink = updated_config.sink.update(patch.sink)
        if patch.ingestor is not None:
            updated_config.ingestor = updated_config.ingestor.update(patch.ingestor)
        if patch.transform is not None:
            updated_config.transform = updated_config.transform.update(patch.transform)
        if patch.join is not None:
            updated_config.join = updated_config.join.update(patch.join)
        return updated_config
