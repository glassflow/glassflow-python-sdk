from typing import Annotated, Optional

from pydantic import BaseModel, Field, WithJsonSchema


class JetStreamResources(BaseModel):
    max_age: Annotated[Optional[str], WithJsonSchema({"immuttable": True})] = Field(
        default=None
    )
    max_bytes: Annotated[Optional[str], WithJsonSchema({"immuttable": True})] = Field(
        default=None
    )


class NATSResources(BaseModel):
    stream: Optional[JetStreamResources] = Field(default=None)


class Resources(BaseModel):
    memory: Annotated[Optional[str], WithJsonSchema({"immuttable": False})] = Field(
        default=None
    )
    cpu: Annotated[Optional[str], WithJsonSchema({"immuttable": False})] = Field(
        default=None
    )


class StorageResources(BaseModel):
    size: Annotated[Optional[str], WithJsonSchema({"immuttable": True})] = Field(
        default=None
    )


class TransformResources(BaseModel):
    storage: Optional[StorageResources] = Field(default=None)
    replicas: Annotated[Optional[int], WithJsonSchema({"immuttable": True})] = Field(
        default=None
    )
    requests: Optional[Resources] = Field(default=None)
    limits: Optional[Resources] = Field(default=None)


class SinkResources(BaseModel):
    replicas: Annotated[Optional[int], WithJsonSchema({"immuttable": True})] = Field(
        default=None
    )
    requests: Optional[Resources] = Field(default=None)
    limits: Optional[Resources] = Field(default=None)


class JoinResources(BaseModel):
    limits: Optional[Resources] = Field(default=None)
    requests: Optional[Resources] = Field(default=None)
    replicas: Annotated[Optional[int], WithJsonSchema({"immuttable": True})] = Field(
        default=None
    )


class IngestorPodResources(BaseModel):
    replicas: Annotated[Optional[int], WithJsonSchema({"immuttable": False})] = Field(
        default=None
    )
    requests: Optional[Resources] = Field(default=None)
    limits: Optional[Resources] = Field(default=None)


class IngestorResources(BaseModel):
    base: Optional[IngestorPodResources] = Field(default=None)
    left: Optional[IngestorPodResources] = Field(default=None)
    right: Optional[IngestorPodResources] = Field(default=None)


class PipelineResourcesConfig(BaseModel):
    nats: Optional[NATSResources] = Field(default=None)
    sink: Optional[SinkResources] = Field(default=None)
    ingestor: Optional[IngestorResources] = Field(default=None)
    transform: Optional[TransformResources] = Field(default=None)
    join: Optional[JoinResources] = Field(default=None)
