"""
Keep here until we transition to full support of YAML
"""

from __future__ import annotations

import uuid
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, ValidationError, model_validator


class GlassFlowConfig(BaseModel):
    organization_id: uuid.UUID


class Pipeline(BaseModel):
    name: str
    pipeline_id: uuid.UUID | None = Field(None)
    space_id: uuid.UUID
    components: list[Component]

    @model_validator(mode="after")
    def check_components(self):
        """Validate pipeline has source, transformer and sink"""

        assert len(self.components) == 3

        source = [c for c in self.components if c.type == "source"]
        transformer = [c for c in self.components if c.type == "transformer"]
        sink = [c for c in self.components if c.type == "sink"]

        assert len(source) == 1
        assert len(transformer) == 1
        assert len(sink) == 1

        assert transformer[0].inputs[0] == source[0].id
        assert sink[0].inputs[0] == transformer[0].id

        return self


class EnvironmentVariable(BaseModel):
    name: str
    value: str | None = Field(None)
    value_secret_ref: str | None = Field(None)

    @model_validator(mode="after")
    def check_filled(self):
        if self.value_secret_ref is None and self.value is None:
            raise ValidationError("value or value_secret_ref must be filled")
        return self


class Requirements(BaseModel):
    path: str | None = Field(None)
    value: str | None = Field(None)

    @model_validator(mode="after")
    def check_filled(self):
        if self.path is None and self.value is None:
            raise ValidationError("Path or value must be filled")
        return self


class Transformation(BaseModel):
    path: str | None = Field(None)
    value: str | None = Field(None)

    @model_validator(mode="after")
    def check_filled(self):
        if self.path is None and self.value is None:
            raise ValidationError("Path or value must be filled")
        return self


class BaseComponent(BaseModel):
    id: str
    name: str
    type: str


class TransformerComponent(BaseComponent):
    type: Literal["transformer"]
    requirements: Requirements
    transformation: Transformation
    inputs: list[str]
    env_vars: list[EnvironmentVariable]


class SourceComponent(BaseComponent):
    type: Literal["source"]
    inputs: list[str]
    kind: str | None = Field(None)
    config: dict | None = Field(None)
    config_secret_ref: str | None = Field(None)

    @model_validator(mode="after")
    def check_filled(self):
        if (
            self.kind is not None
            and self.config is None
            and self.config_secret_ref is None
        ):
            raise ValidationError("config or config_secret_ref must be filled")
        return self


class SinkComponent(BaseComponent):
    type: Literal["sink"]
    kind: str | None = Field(None)
    config: dict | None = Field(None)
    config_secret_ref: str | None = Field(None)
    inputs: list[str]

    @model_validator(mode="after")
    def check_filled(self):
        if (
            self.kind is not None
            and self.config is None
            and self.config_secret_ref is None
        ):
            raise ValidationError("config or config_secret_ref must be filled")
        return self


Component = Annotated[
    Union[TransformerComponent, SourceComponent, SinkComponent],
    Field(discriminator="type"),
]
