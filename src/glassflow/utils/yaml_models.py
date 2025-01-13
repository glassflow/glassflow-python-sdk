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
    blocks: list[Block]

    @model_validator(mode="after")
    def check_blocks(self):
        """Validate pipeline has source, transformer and sink"""

        assert len(self.blocks) == 3

        source = [b for b in self.blocks if b.type == "source"]
        transformer = [b for b in self.blocks if b.type == "transformer"]
        sink = [b for b in self.blocks if b.type == "sink"]

        assert len(source) == 1
        assert len(transformer) == 1
        assert len(sink) == 1

        assert source[0].next_block_id == transformer[0].id
        assert transformer[0].next_block_id == sink[0].id
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


class BaseBlock(BaseModel):
    id: str
    name: str
    type: str


class TransformerBlock(BaseBlock):
    type: Literal["transformer"]
    requirements: Requirements
    transformation: Transformation
    next_block_id: str
    env_vars: list[EnvironmentVariable]


class SourceBlock(BaseBlock):
    type: Literal["source"]
    next_block_id: str
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


class SinkBlock(BaseBlock):
    type: Literal["sink"]
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


Block = Annotated[
    Union[TransformerBlock, SourceBlock, SinkBlock], Field(discriminator="type")
]
