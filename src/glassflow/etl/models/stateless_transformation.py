from typing import Any, List, Optional

from pydantic import BaseModel, Field, model_validator

from .base import CaseInsensitiveStrEnum


class StatelessTransformationType(CaseInsensitiveStrEnum):
    EXPRESSION = "expr_lang_transform"


class Transformation(BaseModel):
    expression: str = Field(description="The transformation expression")
    output_name: str = Field(description="The name of the output column")
    output_type: str = Field(description="The type of the output column")


class ExpressionConfig(BaseModel):
    transform: List[Transformation] = Field(description="The transformation expression")


class StatelessTransformationConfig(BaseModel):
    enabled: bool = Field(
        description="Whether the stateless transformation is enabled",
        default=False,
    )
    id: Optional[str] = Field(
        description="The ID of the stateless transformation", default=None
    )
    type: Optional[StatelessTransformationType] = Field(
        description="The type of the stateless transformation",
        default=StatelessTransformationType.EXPRESSION,
    )
    config: Optional[ExpressionConfig] = Field(
        description="The configuration of the stateless transformation", default=None
    )

    @model_validator(mode="after")
    def validate(self) -> "StatelessTransformationConfig":
        if self.enabled:
            if not self.id:
                raise ValueError(
                    "id is required when stateless transformation is enabled"
                )
            if not self.type:
                raise ValueError(
                    "type is required when stateless transformation is enabled"
                )
            if not self.config:
                raise ValueError(
                    "config is required when stateless transformation is enabled"
                )
        return self

    def update(
        self, patch: "StatelessTransformationConfigPatch"
    ) -> "StatelessTransformationConfig":
        """Apply a patch to this stateless transformation config."""
        update_dict: dict[str, Any] = {}

        if patch.enabled is not None:
            update_dict["enabled"] = patch.enabled
        if patch.id is not None:
            update_dict["id"] = patch.id
        if patch.type is not None:
            update_dict["type"] = patch.type
        if patch.config is not None:
            update_dict["config"] = patch.config

        if update_dict:
            return self.model_copy(update=update_dict)

        return self


class StatelessTransformationConfigPatch(BaseModel):
    enabled: Optional[bool] = Field(default=None)
    id: Optional[str] = Field(default=None)
    type: Optional[StatelessTransformationType] = Field(default=None)
    config: Optional[ExpressionConfig] = Field(default=None)
