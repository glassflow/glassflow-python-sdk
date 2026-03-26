from typing import List, Optional

from pydantic import BaseModel, Field, model_validator

from ..base import CaseInsensitiveStrEnum


class StatelessTransformationType(CaseInsensitiveStrEnum):
    EXPRESSION = "expr_lang_transform"


class Transformation(BaseModel):
    expression: str = Field(description="The transformation expression")
    output_name: str = Field(description="The name of the output column")
    output_type: str = Field(description="The type of the output column")


class ExpressionConfig(BaseModel):
    transform: Optional[List[Transformation]] = Field(
        description="The transformation expression", default=None
    )


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
    # V3: the upstream source_id this transformation reads from
    source_id: Optional[str] = Field(
        description="The source_id this transformation reads from (V3)", default=None
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
            else:
                if not self.config.transform:
                    raise ValueError(
                        "transform is required when stateless transformation is enabled"
                    )
        return self

    def update(
        self, patch: "StatelessTransformationConfigPatch"
    ) -> "StatelessTransformationConfig":
        """Apply a patch to this stateless transformation config."""
        update_dict = self.model_copy(deep=True)

        if patch.enabled is not None:
            update_dict.enabled = patch.enabled
        if patch.id is not None:
            update_dict.id = patch.id
        if patch.type is not None:
            update_dict.type = patch.type
        if patch.source_id is not None:
            update_dict.source_id = patch.source_id
        if patch.config is not None:
            update_dict.config = patch.config

        return update_dict


class StatelessTransformationConfigPatch(BaseModel):
    enabled: Optional[bool] = Field(default=None)
    id: Optional[str] = Field(default=None)
    type: Optional[StatelessTransformationType] = Field(default=None)
    source_id: Optional[str] = Field(default=None)
    config: Optional[ExpressionConfig] = Field(default=None)
