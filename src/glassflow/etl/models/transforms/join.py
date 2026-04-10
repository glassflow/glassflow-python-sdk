from typing import List, Optional

from pydantic import BaseModel, Field, model_validator

from ..base import CaseInsensitiveStrEnum


class JoinOutputField(BaseModel):
    """Selects a single field from a join source to include in the output."""

    source_id: str
    name: str
    output_name: Optional[str] = Field(default=None)


class JoinSourceConfig(BaseModel):
    """Configuration for one side of a join (left or right)."""

    source_id: str
    key: str
    time_window: str


class JoinType(CaseInsensitiveStrEnum):
    TEMPORAL = "temporal"


class JoinConfig(BaseModel):
    """Configuration for joining two sources."""

    enabled: bool = False
    type: Optional[JoinType] = Field(default=None)
    left_source: Optional[JoinSourceConfig] = Field(default=None)
    right_source: Optional[JoinSourceConfig] = Field(default=None)
    output_fields: Optional[List[JoinOutputField]] = Field(default=None)

    @model_validator(mode="after")
    def validate_enabled_fields(self) -> "JoinConfig":
        """Validate required fields when join is enabled."""
        if not self.enabled:
            return self
        if not self.type:
            raise ValueError("type is required when join is enabled")
        if not self.left_source:
            raise ValueError("left_source is required when join is enabled")
        if not self.right_source:
            raise ValueError("right_source is required when join is enabled")
        if not self.output_fields:
            raise ValueError("output_fields is required when join is enabled")
        return self

    def update(self, patch: "JoinConfigPatch") -> "JoinConfig":
        """Apply a patch to this join config."""
        update_dict = self.model_copy(deep=True)

        if patch.enabled is not None:
            update_dict.enabled = patch.enabled
        if patch.type is not None:
            update_dict.type = patch.type
        if patch.left_source is not None:
            update_dict.left_source = patch.left_source
        if patch.right_source is not None:
            update_dict.right_source = patch.right_source
        if patch.output_fields is not None:
            update_dict.output_fields = patch.output_fields

        return update_dict


class JoinConfigPatch(BaseModel):
    enabled: Optional[bool] = Field(default=None)
    type: Optional[JoinType] = Field(default=None)
    left_source: Optional[JoinSourceConfig] = Field(default=None)
    right_source: Optional[JoinSourceConfig] = Field(default=None)
    output_fields: Optional[List[JoinOutputField]] = Field(default=None)
