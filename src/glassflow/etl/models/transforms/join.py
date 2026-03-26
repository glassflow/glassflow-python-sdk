from typing import List, Optional

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from ..base import CaseInsensitiveStrEnum


class JoinOrientation(CaseInsensitiveStrEnum):
    LEFT = "left"
    RIGHT = "right"


class JoinField(BaseModel):
    """Selects a single field from a join source to include in the output (V3)."""

    source_id: str
    name: str
    output_name: Optional[str] = Field(default=None)


class JoinSourceConfig(BaseModel):
    source_id: str
    key: str
    time_window: str
    orientation: JoinOrientation


class JoinType(CaseInsensitiveStrEnum):
    TEMPORAL = "temporal"


class JoinConfig(BaseModel):
    """Configuration for joining multiple sources."""

    enabled: bool = False
    id: Optional[str] = None
    type: Optional[JoinType] = None
    sources: Optional[List[JoinSourceConfig]] = None
    # V3: optional field selection for join output
    fields: Optional[List[JoinField]] = Field(default=None)

    @field_validator("sources")
    @classmethod
    def validate_sources(
        cls, v: Optional[List[JoinSourceConfig]], info: ValidationInfo
    ) -> Optional[List[JoinSourceConfig]]:
        """
        Validate that when join is enabled, there are exactly two sources
        with opposite orientations.
        """
        if not info.data.get("enabled", False):
            return v

        if not v:
            raise ValueError("sources are required when join is enabled")

        if len(v) != 2:
            raise ValueError("join must have exactly two sources when enabled")

        orientations = {source.orientation for source in v}
        if orientations != {JoinOrientation.LEFT, JoinOrientation.RIGHT}:
            raise ValueError(
                "join sources must have opposite orientations (one LEFT and one RIGHT)"
            )

        return v

    @field_validator("type")
    @classmethod
    def validate_type(
        cls, v: Optional[JoinType], info: ValidationInfo
    ) -> Optional[JoinType]:
        """Validate that type is required when join is enabled."""
        if info.data.get("enabled", False) and not v:
            raise ValueError("type is required when join is enabled")
        return v

    @field_validator("fields")
    @classmethod
    def validate_fields(
        cls, v: Optional[List[JoinField]], info: ValidationInfo
    ) -> Optional[List[JoinField]]:
        """Validate that type is required when join is enabled."""
        if info.data.get("enabled", False) and not v:
            raise ValueError("fields is required when join is enabled")
        return v

    def update(self, patch: "JoinConfigPatch") -> "JoinConfig":
        """Apply a patch to this join config."""
        update_dict = self.model_copy(deep=True)

        if patch.enabled is not None:
            update_dict.enabled = patch.enabled
        if patch.id is not None:
            update_dict.id = patch.id
        if patch.type is not None:
            update_dict.type = patch.type
        if patch.sources is not None:
            update_dict.sources = patch.sources
        if patch.fields is not None:
            update_dict.fields = patch.fields

        return update_dict


class JoinConfigPatch(BaseModel):
    enabled: Optional[bool] = Field(default=None)
    id: Optional[str] = Field(default=None)
    type: Optional[JoinType] = Field(default=None)
    sources: Optional[List[JoinSourceConfig]] = Field(default=None)
    fields: Optional[List[JoinField]] = Field(default=None)
