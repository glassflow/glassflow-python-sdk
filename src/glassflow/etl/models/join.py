from typing import List, Optional

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from .base import CaseInsensitiveStrEnum


class JoinOrientation(CaseInsensitiveStrEnum):
    LEFT = "left"
    RIGHT = "right"


class JoinSourceConfig(BaseModel):
    source_id: str
    join_key: str
    time_window: str
    orientation: JoinOrientation


class JoinType(CaseInsensitiveStrEnum):
    TEMPORAL = "temporal"


class JoinConfig(BaseModel):
    """Configuration for joining multiple sources."""

    enabled: bool = False
    type: Optional[JoinType] = None
    sources: Optional[List[JoinSourceConfig]] = None

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


class JoinSourceConfigPatch(BaseModel):
    source_id: Optional[str] = Field(default=None)
    join_key: Optional[str] = Field(default=None)
    time_window: Optional[str] = Field(default=None)
    orientation: Optional[JoinOrientation] = Field(default=None)


class JoinConfigPatch(BaseModel):
    enabled: Optional[bool] = Field(default=None)
    type: Optional[JoinType] = Field(default=None)
    # TODO: How to patch an element in a list?
    sources: Optional[List[JoinSourceConfig]] = Field(default=None)
