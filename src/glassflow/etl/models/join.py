from typing import Any, List, Optional

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

    def update(self, patch: "JoinConfigPatch") -> "JoinConfig":
        """Apply a patch to this join config."""
        update_dict: dict[str, Any] = {}

        if patch.enabled is not None:
            update_dict["enabled"] = patch.enabled
        if patch.type is not None:
            update_dict["type"] = patch.type
        if patch.sources is not None:
            update_dict["sources"] = patch.sources

        if update_dict:
            return self.model_copy(update=update_dict)
        return self


class JoinConfigPatch(BaseModel):
    enabled: Optional[bool] = Field(default=None)
    type: Optional[JoinType] = Field(default=None)
    sources: Optional[List[JoinSourceConfig]] = Field(default=None)
