from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator


class DeduplicationConfig(BaseModel):
    """Deduplication configuration shared across all source types."""

    enabled: bool = False
    key: Optional[str] = Field(default=None)
    time_window: Optional[str] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def normalize_fields(cls, values):
        if not isinstance(values, dict):
            return values

        enabled = values.get("enabled", False)

        if not enabled:
            if values.get("key") == "":
                values["key"] = None
            if values.get("time_window") == "":
                values["time_window"] = None
        else:
            if not values.get("key"):
                raise ValueError("key is required when deduplication is enabled")
            if not values.get("time_window"):
                raise ValueError(
                    "time_window is required when deduplication is enabled"
                )

        return values

    def update(self, patch: "DeduplicationConfigPatch") -> "DeduplicationConfig":
        update_dict: dict[str, Any] = {}
        fields_set = (
            patch.model_fields_set if hasattr(patch, "model_fields_set") else set()
        )

        if "enabled" in fields_set or patch.enabled is not None:
            update_dict["enabled"] = patch.enabled
        if "key" in fields_set:
            update_dict["key"] = patch.key
        if "time_window" in fields_set:
            update_dict["time_window"] = patch.time_window

        if update_dict:
            return self.model_copy(update=update_dict)
        return self


class DeduplicationConfigPatch(BaseModel):
    enabled: Optional[bool] = Field(default=None)
    key: Optional[str] = Field(default=None)
    time_window: Optional[str] = Field(default=None)
