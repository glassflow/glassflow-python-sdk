from typing import Optional

from pydantic import BaseModel, Field, model_validator


class DedupTransformConfig(BaseModel):
    """Config block for a dedup transform entry."""

    key: str
    time_window: str

    @model_validator(mode="before")
    @classmethod
    def normalize_fields(cls, values):
        if not isinstance(values, dict):
            return values
        if not values.get("key"):
            raise ValueError("key is required for dedup transform config")
        if not values.get("time_window"):
            raise ValueError("time_window is required for dedup transform config")
        return values


class DedupTransformConfigPatch(BaseModel):
    key: Optional[str] = Field(default=None)
    time_window: Optional[str] = Field(default=None)
