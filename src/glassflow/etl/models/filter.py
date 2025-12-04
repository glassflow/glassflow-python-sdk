from typing import Any, Optional

from pydantic import BaseModel, Field


class FilterConfig(BaseModel):
    enabled: bool = Field(default=False)
    expression: Optional[str] = Field(default=None, description="The filter expression")

    def update(self, patch: "FilterConfigPatch") -> "FilterConfig":
        """Apply a patch to this filter config."""
        update_dict: dict[str, Any] = {}

        # Check each field explicitly to handle model instances properly
        if patch.enabled is not None:
            update_dict["enabled"] = patch.enabled
        if patch.expression is not None:
            update_dict["expression"] = patch.expression

        if update_dict:
            return self.model_copy(update=update_dict)
        return self


class FilterConfigPatch(BaseModel):
    enabled: Optional[bool] = Field(default=None)
    expression: Optional[str] = Field(default=None)
