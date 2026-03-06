from typing import Optional

from pydantic import BaseModel, Field


class FilterConfig(BaseModel):
    enabled: bool = Field(default=False)
    expression: Optional[str] = Field(default=None, description="The filter expression")

    def update(self, patch: "FilterConfigPatch") -> "FilterConfig":
        """Apply a patch to this filter config."""
        update_dict = self.model_copy(deep=True)

        # Check each field explicitly to handle model instances properly
        if patch.enabled is not None:
            update_dict.enabled = patch.enabled
        if patch.expression is not None:
            update_dict.expression = patch.expression

        return update_dict


class FilterConfigPatch(BaseModel):
    enabled: Optional[bool] = Field(default=None)
    expression: Optional[str] = Field(default=None)
