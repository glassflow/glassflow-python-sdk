from pydantic import BaseModel


class FilterTransformConfig(BaseModel):
    """Config block for a filter transform entry."""

    expression: str
