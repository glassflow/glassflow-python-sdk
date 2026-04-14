from typing import List

from pydantic import BaseModel, Field


class Transformation(BaseModel):
    expression: str = Field(description="The transformation expression")
    output_name: str = Field(description="The name of the output column")
    output_type: str = Field(description="The type of the output column")


class StatelessTransformConfig(BaseModel):
    """Config block for a stateless transform entry."""

    transforms: List[Transformation] = Field(
        description="The list of transformation expressions"
    )
