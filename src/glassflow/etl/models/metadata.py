from typing import List

from pydantic import BaseModel, Field


class MetadataConfig(BaseModel):
    tags: List[str] = Field(default=[])
