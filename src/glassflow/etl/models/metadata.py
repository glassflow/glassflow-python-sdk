from typing import List, Optional

from pydantic import BaseModel, Field


class MetadataConfig(BaseModel):
    tags: Optional[List[str]] = Field(default=None)
