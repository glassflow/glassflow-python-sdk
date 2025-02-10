from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field
from requests import Response


class BaseResponse(BaseModel):
    content_type: str | None = None
    status_code: int | None = None
    raw_response: Response | None = Field(...)

    model_config = ConfigDict(arbitrary_types_allowed=True)
