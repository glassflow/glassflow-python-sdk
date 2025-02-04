from typing import Optional
from pydantic import BaseModel, Field, ConfigDict
from requests import Response

class BaseResponse(BaseModel):
    content_type: Optional[str] = None
    status_code: Optional[int] = None
    raw_response: Optional[Response] = Field(...)

    model_config = ConfigDict(arbitrary_types_allowed=True)
