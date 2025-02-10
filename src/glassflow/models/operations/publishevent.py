"""Pydantic models for publish event operation"""

from pydantic import BaseModel

from .base import BaseResponse


class PublishEventResponseBody(BaseModel):
    """Message pushed to the pipeline."""

    pass


class PublishEventResponse(BaseResponse):
    pass
