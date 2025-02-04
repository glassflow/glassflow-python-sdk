"""Pydantic models for publish event operation"""
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict
from .base import BaseResponse


class PublishEventResponseBody(BaseModel):
    """Message pushed to the pipeline."""
    pass


class PublishEventResponse(BaseResponse):
    pass
