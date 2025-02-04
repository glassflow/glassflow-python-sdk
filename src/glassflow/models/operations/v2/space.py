
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from .base import BaseResponse
from src.glassflow.models.api.v2 import Space


class CreateSpaceResponse(BaseResponse):
    body: Optional[Space] = None