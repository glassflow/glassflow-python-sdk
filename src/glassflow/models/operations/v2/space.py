from typing import Optional

from src.glassflow.models.api.v2 import Space

from .base import BaseResponse


class CreateSpaceResponse(BaseResponse):
    body: Optional[Space] = None
