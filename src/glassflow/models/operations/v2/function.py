from typing import Optional

from src.glassflow.models.api.v2 import ConsumeOutputEvent

from .base import BaseResponse


class TestFunctionResponse(BaseResponse):
    body: Optional[ConsumeOutputEvent] = None
