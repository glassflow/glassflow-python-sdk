from typing import Optional

from src.glassflow.models.api.v2 import ConsumeOutputEvent

from .base import BaseResponse


class ConsumeEventResponse(BaseResponse):
    body: Optional[ConsumeOutputEvent] = None

    def event(self):
        if self.body:
            return self.body["response"]
        return None


class ConsumeFailedResponse(BaseResponse):
    body: Optional[ConsumeOutputEvent] = None
