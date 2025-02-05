from datetime import datetime
from typing import List

from pydantic import BaseModel


class Space(BaseModel):
    name: str
    id: str
    created_at: datetime
    permission: str


class ListSpacesResponse(BaseModel):
    total_amount: int
    spaces: List[Space]
