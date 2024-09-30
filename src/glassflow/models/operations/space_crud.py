from __future__ import annotations

import dataclasses
from enum import Enum

from ..api import CreateSpace, SpaceScope
from .base import BaseManagementRequest, BaseResponse, BaseSpaceManagementDataRequest


@dataclasses.dataclass
class ListSpacesResponse(BaseResponse):
    total_amount: int
    spaces: list[SpaceScope]


class Order(str, Enum):
    asc = "asc"
    desc = "desc"


@dataclasses.dataclass
class ListSpacesRequest(BaseManagementRequest):
    page_size: int = 50
    page: int = 1
    order_by: Order = Order.asc


@dataclasses.dataclass
class CreateSpaceRequest(BaseManagementRequest, CreateSpace):
    pass


@dataclasses.dataclass
class CreateSpaceResponse(BaseResponse):
    name: str
    id: str
    created_at: str


@dataclasses.dataclass
class DeleteSpaceRequest(BaseSpaceManagementDataRequest):
    pass
