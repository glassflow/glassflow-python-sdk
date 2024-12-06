from __future__ import annotations

import dataclasses
from enum import Enum

from ..api import CreateSpace, SpaceScope
from .base import BaseManagementRequest, BaseResponse, BaseSpaceManagementDataRequest
from ...utils import generate_metadata_for_query_parameters


@dataclasses.dataclass
class ListSpacesResponse(BaseResponse):
    total_amount: int
    spaces: list[SpaceScope]


class Order(str, Enum):
    asc = "asc"
    desc = "desc"


@dataclasses.dataclass
class ListSpacesRequest(BaseManagementRequest):
    page_size: int = dataclasses.field(
        default=50,
        metadata=generate_metadata_for_query_parameters("page_size"),
    )
    page: int = dataclasses.field(
        default=1,
        metadata=generate_metadata_for_query_parameters("page"),
    )
    order_by: Order = dataclasses.field(
        default=Order.asc,
        metadata=generate_metadata_for_query_parameters("order_by"),
    )


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
