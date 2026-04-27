from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field

from ..base import CaseInsensitiveStrEnum
from .deduplication import DedupTransformConfig, DedupTransformConfigPatch
from .filter import FilterTransformConfig
from .join import (
    JoinConfig,
    JoinConfigPatch,
    JoinOutputField,
    JoinSourceConfig,
    JoinType,
)
from .stateless import (
    StatelessTransformConfig,
    Transformation,
)


class TransformType(CaseInsensitiveStrEnum):
    DEDUP = "dedup"
    FILTER = "filter"
    STATELESS = "stateless"


class DedupTransform(BaseModel):
    """A dedup transform entry in the transforms list."""

    type: Literal[TransformType.DEDUP] = TransformType.DEDUP
    source_id: str
    config: DedupTransformConfig


class FilterTransform(BaseModel):
    """A filter transform entry in the transforms list."""

    type: Literal[TransformType.FILTER] = TransformType.FILTER
    source_id: str
    config: FilterTransformConfig


class StatelessTransform(BaseModel):
    """A stateless transform entry in the transforms list."""

    type: Literal[TransformType.STATELESS] = TransformType.STATELESS
    source_id: str
    config: StatelessTransformConfig


TransformEntry = Annotated[
    Union[DedupTransform, FilterTransform, StatelessTransform],
    Field(discriminator="type"),
]


__all__ = [
    # Transform types
    "TransformType",
    "TransformEntry",
    "DedupTransform",
    "FilterTransform",
    "StatelessTransform",
    # Config models
    "DedupTransformConfig",
    "DedupTransformConfigPatch",
    "FilterTransformConfig",
    "StatelessTransformConfig",
    "Transformation",
    # Join
    "JoinConfig",
    "JoinConfigPatch",
    "JoinOutputField",
    "JoinSourceConfig",
    "JoinType",
]
