from typing import Optional

from pydantic import BaseModel, Field

from .base import CaseInsensitiveStrEnum


class SourceType(CaseInsensitiveStrEnum):
    KAFKA = "kafka"
    OTLP_LOGS = "otlp.logs"
    OTLP_METRICS = "otlp.metrics"
    OTLP_TRACES = "otlp.traces"


class SourceBaseConfig(BaseModel):
    """Base source config for all source types."""

    type: Optional[SourceType] = Field(default=None)
    source_id: str


class SourceBaseConfigPatch(BaseModel):
    """Patch model source config."""

    type: Optional[SourceType] = Field(default=None)
    source_id: Optional[str] = Field(default=None)
