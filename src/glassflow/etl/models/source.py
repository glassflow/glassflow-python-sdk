from typing import Optional

from pydantic import BaseModel, Field

from .base import CaseInsensitiveStrEnum


class SourceType(CaseInsensitiveStrEnum):
    KAFKA = "kafka"
    OTLP_LOGS = "otlp.logs"
    OTLP_METRICS = "otlp.metrics"
    OTLP_TRACES = "otlp.traces"


class SourceBaseConfig(BaseModel):
    """Source config."""

    type: Optional[SourceType] = Field(default=None)
    id: Optional[str] = Field(default=None)


class SourceBaseConfigPatch(BaseModel):
    """Patch model source config."""

    type: Optional[SourceType] = Field(default=None)
    id: Optional[str] = Field(default=None)
