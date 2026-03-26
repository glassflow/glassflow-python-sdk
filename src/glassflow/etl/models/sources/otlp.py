"""OTLP source models."""

from typing import Literal, Optional

from pydantic import Field

from ..source import SourceBaseConfig, SourceBaseConfigPatch, SourceType
from ..transforms.deduplication import DeduplicationConfig


class OTLPSource(SourceBaseConfig):
    """Base class for all OTLP source types (V3).

    Use OTLPLogsSource, OTLPMetricsSource, or OTLPTracesSource for construction.
    isinstance(source, OTLPSource) returns True for all three concrete types.
    """

    id: str  # required for OTLP (overrides Optional[str] from SourceBaseConfig)
    deduplication: Optional[DeduplicationConfig] = Field(default=None)


class OTLPLogsSource(OTLPSource):
    """OTLP logs source (type: otlp.logs)."""

    type: Literal[SourceType.OTLP_LOGS] = SourceType.OTLP_LOGS


class OTLPMetricsSource(OTLPSource):
    """OTLP metrics source (type: otlp.metrics)."""

    type: Literal[SourceType.OTLP_METRICS] = SourceType.OTLP_METRICS


class OTLPTracesSource(OTLPSource):
    """OTLP traces source (type: otlp.traces)."""

    type: Literal[SourceType.OTLP_TRACES] = SourceType.OTLP_TRACES


class OTLPSourcePatch(SourceBaseConfigPatch):
    pass
