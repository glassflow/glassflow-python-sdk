"""OTLP source models."""

from typing import Literal

from ..source import SourceBaseConfig, SourceBaseConfigPatch, SourceType


class OTLPSource(SourceBaseConfig):
    """Base class for all OTLP source types.

    Use OTLPLogsSource, OTLPMetricsSource, or OTLPTracesSource for construction.
    isinstance(source, OTLPSource) returns True for all three concrete types.
    """


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
