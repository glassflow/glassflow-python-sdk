"""Source models package.

SourceConfig is a Pydantic discriminated union that routes to the correct
concrete class based on the `type` field:
  - "kafka"        -> KafkaSource
  - "otlp.logs"    -> OTLPLogsSource
  - "otlp.metrics" -> OTLPMetricsSource
  - "otlp.traces"  -> OTLPTracesSource

Use isinstance(source, OTLPSource) to check for any OTLP type.
"""

from typing import Annotated, Union

from pydantic import Field  # noqa: F401

from ..registry import register_source
from ..source import SourceBaseConfig, SourceBaseConfigPatch, SourceType
from .kafka import (
    ConsumerGroupOffset,
    KafkaConnectionParams,
    KafkaConnectionParamsPatch,
    KafkaField,
    KafkaFormat,
    KafkaMechanism,
    KafkaProtocol,
    KafkaSchema,
    KafkaSource,
    KafkaSourcePatch,
    SchemaRegistry,
)
from .otlp import (
    OTLPLogsSource,
    OTLPMetricsSource,
    OTLPSource,
    OTLPSourcePatch,
    OTLPTracesSource,
)

# Discriminated union -- kept as a convenience type alias for the OSS source
# set. The PipelineConfig.sources field no longer uses it directly; instead it
# dispatches via the source registry so editions can add new source types
# without redefining this union.
SourceConfig = Annotated[
    Union[KafkaSource, OTLPLogsSource, OTLPMetricsSource, OTLPTracesSource],
    Field(discriminator="type"),
]

SourceConfigPatch = Union[KafkaSourcePatch, OTLPSourcePatch]

# Convenience alias
AnySource = SourceConfig
AnySourcePatch = SourceConfigPatch

# Register the OSS source types so the registry-backed dispatch can resolve them.
for _source_cls in (
    KafkaSource,
    OTLPLogsSource,
    OTLPMetricsSource,
    OTLPTracesSource,
):
    register_source(_source_cls)

__all__ = [
    # Base
    "SourceType",
    "SourceBaseConfig",
    "SourceBaseConfigPatch",
    # Kafka
    "ConsumerGroupOffset",
    "KafkaConnectionParams",
    "KafkaConnectionParamsPatch",
    "KafkaField",
    "KafkaFormat",
    "KafkaMechanism",
    "KafkaProtocol",
    "KafkaSchema",
    "KafkaSource",
    "KafkaSourcePatch",
    "SchemaRegistry",
    # OTLP
    "OTLPLogsSource",
    "OTLPMetricsSource",
    "OTLPSource",
    "OTLPSourcePatch",
    "OTLPTracesSource",
    # Union
    "AnySource",
    "SourceConfig",
    "SourceConfigPatch",
    "AnySourcePatch",
]
