"""Source models package.

SourceConfig is a Pydantic discriminated union that routes to the correct
concrete class based on the `type` field:
  - "kafka"        → KafkaSource
  - "otlp.logs"    → OTLPLogsSource
  - "otlp.metrics" → OTLPMetricsSource
  - "otlp.traces"  → OTLPTracesSource

Use isinstance(source, OTLPSource) to check for any OTLP type.
"""

from typing import Annotated, Union

from pydantic import Field  # noqa: F401

from ..source import SourceBaseConfig, SourceBaseConfigPatch, SourceType
from .kafka import (
    ConsumerGroupOffset,
    DeduplicationConfig,
    KafkaConnectionParams,
    KafkaConnectionParamsPatch,
    KafkaField,
    KafkaMechanism,
    KafkaProtocol,
    KafkaSource,
    KafkaSourcePatch,
    SchemaRegistry,
    TopicConfig,
)
from .otlp import (
    OTLPLogsSource,
    OTLPMetricsSource,
    OTLPSource,
    OTLPSourcePatch,
    OTLPTracesSource,
)

# Discriminated union — Pydantic resolves the concrete class via the `type` field.
SourceConfig = Annotated[
    Union[KafkaSource, OTLPLogsSource, OTLPMetricsSource, OTLPTracesSource],
    Field(discriminator="type"),
]

SourceConfigPatch = Union[KafkaSourcePatch, OTLPSourcePatch]

# Convenience alias
AnySource = SourceConfig
AnySourcePatch = SourceConfigPatch

__all__ = [
    # Base
    "SourceType",
    "SourceBaseConfig",
    "SourceBaseConfigPatch",
    # Kafka
    "ConsumerGroupOffset",
    "DeduplicationConfig",
    "KafkaConnectionParams",
    "KafkaConnectionParamsPatch",
    "KafkaField",
    "KafkaMechanism",
    "KafkaProtocol",
    "KafkaSource",
    "KafkaSourcePatch",
    "SchemaRegistry",
    "TopicConfig",
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
