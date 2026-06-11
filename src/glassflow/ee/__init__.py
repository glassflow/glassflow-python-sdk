"""
GlassFlow Enterprise SDK.

Drop-in superset of :mod:`glassflow.etl` that adds Enterprise-only capabilities.
Use it exactly like the OSS client::

    from glassflow.ee import Client

    client = Client(host="https://...")
    pipeline = client.get_pipeline("my-pipeline")

All open-source models are re-exported from :mod:`glassflow.etl` for
convenience, so a single import path covers both tiers.
"""

from glassflow.etl.models import (
    JoinConfig,
    PipelineConfig,
    SinkConfig,
    SourceConfig,
)

from .client import Client
from .pipeline import Pipeline

__all__ = [
    "Pipeline",
    "Client",
    "PipelineConfig",
    "SourceConfig",
    "SinkConfig",
    "JoinConfig",
]
