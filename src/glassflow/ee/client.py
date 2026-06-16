from __future__ import annotations

from glassflow.etl.client import Client as _OSSClient

from .pipeline import Pipeline


class Client(_OSSClient):
    """Enterprise GlassFlow client.

    Extends the open-source :class:`glassflow.etl.client.Client`. Every pipeline
    it returns is the Enterprise :class:`~.pipeline.Pipeline`, giving
    Enterprise-only capabilities a home as they are added. Backend entitlement
    is enforced server-side.
    """

    _pipeline_class = Pipeline
