from __future__ import annotations

from glassflow.etl.pipeline import Pipeline as _OSSPipeline


class Pipeline(_OSSPipeline):
    """Enterprise Pipeline.

    Extends the open-source :class:`glassflow.etl.pipeline.Pipeline`. Currently
    a pass-through; Enterprise-only pipeline capabilities are added here in
    follow-up work.
    """
