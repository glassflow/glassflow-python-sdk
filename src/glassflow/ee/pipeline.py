from __future__ import annotations

from glassflow.etl.pipeline import Pipeline as _OSSPipeline

from .dlq import DLQ


class Pipeline(_OSSPipeline):
    """Enterprise Pipeline.

    Extends the open-source :class:`glassflow.etl.pipeline.Pipeline`. Its ``dlq``
    property exposes the Enterprise :class:`~.dlq.DLQ` (with
    ``list``/``reprocess``/``discard``). Construction is inherited unchanged;
    only the DLQ collaborator class is swapped via ``_dlq_class``.
    """

    _dlq_class = DLQ

    @property
    def dlq(self) -> DLQ:
        """Get the Enterprise DLQ client for this pipeline."""
        return self._dlq

    @dlq.setter
    def dlq(self, dlq: DLQ) -> None:
        self._dlq = dlq
