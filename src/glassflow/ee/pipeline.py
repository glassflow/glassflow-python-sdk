from __future__ import annotations

from typing import Any, Dict, List

from glassflow.etl import errors
from glassflow.etl.pipeline import Pipeline as _OSSPipeline

from .dlq import DLQ


class Pipeline(_OSSPipeline):
    """Enterprise Pipeline.

    Extends the open-source :class:`glassflow.etl.pipeline.Pipeline`. Its ``dlq``
    property exposes the Enterprise :class:`~.dlq.DLQ` (with
    ``list``/``reprocess``/``discard``), and it adds :meth:`get_streams`.
    Construction is inherited unchanged; only the DLQ collaborator class is
    swapped via ``_dlq_class``.
    """

    _dlq_class = DLQ

    @property
    def dlq(self) -> DLQ:
        """Get the Enterprise DLQ client for this pipeline."""
        return self._dlq

    @dlq.setter
    def dlq(self, dlq: DLQ) -> None:
        self._dlq = dlq

    def get_streams(self) -> List[Dict[str, Any]]:
        """Return the NATS JetStream streams backing this pipeline.

        Each entry has a ``stream_name`` and the ``component`` the stream belongs
        to (for example ``ingestor``, ``join``, ``sink``, ``dedup``, ``dlq``).
        Useful for diagnosing NATS-level issues.

        Returns:
            List of ``{"stream_name": ..., "component": ...}`` dicts.

        Raises:
            PipelineNotFoundError: If the pipeline does not exist.
            FeatureNotLicensedError: If the backend is not licensed for this.
            APIError: If the API request fails.
        """
        try:
            response = self._request(
                "GET",
                f"{self.ENDPOINT}/{self.pipeline_id}/streams",
                event_name="PipelineStreamsGet",
            )
        except errors.ForbiddenError as e:
            raise errors.FeatureNotLicensedError(
                status_code=e.status_code,
                message="Pipeline streams require a GlassFlow Enterprise license",
                response=e.response,
                details=e.details,
            ) from e
        if response.status_code == 204 or not response.content:
            return []
        return response.json().get("streams", [])
