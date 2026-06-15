from __future__ import annotations

import warnings
from typing import Any, Dict, Iterator, List, Optional

from glassflow.etl import errors
from glassflow.etl.dlq import DLQ as _OSSDLQ

# Mirrors the backend cap on message_ids per mode=selected request
# (dlqSelectedMaxMessageIDs in glassflow-etl-ee). Validated client-side for a
# fast, offline error instead of a round-trip 400.
MAX_SELECTED_MESSAGE_IDS = 1000

# Data-plane components a DLQ message can come from (DataPlaneRoles in
# glassflow-etl-ee). Used to validate the list() component filter client-side.
DLQ_COMPONENTS = ("ingestor", "join", "sink", "dedup", "oltp-receiver")


class DLQ(_OSSDLQ):
    """Enterprise Dead Letter Queue client.

    Extends the open-source :class:`glassflow.etl.dlq.DLQ` with message
    management: a non-destructive paginated :meth:`list`, and
    :meth:`reprocess`/:meth:`discard` (plus their ``*_all`` variants) that move
    messages back into the pipeline or permanently remove them.

    Backend entitlement is enforced server-side; calling these against a backend
    that is not licensed for them raises :class:`FeatureNotLicensedError`.
    """

    def list(
        self,
        batch_size: int = 100,
        cursor: Optional[str] = None,
        component: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Read messages from the DLQ without removing them.

        The non-destructive successor to :meth:`consume`. Each message in the
        returned page carries a stable ``message_id`` (NATS sequence number, as
        a string) plus ``component``, ``error``, ``original_message`` and
        ``received_at``. The ``message_id`` values are what :meth:`reprocess`
        and :meth:`discard` act on in ``mode=selected``.

        Args:
            batch_size: Number of messages per page (between 1 and 1000).
            cursor: NATS sequence to resume from, taken from the previous page's
                ``next_cursor``; omit for the first page.
            component: Filter to messages from a single data-plane component;
                must be one of :data:`DLQ_COMPONENTS` (``ingestor``, ``join``,
                ``sink``, ``dedup``, ``oltp-receiver``); omit for all components.

        Returns:
            A dict with ``messages`` (list of message dicts), ``has_more``
            (bool), and ``next_cursor`` (str, present when ``has_more`` is
            true; pass it back as ``cursor`` to fetch the next page).

        Raises:
            ValueError: If ``batch_size`` or ``component`` is invalid.
            APIError: If the API request fails.
        """
        if (
            not isinstance(batch_size, int)
            or batch_size < 1
            or batch_size > MAX_SELECTED_MESSAGE_IDS
        ):
            raise ValueError(
                f"batch_size must be an integer between 1 and "
                f"{MAX_SELECTED_MESSAGE_IDS}"
            )

        if component is not None and component not in DLQ_COMPONENTS:
            raise ValueError(f"component must be one of {', '.join(DLQ_COMPONENTS)}")

        params: Dict[str, Any] = {"batch_size": batch_size}
        if cursor is not None:
            params["cursor"] = cursor
        if component is not None:
            params["component"] = component

        response = self._request("GET", f"{self.endpoint}/list", params=params)
        if response.status_code == 204 or not response.content:
            return {"messages": [], "has_more": False}
        return response.json()

    def list_iter(
        self,
        batch_size: int = 100,
        component: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Lazily iterate over every DLQ message, paging via the cursor for you.

        The streaming companion to :meth:`list`: it calls :meth:`list` page by
        page and yields each message, so callers do not manage the cursor
        themselves. Memory stays flat (one page at a time) and it composes with
        ``itertools`` (e.g. ``itertools.islice`` for the first N).

        Args:
            batch_size: Messages fetched per underlying page request (1-1000).
            component: Optional component filter; see :meth:`list`.
            cursor: Optional starting cursor (e.g. to resume a previous run);
                omit to start at the beginning of the DLQ.

        Yields:
            Individual DLQ message dicts.
        """
        while True:
            page = self.list(batch_size=batch_size, cursor=cursor, component=component)
            yield from page.get("messages", [])
            if not page.get("has_more"):
                return
            cursor = page.get("next_cursor")
            # Defensive: a truthy has_more without a cursor would loop forever.
            if not cursor:
                return

    def reprocess(self, message_ids: List[str]) -> Dict[str, Any]:
        """Move specific messages from the DLQ back into the pipeline input.

        Args:
            message_ids: ``message_id`` values (from :meth:`list`) to reprocess;
                must be non-empty and at most ``MAX_SELECTED_MESSAGE_IDS``.

        Returns:
            Dict with ``request_id`` and ``status`` ("accepted"). The republish
            happens asynchronously.

        Raises:
            ValueError: If ``message_ids`` is empty or too large.
            FeatureNotLicensedError: If the backend is not licensed for this.
            PipelineNotRunningError: If the pipeline is not in the Running state.
            APIError: If the API request fails.
        """
        return self._action("reprocess", "selected", message_ids)

    def reprocess_all(self) -> Dict[str, Any]:
        """Reprocess every message currently in the DLQ (up to the head at
        request time). See :meth:`reprocess`."""
        return self._action("reprocess", "all", None)

    def discard(self, message_ids: List[str]) -> Dict[str, Any]:
        """Permanently remove specific messages from the DLQ without
        reprocessing them.

        Args:
            message_ids: ``message_id`` values (from :meth:`list`) to discard;
                must be non-empty and at most ``MAX_SELECTED_MESSAGE_IDS``.

        Returns:
            Dict with ``request_id`` and ``discarded_count``.

        Raises:
            ValueError: If ``message_ids`` is empty or too large.
            FeatureNotLicensedError: If the backend is not licensed for this.
            APIError: If the API request fails.
        """
        return self._action("discard", "selected", message_ids)

    def discard_all(self) -> Dict[str, Any]:
        """Permanently remove every message currently in the DLQ. See
        :meth:`discard`."""
        return self._action("discard", "all", None)

    # Deprecated, inherited operations -------------------------------------

    def consume(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        """Deprecated: use :meth:`list`.

        The ``/dlq/consume`` endpoint is being removed in favour of the
        non-destructive ``/dlq/list``. This override delegates to :meth:`list`
        so existing callers keep working through the transition.
        """
        warnings.warn(
            "DLQ.consume() is deprecated and the /dlq/consume endpoint is being "
            "removed; use DLQ.list() (non-destructive).",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.list(batch_size=batch_size).get("messages", [])

    def purge(self) -> None:
        """Deprecated: use :meth:`discard_all`.

        ``/dlq/purge`` remains for backward compatibility but is superseded by
        :meth:`discard_all`.
        """
        warnings.warn(
            "DLQ.purge() is deprecated; use DLQ.discard_all().",
            DeprecationWarning,
            stacklevel=2,
        )
        super().purge()

    def _action(
        self, action: str, mode: str, message_ids: Optional[List[str]]
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {"mode": mode}
        if mode == "selected":
            if not message_ids:
                raise ValueError(
                    "message_ids must be non-empty when selecting messages"
                )
            if len(message_ids) > MAX_SELECTED_MESSAGE_IDS:
                raise ValueError(
                    f"message_ids cannot exceed {MAX_SELECTED_MESSAGE_IDS} entries"
                )
            body["message_ids"] = message_ids

        try:
            response = self._request("POST", f"{self.endpoint}/{action}", json=body)
            if response.status_code == 204 or not response.content:
                return {}
            return response.json()
        except errors.ForbiddenError as e:
            raise errors.FeatureNotLicensedError(
                status_code=e.status_code,
                message=(f"DLQ {action} requires a GlassFlow Enterprise license"),
                response=e.response,
            ) from e
        except errors.ConflictError as e:
            # Reprocess replays through the running pipeline, so the API rejects
            # it with 409 when the pipeline is not in the Running state. Discard
            # acts on the queue directly and has no such constraint.
            if action == "reprocess":
                raise errors.PipelineNotRunningError(
                    status_code=e.status_code,
                    message=(
                        "Pipeline must be in the Running state to reprocess DLQ "
                        "messages"
                    ),
                    response=e.response,
                ) from e
            raise
