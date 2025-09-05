from __future__ import annotations

import os
import platform
from importlib.metadata import version
from typing import Any, Dict

import mixpanel


class Tracking:
    """Mixpanel tracking implementation for GlassFlow Clickhouse ETL."""

    def __init__(self, distinct_id: str) -> None:
        """Initialize the tracking client"""
        self.enabled = os.getenv("GF_TRACKING_ENABLED", "true").lower() == "true"
        self._project_token = "209670ec9b352915013a5dfdb169dd25"
        self._distinct_id = distinct_id
        self.client = mixpanel.Mixpanel(self._project_token)

        self.sdk_version = version("glassflow")
        self.platform = platform.system()
        self.python_version = platform.python_version()

    def track_event(
        self, event_name: str, properties: Dict[str, Any] | None = None
    ) -> None:
        """Track an event in Mixpanel.

        Args:
            event_name: Name of the event to track
            properties: Additional properties to include with the event
        """
        if not self.enabled:
            return

        base_properties = {
            "sdk_version": self.sdk_version,
            "platform": self.platform,
            "python_version": self.python_version,
        }
        if properties is None:
            properties = {}
        properties = {**base_properties, **properties}

        try:
            self.client.track(
                distinct_id=self._distinct_id,
                event_name=event_name,
                properties=properties,
            )
        except Exception:
            pass
