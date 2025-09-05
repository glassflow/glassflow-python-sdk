import os
from unittest.mock import patch

from glassflow.etl.tracking import Tracking


class TestTracking:
    """Tests for the Tracking class."""

    def test_tracking_disabled(self, mock_track):
        """Test that tracking is not called when disabled."""
        with patch.dict(os.environ, {"GF_TRACKING_ENABLED": "false"}):
            tracking = Tracking(distinct_id="distinct-id")
            assert not tracking.enabled

            tracking.track_event("test_event", {"test": "data"})
            mock_track.assert_not_called()

    def test_tracking_enabled(self, mock_track):
        """Test that tracking is called with correct data when enabled."""
        with patch.dict(os.environ, {"GF_TRACKING_ENABLED": "true"}):
            tracking = Tracking(distinct_id="distinct-id")
            assert tracking.enabled

            tracking.track_event("test_event", {"test": "data"})
            mock_track.assert_called_once_with(
                distinct_id=tracking._distinct_id,
                event_name="test_event",
                properties={
                    "sdk_version": tracking.sdk_version,
                    "platform": tracking.platform,
                    "python_version": tracking.python_version,
                    "test": "data",
                },
            )

    def test_tracking_enabled_no_properties(self, mock_track):
        """
        Test that tracking is called with only base properties when no
        additional properties are provided.
        """
        with patch.dict(os.environ, {"GF_TRACKING_ENABLED": "true"}):
            tracking = Tracking(distinct_id="distinct-id")
            assert tracking.enabled

            tracking.track_event("test_event")
            mock_track.assert_called_once_with(
                distinct_id=tracking._distinct_id,
                event_name="test_event",
                properties={
                    "sdk_version": tracking.sdk_version,
                    "platform": tracking.platform,
                    "python_version": tracking.python_version,
                },
            )

    def test_tracking_error_handling(self, mock_track):
        """Test that tracking errors are handled gracefully."""
        with patch.dict(os.environ, {"GF_TRACKING_ENABLED": "true"}):
            tracking = Tracking(distinct_id="distinct-id")
            assert tracking.enabled

            mock_track.side_effect = Exception("Test error")
            # Should not raise an exception
            tracking.track_event("test_event", {"test": "data"})
