import os
import uuid
from unittest.mock import mock_open, patch

from glassflow.etl.tracking import Tracking, _get_distinct_id


def test_tracking_disabled(mock_track):
    """Test that tracking is not called when disabled."""
    with patch.dict(os.environ, {"GF_TRACKING_ENABLED": "false"}):
        tracking = Tracking()
        assert not tracking.enabled

        tracking.track_event("test_event", {"test": "data"})
        mock_track.assert_not_called()


def test_tracking_enabled(mock_track):
    """Test that tracking is called with correct data when enabled."""
    with patch.dict(os.environ, {"GF_TRACKING_ENABLED": "true"}):
        tracking = Tracking()
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


def test_tracking_enabled_no_properties(mock_track):
    """
    Test that tracking is called with only base properties when no
    additional properties are provided.
    """
    with patch.dict(os.environ, {"GF_TRACKING_ENABLED": "true"}):
        tracking = Tracking()
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


def test_tracking_error_handling(mock_track):
    """Test that tracking errors are handled gracefully."""
    with patch.dict(os.environ, {"GF_TRACKING_ENABLED": "true"}):
        tracking = Tracking()
        assert tracking.enabled

        mock_track.side_effect = Exception("Test error")
        # Should not raise an exception
        tracking.track_event("test_event", {"test": "data"})


def test_get_distinct_id():
    """Test the _get_distinct_id function."""
    # Test case 1: New config file creation
    with (
        patch("os.path.exists") as mock_exists,
        patch("os.makedirs") as mock_makedirs,
        patch("builtins.open", mock_open()) as mock_file,
    ):
        mock_exists.return_value = False
        distinct_id = _get_distinct_id()

        mock_makedirs.assert_called_once()
        mock_file.assert_called_once()
        assert uuid.UUID(distinct_id)

    # Test case 2: Existing config file with ID
    with (
        patch("os.path.exists") as mock_exists,
        patch("configparser.ConfigParser") as mock_config_parser,
        patch("builtins.open", mock_open()) as mock_file,
    ):
        mock_exists.return_value = True
        mock_config = mock_config_parser.return_value
        mock_config.__contains__.return_value = True
        mock_config.__getitem__.return_value = {"distinct_id": "test-uuid"}
        distinct_id = _get_distinct_id()

        # Verify the existing ID was returned
        assert distinct_id == "test-uuid"
