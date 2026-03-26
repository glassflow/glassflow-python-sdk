import pytest

from glassflow.etl import models


class TestDeduplicationConfig:
    """Tests for DeduplicationConfig model."""

    def test_deduplication_config_enabled_true(self):
        """Test DeduplicationConfig when enabled is True."""
        with pytest.raises(ValueError) as exc_info:
            models.DeduplicationConfig(
                enabled=True,
                key=None,
                time_window=None,
            )
        assert "is required when deduplication is enabled" in str(exc_info.value)

        # All fields should be required when enabled is True
        config = models.DeduplicationConfig(
            enabled=True,
            key="id",
            time_window="1h",
        )
        assert config.enabled is True
        assert config.key == "id"
        assert config.time_window == "1h"

    def test_deduplication_config_enabled_false(self):
        """Test DeduplicationConfig when enabled is False."""
        # All fields should be optional when enabled is False
        config = models.DeduplicationConfig(
            enabled=False,
            key=None,
            time_window=None,
        )
        assert config.enabled is False
        assert config.key is None
        assert config.time_window is None

    def test_deduplication_config_enabled_false_with_fields(self):
        """Test DeduplicationConfig when enabled is False."""
        # All fields should be optional when enabled is False
        config = models.DeduplicationConfig(
            enabled=False,
            key="",
            time_window=None,
        )
        assert config.enabled is False
        assert config.key is None
        assert config.time_window is None

    def test_deduplication_key_field(self):
        """key is the canonical deduplication field name."""
        config = models.DeduplicationConfig(
            enabled=True,
            key="session_id",
            time_window="12h",
        )
        assert config.key == "session_id"

    def test_deduplication_key_via_dict(self):
        data = {"enabled": True, "key": "order_id", "time_window": "1h"}
        config = models.DeduplicationConfig.model_validate(data)
        assert config.key == "order_id"

    def test_deduplication_requires_key_when_enabled(self):
        with pytest.raises(ValueError, match="key is required"):
            models.DeduplicationConfig(enabled=True, time_window="1h")

    def test_deduplication_requires_time_window_when_enabled(self):
        with pytest.raises(ValueError, match="time_window is required"):
            models.DeduplicationConfig(enabled=True, key="session_id")

    def test_deduplication_defaults(self):
        d = models.DeduplicationConfig()
        assert d.enabled is False
