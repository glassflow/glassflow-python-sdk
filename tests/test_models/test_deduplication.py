import pytest

from glassflow.etl import models


class TestDeduplicationConfig:
    """Tests for DeduplicationConfig model."""

    def test_deduplication_config_enabled_true(self):
        """Test DeduplicationConfig when enabled is True."""
        with pytest.raises(ValueError) as exc_info:
            models.DeduplicationConfig(
                enabled=True,
                id_field=None,
                id_field_type=None,
                time_window=None,
            )
        assert "is required when deduplication is enabled" in str(exc_info.value)

        # All fields should be required when enabled is True
        config = models.DeduplicationConfig(
            enabled=True,
            id_field="id",
            id_field_type="string",
            time_window="1h",
        )
        assert config.enabled is True
        assert config.id_field == "id"
        assert config.id_field_type == "string"
        assert config.time_window == "1h"

    def test_deduplication_config_enabled_false(self):
        """Test DeduplicationConfig when enabled is False."""
        # All fields should be optional when enabled is False
        config = models.DeduplicationConfig(
            enabled=False,
            id_field=None,
            id_field_type=None,
            time_window=None,
        )
        assert config.enabled is False
        assert config.id_field is None
        assert config.id_field_type is None
        assert config.time_window is None

    def test_deduplication_config_enabled_false_with_fields(self):
        """Test DeduplicationConfig when enabled is False."""
        # All fields should be optional when enabled is False
        config = models.DeduplicationConfig(
            enabled=False,
            id_field="",
            id_field_type="",
            time_window=None,
        )
        assert config.enabled is False
        assert config.id_field is None
        assert config.id_field_type is None
        assert config.time_window is None
