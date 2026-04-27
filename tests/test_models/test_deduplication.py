import pytest

from glassflow.etl import models


class TestDedupTransformConfig:
    """Tests for DedupTransformConfig model."""

    def test_dedup_config_valid(self):
        config = models.DedupTransformConfig(key="id", time_window="1h")
        assert config.key == "id"
        assert config.time_window == "1h"

    def test_dedup_config_missing_key(self):
        with pytest.raises(ValueError, match="key is required"):
            models.DedupTransformConfig(key="", time_window="1h")

    def test_dedup_config_missing_time_window(self):
        with pytest.raises(ValueError, match="time_window is required"):
            models.DedupTransformConfig(key="id", time_window="")

    def test_dedup_config_from_dict(self):
        data = {"key": "order_id", "time_window": "1h"}
        config = models.DedupTransformConfig.model_validate(data)
        assert config.key == "order_id"


class TestDedupTransform:
    """Tests for DedupTransform entry model."""

    def test_dedup_transform_creation(self):
        t = models.DedupTransform(
            source_id="orders",
            config=models.DedupTransformConfig(key="order_id", time_window="1h"),
        )
        assert t.type == "dedup"
        assert t.source_id == "orders"
        assert t.config.key == "order_id"

    def test_dedup_transform_from_dict(self):
        data = {
            "type": "dedup",
            "source_id": "orders",
            "config": {"key": "order_id", "time_window": "1h"},
        }
        t = models.DedupTransform.model_validate(data)
        assert t.config.key == "order_id"
