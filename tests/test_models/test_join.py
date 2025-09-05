import pytest

from glassflow.etl import models
from tests.data import error_scenarios


class TestJoinConfig:
    """Tests for JoinConfig model."""

    def test_valid_enabled_join_config(self):
        """Test JoinConfig when enabled is True."""
        config = models.JoinConfig(
            enabled=True,
            type=models.JoinType.TEMPORAL,
            sources=[
                models.JoinSourceConfig(
                    source_id="test-topic-1",
                    join_key="id",
                    time_window="1h",
                    orientation=models.JoinOrientation.LEFT,
                ),
                models.JoinSourceConfig(
                    source_id="test-topic-2",
                    join_key="id",
                    time_window="1h",
                    orientation=models.JoinOrientation.RIGHT,
                ),
            ],
        )
        assert config.enabled is True
        assert config.type == models.JoinType.TEMPORAL
        assert len(config.sources) == 2
        assert config.sources[0].orientation == models.JoinOrientation.LEFT
        assert config.sources[1].orientation == models.JoinOrientation.RIGHT

    def test_valid_disabled_join_config(self):
        """Test JoinConfig when enabled is False."""
        config = models.JoinConfig(
            enabled=False,
            type=None,
            sources=None,
        )
        assert config.enabled is False
        assert config.type is None
        assert config.sources is None

    @pytest.mark.parametrize(
        "scenario",
        error_scenarios.get_join_validation_error_scenarios(),
        ids=lambda s: s["name"],
    )
    def test_join_validation_error_scenarios(self, valid_config, scenario):
        """Test join validation with various error scenarios."""
        with pytest.raises(scenario["expected_error"]) as exc_info:
            models.PipelineConfig(
                pipeline_id="test-pipeline",
                source=valid_config["source"],
                join=scenario["join"](valid_config),
                sink=valid_config["sink"],
            )
        assert scenario["error_message"] in str(exc_info.value)
