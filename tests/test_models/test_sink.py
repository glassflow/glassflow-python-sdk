import pytest

from glassflow.etl import models
from tests.data import error_scenarios


class TestSinkConfig:
    """Tests for SinkConfig validation."""

    @pytest.mark.parametrize(
        "scenario",
        error_scenarios.get_sink_validation_error_scenarios(),
        ids=lambda s: s["name"],
    )
    def test_sink_validation_error_scenarios(self, valid_config, scenario):
        """Test sink validation with various error scenarios."""

        with pytest.raises(scenario["expected_error"]) as exc_info:
            models.PipelineConfig(
                pipeline_id="test-pipeline",
                source=valid_config["source"],
                sink=scenario["sink"](valid_config),
            )
        assert scenario["error_message"] in str(exc_info.value)
