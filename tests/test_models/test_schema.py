import pytest

from glassflow.etl import models
from tests.data import error_scenarios


class TestSchemaConfig:
    """Tests for SchemaConfig validation."""

    @pytest.mark.parametrize(
        "scenario",
        error_scenarios.get_schema_validation_error_scenarios(),
        ids=lambda s: s["name"],
    )
    def test_schema_validation_error_scenarios(self, valid_config, scenario):
        """Test schema validation with various error scenarios."""

        with pytest.raises(scenario["expected_error"]) as exc_info:
            models.PipelineConfig(
                pipeline_id="test-pipeline",
                source=valid_config["source"],
                sink=valid_config["sink"],
                schema=scenario["schema"](valid_config),
                filter=valid_config["filter"],
                join=valid_config["join"],
            )
        assert scenario["error_message"] in str(exc_info.value)
