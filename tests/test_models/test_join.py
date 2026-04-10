import pytest

from glassflow.etl import models
from tests.data import error_scenarios


class TestJoinSourceConfig:
    """Tests for JoinSourceConfig model."""

    def test_join_source_config_key(self):
        js = models.JoinSourceConfig(
            source_id="src-logins",
            key="user_id",
            time_window="1h",
        )
        assert js.key == "user_id"

    def test_join_source_config_key_from_dict(self):
        data = {
            "source_id": "src-orders",
            "key": "user_id",
            "time_window": "1h",
        }
        js = models.JoinSourceConfig.model_validate(data)
        assert js.key == "user_id"


class TestJoinOutputField:
    """Tests for the JoinOutputField model."""

    def test_join_output_field_with_output_name(self):
        f = models.JoinOutputField(
            source_id="src-logins",
            name="session_id",
            output_name="login_session_id",
        )
        assert f.source_id == "src-logins"
        assert f.name == "session_id"
        assert f.output_name == "login_session_id"

    def test_join_output_field_without_output_name(self):
        f = models.JoinOutputField(source_id="src-orders", name="order_id")
        assert f.output_name is None

    def test_join_config_with_output_fields(self):
        config = models.JoinConfig(
            enabled=True,
            type=models.JoinType.TEMPORAL,
            left_source=models.JoinSourceConfig(
                source_id="src1", key="id", time_window="1h"
            ),
            right_source=models.JoinSourceConfig(
                source_id="src2", key="id", time_window="1h"
            ),
            output_fields=[
                models.JoinOutputField(
                    source_id="src1", name="field_a", output_name="a"
                ),
                models.JoinOutputField(source_id="src2", name="field_b"),
            ],
        )
        assert len(config.output_fields) == 2
        assert config.output_fields[0].output_name == "a"
        assert config.output_fields[1].output_name is None


class TestJoinConfig:
    """Tests for JoinConfig model."""

    def test_valid_enabled_join_config(self):
        """Test JoinConfig when enabled is True."""
        config = models.JoinConfig(
            enabled=True,
            type=models.JoinType.TEMPORAL,
            left_source=models.JoinSourceConfig(
                source_id="test-topic-1", key="id", time_window="1h"
            ),
            right_source=models.JoinSourceConfig(
                source_id="test-topic-2", key="id", time_window="1h"
            ),
            output_fields=[
                models.JoinOutputField(source_id="test-topic-1", name="f1"),
            ],
        )
        assert config.enabled is True
        assert config.type == models.JoinType.TEMPORAL
        assert config.left_source.source_id == "test-topic-1"
        assert config.right_source.source_id == "test-topic-2"

    def test_valid_disabled_join_config(self):
        """Test JoinConfig when enabled is False."""
        config = models.JoinConfig(
            enabled=False,
            type=None,
        )
        assert config.enabled is False
        assert config.type is None
        assert config.left_source is None
        assert config.right_source is None

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
                sources=valid_config["sources"],
                join=scenario["join"](valid_config),
                sink=valid_config["sink"],
                transforms=valid_config["transforms"],
            )
        assert scenario["error_message"] in str(exc_info.value)
