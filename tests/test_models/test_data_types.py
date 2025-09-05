import pytest

from glassflow.etl import errors, models


class TestDataTypeCompatibility:
    """Tests for data type compatibility validation."""

    def test_validate_data_type_compatibility_invalid_mapping(self, valid_config):
        """Test data type compatibility validation with invalid type mappings."""
        # Modify the sink configuration to have an invalid type mapping
        valid_config["sink"]["table_mapping"][0]["column_type"] = (
            models.ClickhouseDataType.INT32
        )

        with pytest.raises(errors.InvalidDataTypeMappingError):
            models.PipelineConfig(**valid_config)
