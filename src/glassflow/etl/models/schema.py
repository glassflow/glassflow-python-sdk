from typing import List, Optional

from pydantic import BaseModel, Field, model_validator

from ..errors import InvalidDataTypeMappingError
from .data_types import (
    ClickhouseDataType,
    KafkaDataType,
    kafka_to_clickhouse_data_type_mappings,
)


class SchemaField(BaseModel):
    column_name: Optional[str] = Field(
        default=None,
        description="The name of the column in the sink table. If not provided, "
        "the name of the source field will not be mapped to the sink table.",
    )
    column_type: Optional[ClickhouseDataType] = Field(
        default=None,
        description="The data type of the column in the sink table. If not provided, "
        "the data type of the source field will not be mapped to the sink table.",
    )
    name: str = Field(description="The name of the source field")
    source_id: Optional[str] = Field(description="The name of the source topic")
    type: KafkaDataType = Field(description="The data type of the source field")

    @model_validator(mode="after")
    def validate_schema_field(self) -> "SchemaField":
        # Validate not mapped fields
        if (self.column_name is None and self.column_type is not None) or (
            self.column_name is not None and self.column_type is None
        ):
            raise ValueError(
                "column_name and column_type must both be provided or both be None"
            )

        # Validate column type compatibility
        if self.column_type is not None:
            compatible_types = kafka_to_clickhouse_data_type_mappings.get(self.type, [])
            if self.column_type not in compatible_types:
                raise InvalidDataTypeMappingError(
                    f"Data type '{self.column_type}' is not compatible with source type"
                    f" '{self.type}' for field '{self.name}' in source"
                    f" '{self.source_id}'"
                )
        return self


class Schema(BaseModel):
    fields: List[SchemaField]

    def is_field_in_schema(
        self,
        field_name: str,
        source_id: Optional[str] = None,
    ) -> bool:
        """
        Check if a field exists in the schema. If source_id is provided, check if the
        field exists in the specified source.

        Args:
            field_name: The name of the field to check
            source_id: The ID of the source to check the field in

        Returns:
            True if the field exists in the schema, False otherwise
        """
        for field in self.fields:
            if field.name == field_name and (
                source_id is None or field.source_id == source_id
            ):
                return True
        return False
