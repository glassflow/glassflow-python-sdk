from typing import Any, List, Optional

from pydantic import BaseModel, Field

from .base import CaseInsensitiveStrEnum
from .data_types import ClickhouseDataType


class TableMapping(BaseModel):
    source_id: str
    field_name: str
    column_name: str
    column_type: ClickhouseDataType


class SinkType(CaseInsensitiveStrEnum):
    CLICKHOUSE = "clickhouse"


class SinkConfig(BaseModel):
    type: SinkType = SinkType.CLICKHOUSE
    provider: Optional[str] = Field(default=None)
    host: str
    port: str
    http_port: Optional[str] = Field(default=None)
    database: str
    username: str
    password: str
    secure: bool = Field(default=False)
    skip_certificate_verification: bool = Field(default=False)
    max_batch_size: int = Field(default=1000)
    max_delay_time: str = Field(default="10m")
    table: str
    table_mapping: List[TableMapping]

    def update(self, patch: "SinkConfigPatch") -> "SinkConfig":
        """Apply a patch to this sink config."""
        update_dict: dict[str, Any] = {}

        # Check each field explicitly to handle model instances properly
        if patch.provider is not None:
            update_dict["provider"] = patch.provider
        if patch.host is not None:
            update_dict["host"] = patch.host
        if patch.port is not None:
            update_dict["port"] = patch.port
        if patch.http_port is not None:
            update_dict["http_port"] = patch.http_port
        if patch.database is not None:
            update_dict["database"] = patch.database
        if patch.username is not None:
            update_dict["username"] = patch.username
        if patch.password is not None:
            update_dict["password"] = patch.password
        if patch.secure is not None:
            update_dict["secure"] = patch.secure
        if patch.skip_certificate_verification is not None:
            update_dict["skip_certificate_verification"] = (
                patch.skip_certificate_verification
            )
        if patch.max_batch_size is not None:
            update_dict["max_batch_size"] = patch.max_batch_size
        if patch.max_delay_time is not None:
            update_dict["max_delay_time"] = patch.max_delay_time
        if patch.table is not None:
            update_dict["table"] = patch.table
        if patch.table_mapping is not None:
            update_dict["table_mapping"] = patch.table_mapping

        if update_dict:
            return self.model_copy(update=update_dict)
        return self


class SinkConfigPatch(BaseModel):
    provider: Optional[str] = Field(default=None)
    host: Optional[str] = Field(default=None)
    port: Optional[str] = Field(default=None)
    http_port: Optional[str] = Field(default=None)
    database: Optional[str] = Field(default=None)
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    secure: Optional[bool] = Field(default=None)
    skip_certificate_verification: Optional[bool] = Field(default=None)
    max_batch_size: Optional[int] = Field(default=None)
    max_delay_time: Optional[str] = Field(default=None)
    table: Optional[str] = Field(default=None)
    table_mapping: Optional[List[TableMapping]] = Field(default=None)
