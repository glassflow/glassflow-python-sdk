from typing import List, Optional

from pydantic import BaseModel, Field

from .base import CaseInsensitiveStrEnum
from .data_types import ClickhouseDataType


class SinkType(CaseInsensitiveStrEnum):
    CLICKHOUSE = "clickhouse"


class ClickhouseConnectionParams(BaseModel):
    """Connection parameters for a ClickHouse sink."""

    host: str
    port: str
    http_port: Optional[str] = None
    database: str
    username: str
    password: str
    secure: bool = False
    skip_certificate_verification: bool = False

    def update(
        self, patch: "ClickhouseConnectionParamsPatch"
    ) -> "ClickhouseConnectionParams":
        """Apply a patch to this connection params config."""
        update_dict = self.model_copy(deep=True)

        if patch.host is not None:
            update_dict.host = patch.host
        if patch.port is not None:
            update_dict.port = patch.port
        if patch.http_port is not None:
            update_dict.http_port = patch.http_port
        if patch.database is not None:
            update_dict.database = patch.database
        if patch.username is not None:
            update_dict.username = patch.username
        if patch.password is not None:
            update_dict.password = patch.password
        if patch.secure is not None:
            update_dict.secure = patch.secure
        if patch.skip_certificate_verification is not None:
            update_dict.skip_certificate_verification = (
                patch.skip_certificate_verification
            )

        return update_dict


class SinkFieldMapping(BaseModel):
    """Mapping from an upstream field to a ClickHouse table column."""

    name: str
    column_name: str
    column_type: ClickhouseDataType


class SinkConfig(BaseModel):
    type: SinkType = SinkType.CLICKHOUSE
    connection_params: ClickhouseConnectionParams
    table: str
    max_batch_size: Optional[int] = Field(default=None)
    max_delay_time: Optional[str] = Field(default=None)
    source_id: Optional[str] = Field(default=None)
    mapping: Optional[List[SinkFieldMapping]] = Field(default=None)

    def update(self, patch: "SinkConfigPatch") -> "SinkConfig":
        """Apply a patch to this sink config."""
        update_dict = self.model_copy(deep=True)

        if patch.connection_params is not None:
            update_dict.connection_params = self.connection_params.update(
                patch.connection_params
            )
        if patch.table is not None:
            update_dict.table = patch.table
        if patch.max_batch_size is not None:
            update_dict.max_batch_size = patch.max_batch_size
        if patch.max_delay_time is not None:
            update_dict.max_delay_time = patch.max_delay_time
        if patch.source_id is not None:
            update_dict.source_id = patch.source_id
        if patch.mapping is not None:
            update_dict.mapping = patch.mapping

        return update_dict


class ClickhouseConnectionParamsPatch(BaseModel):
    host: Optional[str] = Field(default=None)
    port: Optional[str] = Field(default=None)
    http_port: Optional[str] = Field(default=None)
    database: Optional[str] = Field(default=None)
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    secure: Optional[bool] = Field(default=None)
    skip_certificate_verification: Optional[bool] = Field(default=None)


class SinkConfigPatch(BaseModel):
    connection_params: Optional[ClickhouseConnectionParamsPatch] = Field(default=None)
    table: Optional[str] = Field(default=None)
    max_batch_size: Optional[int] = Field(default=None)
    max_delay_time: Optional[str] = Field(default=None)
    source_id: Optional[str] = Field(default=None)
    mapping: Optional[List[SinkFieldMapping]] = Field(default=None)
