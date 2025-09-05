from typing import List, Optional

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


class SinkConfigPatch(BaseModel):
    provider: Optional[str] = Field(default=None)
    host: Optional[str] = Field(default=None)
    port: Optional[str] = Field(default=None)
    http_port: Optional[str] = Field(default=None)
    database: Optional[str] = Field(default=None)
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    secure: Optional[bool] = Field(default=None)
    max_batch_size: Optional[int] = Field(default=None)
    max_delay_time: Optional[str] = Field(default=None)
    table: Optional[str] = Field(default=None)
    table_mapping: Optional[List[TableMapping]] = Field(default=None)
