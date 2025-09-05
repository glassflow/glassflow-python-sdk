from .base import CaseInsensitiveStrEnum


class KafkaDataType(CaseInsensitiveStrEnum):
    STRING = "string"
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    BOOL = "bool"
    BYTES = "bytes"
    ARRAY = "array"


class ClickhouseDataType(CaseInsensitiveStrEnum):
    INT8 = "Int8"
    INT16 = "Int16"
    INT32 = "Int32"
    INT64 = "Int64"
    FLOAT32 = "Float32"
    FLOAT64 = "Float64"
    STRING = "String"
    FIXEDSTRING = "FixedString"
    DATETIME = "DateTime"
    DATETIME64 = "DateTime64"
    BOOL = "Bool"
    UUID = "UUID"
    ENUM8 = "Enum8"
    ENUM16 = "Enum16"
    LC_INT8 = "LowCardinality(Int8)"
    LC_INT16 = "LowCardinality(Int16)"
    LC_INT32 = "LowCardinality(Int32)"
    LC_INT64 = "LowCardinality(Int64)"
    LC_UINT8 = "LowCardinality(UInt8)"
    LC_UINT16 = "LowCardinality(UInt16)"
    LC_UINT32 = "LowCardinality(UInt32)"
    LC_UINT64 = "LowCardinality(UInt64)"
    LC_FLOAT32 = "LowCardinality(Float32)"
    LC_FLOAT64 = "LowCardinality(Float64)"
    LC_STRING = "LowCardinality(String)"
    LC_FIXEDSTRING = "LowCardinality(FixedString)"
    LC_DATETIME = "LowCardinality(DateTime)"
    ARRAY_STRING = "Array(String)"
    ARRAY_INT8 = "Array(Int8)"
    ARRAY_INT16 = "Array(Int16)"
    ARRAY_INT32 = "Array(Int32)"
    ARRAY_INT64 = "Array(Int64)"


kafka_to_clickhouse_data_type_mappings = {
    KafkaDataType.STRING: [
        ClickhouseDataType.STRING,
        ClickhouseDataType.FIXEDSTRING,
        ClickhouseDataType.DATETIME,
        ClickhouseDataType.DATETIME64,
        ClickhouseDataType.UUID,
        ClickhouseDataType.ENUM8,
        ClickhouseDataType.ENUM16,
        ClickhouseDataType.LC_STRING,
        ClickhouseDataType.LC_FIXEDSTRING,
        ClickhouseDataType.LC_DATETIME,
    ],
    KafkaDataType.INT8: [ClickhouseDataType.INT8, ClickhouseDataType.LC_INT8],
    KafkaDataType.INT16: [ClickhouseDataType.INT16, ClickhouseDataType.LC_INT16],
    KafkaDataType.INT32: [ClickhouseDataType.INT32, ClickhouseDataType.LC_INT32],
    KafkaDataType.INT64: [
        ClickhouseDataType.INT64,
        ClickhouseDataType.DATETIME,
        ClickhouseDataType.DATETIME64,
        ClickhouseDataType.LC_INT64,
        ClickhouseDataType.LC_DATETIME,
    ],
    KafkaDataType.FLOAT32: [ClickhouseDataType.FLOAT32, ClickhouseDataType.LC_FLOAT32],
    KafkaDataType.FLOAT64: [
        ClickhouseDataType.FLOAT64,
        ClickhouseDataType.DATETIME,
        ClickhouseDataType.DATETIME64,
        ClickhouseDataType.LC_FLOAT64,
        ClickhouseDataType.LC_DATETIME,
    ],
    KafkaDataType.BOOL: [ClickhouseDataType.BOOL],
    KafkaDataType.BYTES: [ClickhouseDataType.STRING],
    KafkaDataType.ARRAY: [
        ClickhouseDataType.STRING,
        ClickhouseDataType.ARRAY_STRING,
        ClickhouseDataType.ARRAY_INT8,
        ClickhouseDataType.ARRAY_INT16,
        ClickhouseDataType.ARRAY_INT32,
        ClickhouseDataType.ARRAY_INT64,
    ],
}
