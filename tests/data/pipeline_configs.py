"""Pipeline configuration test data."""

import copy

_KAFKA_CONNECTION_PARAMS = {
    "brokers": [
        "kafka-broker-0:9092",
        "kafka-broker-1:9092",
    ],
    "protocol": "SASL_SSL",
    "mechanism": "SCRAM-SHA-256",
    "username": "<user>",
    "password": "<password>",
    "root_ca": "<base64 encoded ca>",
}


def get_valid_pipeline_config() -> dict:
    """Get a valid pipeline configuration for testing (with join)."""
    return {
        "pipeline_id": "test-pipeline",
        "sources": [
            {
                "type": "kafka",
                "source_id": "user-logins",
                "connection_params": copy.deepcopy(_KAFKA_CONNECTION_PARAMS),
                "topic": "user_logins",
                "consumer_group_initial_offset": "earliest",
                "schema_fields": [
                    {"name": "session_id", "type": "string"},
                    {"name": "user_id", "type": "string"},
                    {"name": "timestamp", "type": "string"},
                ],
            },
            {
                "type": "kafka",
                "source_id": "orders",
                "connection_params": copy.deepcopy(_KAFKA_CONNECTION_PARAMS),
                "topic": "orders",
                "consumer_group_initial_offset": "earliest",
                "schema_fields": [
                    {"name": "order_id", "type": "string"},
                    {"name": "user_id", "type": "string"},
                    {"name": "timestamp", "type": "string"},
                    {"name": "skip_sink_field", "type": "string"},
                ],
            },
        ],
        "transforms": [
            {
                "type": "dedup",
                "source_id": "user-logins",
                "config": {"key": "session_id", "time_window": "12h"},
            },
            {
                "type": "dedup",
                "source_id": "orders",
                "config": {"key": "order_id", "time_window": "12h"},
            },
            {
                "type": "filter",
                "source_id": "user-logins",
                "config": {"expression": "user_id = '123'"},
            },
            {
                "type": "stateless",
                "source_id": "user-logins",
                "config": {
                    "transforms": [
                        {
                            "expression": "upper(user_id)",
                            "output_name": "upper_user_id",
                            "output_type": "string",
                        },
                    ],
                },
            },
        ],
        "join": {
            "enabled": True,
            "type": "temporal",
            "left_source": {
                "source_id": "user-logins",
                "key": "user_id",
                "time_window": "1h",
            },
            "right_source": {
                "source_id": "orders",
                "key": "user_id",
                "time_window": "1h",
            },
            "output_fields": [
                {"source_id": "user-logins", "name": "session_id"},
                {"source_id": "orders", "name": "order_id"},
                {
                    "source_id": "orders",
                    "name": "timestamp",
                    "output_name": "order_placed_at",
                },
                {
                    "source_id": "user-logins",
                    "name": "timestamp",
                    "output_name": "login_at",
                },
                {
                    "source_id": "user-logins",
                    "name": "user_id",
                    "output_name": "upper_user_id",
                },
            ],
        },
        "sink": {
            "type": "clickhouse",
            "connection_params": {
                "host": "<host>",
                "port": "12753",
                "http_port": "12754",
                "database": "default",
                "username": "<user>",
                "password": "<password>",
                "secure": True,
            },
            "table": "user_orders",
            "max_batch_size": 1,
            "mapping": [
                {
                    "name": "session_id",
                    "column_name": "session_id",
                    "column_type": "String",
                },
                {"name": "user_id", "column_name": "user_id", "column_type": "String"},
                {
                    "name": "order_id",
                    "column_name": "order_id",
                    "column_type": "String",
                },
                {
                    "name": "timestamp",
                    "column_name": "order_placed_at",
                    "column_type": "DateTime",
                },
                {
                    "name": "timestamp",
                    "column_name": "login_at",
                    "column_type": "DateTime",
                },
                {
                    "name": "upper_user_id",
                    "column_name": "upper_user_id",
                    "column_type": "String",
                },
            ],
        },
    }


def get_valid_config_without_joins() -> dict:
    """Get a valid pipeline configuration without joins."""
    return {
        "pipeline_id": "test-pipeline",
        "sources": [
            {
                "type": "kafka",
                "source_id": "user-logins",
                "connection_params": copy.deepcopy(_KAFKA_CONNECTION_PARAMS),
                "topic": "user_logins",
                "consumer_group_initial_offset": "earliest",
                "schema_fields": [
                    {"name": "session_id", "type": "string"},
                    {"name": "user_id", "type": "string"},
                    {"name": "timestamp", "type": "string"},
                ],
            },
        ],
        "transforms": [
            {
                "type": "dedup",
                "source_id": "user-logins",
                "config": {"key": "session_id", "time_window": "12h"},
            },
        ],
        "sink": {
            "type": "clickhouse",
            "connection_params": {
                "host": "<host>",
                "port": "12753",
                "http_port": "12754",
                "database": "default",
                "username": "<user>",
                "password": "<password>",
                "secure": True,
            },
            "table": "user_orders",
            "max_batch_size": 1,
            "source_id": "user-logins",
            "mapping": [
                {
                    "name": "session_id",
                    "column_name": "session_id",
                    "column_type": "String",
                },
                {"name": "user_id", "column_name": "user_id", "column_type": "String"},
                {
                    "name": "timestamp",
                    "column_name": "login_at",
                    "column_type": "DateTime",
                },
            ],
        },
    }


def get_valid_config_with_pipeline_resources() -> dict:
    """Get a valid pipeline configuration including resources."""
    config = copy.deepcopy(get_valid_pipeline_config())
    config["resources"] = {
        "nats": {
            "stream": {
                "maxAge": "72h",
                "maxBytes": "1Gi",
            },
        },
        "sink": {
            "replicas": 2,
            "requests": {"memory": "256Mi", "cpu": "100m"},
            "limits": {"memory": "512Mi", "cpu": "500m"},
        },
        "sources": [
            {
                "source_id": "user-logins",
                "replicas": 2,
                "requests": {"memory": "128Mi", "cpu": "50m"},
                "limits": {"memory": "256Mi", "cpu": "200m"},
            },
        ],
        "transform": [
            {
                "source_id": "user-logins",
                "storage": {"size": "10Gi"},
                "replicas": 1,
                "requests": {"memory": "128Mi", "cpu": "50m"},
                "limits": {"memory": "256Mi", "cpu": "200m"},
            },
        ],
    }
    return config


def get_valid_config_with_dedup_disabled() -> dict:
    """Get a valid pipeline configuration with deduplication disabled."""
    config = copy.deepcopy(get_valid_pipeline_config())
    config["transforms"] = [t for t in config["transforms"] if t["type"] != "dedup"]
    return config


def get_valid_config_without_joins_and_dedup_disabled() -> dict:
    """Get a valid pipeline configuration without joins and deduplication."""
    config = copy.deepcopy(get_valid_config_without_joins())
    config["transforms"] = [
        t for t in (config.get("transforms") or []) if t["type"] != "dedup"
    ]
    if not config["transforms"]:
        config["transforms"] = None
    return config


def get_invalid_config() -> dict:
    """Get an invalid pipeline configuration for testing."""
    return {
        "pipeline_id": "",  # Empty pipeline_id should trigger validation error
        "sources": [
            {
                "type": "kafka",
                "source_id": "test",
                "connection_params": {
                    "brokers": ["kafka:9092"],
                    "protocol": "SASL_SSL",
                    "mechanism": "SCRAM-SHA-256",
                    "username": "user",
                    "password": "pass",
                },
                "topic": "test",
            },
        ],
        "sink": {
            "type": "clickhouse",
            "connection_params": {
                "host": "clickhouse:8443",
                "port": "8443",
                "database": "test",
                "username": "default",
                "password": "pass",
            },
            "table": "test_table",
        },
    }


def get_health_payload(pipeline_id: str) -> dict:
    """Get a health payload for a pipeline."""
    return {
        "pipeline_id": pipeline_id,
        "pipeline_name": "Test Pipeline",
        "overall_status": "Running",
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
    }


# ---------------------------------------------------------------------------
# V3 pipeline config fixtures
# ---------------------------------------------------------------------------


def get_valid_v3_pipeline_config() -> dict:
    """Get a valid V3 Kafka pipeline configuration with explicit source_ids,
    schema_registry, and join with output_fields."""
    return {
        "version": "v3",
        "pipeline_id": "test-v3-pipeline",
        "sources": [
            {
                "type": "kafka",
                "source_id": "src-logins",
                "connection_params": {
                    "brokers": ["kafka-broker-0:9092"],
                    "protocol": "SASL_SSL",
                    "mechanism": "SCRAM-SHA-256",
                    "username": "<user>",
                    "password": "<password>",
                },
                "topic": "user_logins",
                "consumer_group_initial_offset": "earliest",
                "schema_registry": {
                    "url": "https://schema-registry.example.com",
                    "api_key": "key123",
                    "api_secret": "secret456",
                },
                "schema_version": "1",
                "schema_fields": [
                    {"name": "session_id", "type": "string"},
                    {"name": "user_id", "type": "string"},
                ],
            },
            {
                "type": "kafka",
                "source_id": "src-orders",
                "connection_params": {
                    "brokers": ["kafka-broker-0:9092"],
                    "protocol": "SASL_SSL",
                    "mechanism": "SCRAM-SHA-256",
                    "username": "<user>",
                    "password": "<password>",
                },
                "topic": "orders",
                "consumer_group_initial_offset": "earliest",
                "schema_fields": [
                    {"name": "order_id", "type": "string"},
                    {"name": "user_id", "type": "string"},
                ],
            },
        ],
        "transforms": [
            {
                "type": "dedup",
                "source_id": "src-logins",
                "config": {"key": "session_id", "time_window": "12h"},
            },
            {
                "type": "dedup",
                "source_id": "src-orders",
                "config": {"key": "order_id", "time_window": "12h"},
            },
            {
                "type": "stateless",
                "source_id": "src-logins",
                "config": {
                    "transforms": [
                        {
                            "expression": "upper(user_id)",
                            "output_name": "upper_user_id",
                            "output_type": "string",
                        },
                    ],
                },
            },
        ],
        "join": {
            "enabled": True,
            "type": "temporal",
            "left_source": {
                "source_id": "src-logins",
                "key": "user_id",
                "time_window": "1h",
            },
            "right_source": {
                "source_id": "src-orders",
                "key": "user_id",
                "time_window": "1h",
            },
            "output_fields": [
                {
                    "source_id": "src-logins",
                    "name": "session_id",
                    "output_name": "login_session_id",
                },
                {"source_id": "src-orders", "name": "order_id"},
            ],
        },
        "sink": {
            "type": "clickhouse",
            "connection_params": {
                "host": "<host>",
                "port": "12753",
                "http_port": "12754",
                "database": "default",
                "username": "<user>",
                "password": "plaintext-password",
                "secure": True,
            },
            "table": "user_orders",
            "max_batch_size": 1,
            "mapping": [
                {
                    "name": "session_id",
                    "column_name": "session_id",
                    "column_type": "String",
                },
                {"name": "user_id", "column_name": "user_id", "column_type": "String"},
                {
                    "name": "order_id",
                    "column_name": "order_id",
                    "column_type": "String",
                },
                {
                    "name": "upper_user_id",
                    "column_name": "upper_user_id",
                    "column_type": "String",
                },
            ],
        },
    }


def get_valid_otlp_logs_pipeline_config() -> dict:
    """Get a valid V3 OTLP logs pipeline configuration."""
    return {
        "version": "v3",
        "pipeline_id": "test-otlp-logs",
        "sources": [
            {
                "type": "otlp.logs",
                "source_id": "otlp-src",
            },
        ],
        "sink": {
            "type": "clickhouse",
            "connection_params": {
                "host": "<host>",
                "port": "12753",
                "database": "default",
                "username": "<user>",
                "password": "plaintext-password",
                "secure": True,
            },
            "table": "otel_logs",
            "max_batch_size": 1,
            "source_id": "otlp-src",
            "mapping": [
                {"name": "body", "column_name": "body", "column_type": "String"},
            ],
        },
    }


def get_valid_otlp_metrics_pipeline_config() -> dict:
    """Get a valid V3 OTLP metrics pipeline configuration."""
    return {
        "version": "v3",
        "pipeline_id": "test-otlp-metrics",
        "sources": [
            {
                "type": "otlp.metrics",
                "source_id": "metrics-src",
            },
        ],
        "sink": {
            "type": "clickhouse",
            "connection_params": {
                "host": "<host>",
                "port": "12753",
                "database": "default",
                "username": "<user>",
                "password": "plaintext-password",
                "secure": True,
            },
            "table": "otel_metrics",
            "max_batch_size": 1,
            "source_id": "metrics-src",
            "mapping": [
                {
                    "name": "metric_name",
                    "column_name": "metric_name",
                    "column_type": "String",
                },
            ],
        },
    }


def get_valid_otlp_traces_pipeline_config() -> dict:
    """Get a valid V3 OTLP traces pipeline configuration."""
    return {
        "version": "v3",
        "pipeline_id": "test-otlp-traces",
        "sources": [
            {
                "type": "otlp.traces",
                "source_id": "traces-src",
            },
        ],
        "sink": {
            "type": "clickhouse",
            "connection_params": {
                "host": "<host>",
                "port": "12753",
                "database": "default",
                "username": "<user>",
                "password": "plaintext-password",
                "secure": True,
            },
            "table": "otel_traces",
            "max_batch_size": 1,
            "source_id": "traces-src",
            "mapping": [
                {
                    "name": "trace_id",
                    "column_name": "trace_id",
                    "column_type": "String",
                },
            ],
        },
    }


def get_valid_otlp_with_transformation_config() -> dict:
    """Get a valid V3 OTLP pipeline with a stateless transformation."""
    return {
        "version": "v3",
        "pipeline_id": "test-otlp-transform",
        "sources": [
            {
                "type": "otlp.logs",
                "source_id": "otlp-src",
            },
        ],
        "transforms": [
            {
                "type": "stateless",
                "source_id": "otlp-src",
                "config": {
                    "transforms": [
                        {
                            "expression": "upper(body)",
                            "output_name": "upper_body",
                            "output_type": "string",
                        },
                    ],
                },
            },
        ],
        "sink": {
            "type": "clickhouse",
            "connection_params": {
                "host": "<host>",
                "port": "12753",
                "database": "default",
                "username": "<user>",
                "password": "plaintext-password",
                "secure": True,
            },
            "table": "otel_logs",
            "max_batch_size": 1,
            "source_id": "otlp-src",
            "mapping": [
                {"name": "body", "column_name": "body", "column_type": "String"},
                {
                    "name": "upper_body",
                    "column_name": "upper_body",
                    "column_type": "String",
                },
            ],
        },
    }
