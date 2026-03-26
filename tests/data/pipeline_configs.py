"""Pipeline configuration test data."""

import copy


def get_valid_pipeline_config() -> dict:
    """Get a valid pipeline configuration for testing."""
    return {
        "pipeline_id": "test-pipeline",
        "source": {
            "type": "kafka",
            "provider": "aiven",
            "connection_params": {
                "brokers": [
                    "kafka-broker-0:9092",
                    "kafka-broker-1:9092",
                ],
                "protocol": "SASL_SSL",
                "mechanism": "SCRAM-SHA-256",
                "username": "<user>",
                "password": "<password>",
                "root_ca": "<base64 encoded ca>",
            },
            "topics": [
                {
                    "consumer_group_initial_offset": "earliest",
                    "name": "user_logins",
                    "replicas": 3,
                    "deduplication": {
                        "enabled": True,
                        "key": "session_id",
                        "time_window": "12h",
                    },
                    "schema_fields": [
                        {"name": "session_id", "type": "string"},
                        {"name": "user_id", "type": "string"},
                        {"name": "timestamp", "type": "string"},
                    ],
                },
                {
                    "consumer_group_initial_offset": "earliest",
                    "name": "orders",
                    "replicas": 1,
                    "deduplication": {
                        "enabled": True,
                        "key": "order_id",
                        "time_window": "12h",
                    },
                    "schema_fields": [
                        {"name": "order_id", "type": "string"},
                        {"name": "user_id", "type": "string"},
                        {"name": "timestamp", "type": "string"},
                        {"name": "skip_sink_field", "type": "string"},
                    ],
                },
            ],
        },
        "join": {
            "enabled": True,
            "type": "temporal",
            "sources": [
                {
                    "source_id": "user_logins",
                    "key": "user_id",
                    "time_window": "1h",
                    "orientation": "left",
                },
                {
                    "source_id": "orders",
                    "key": "user_id",
                    "time_window": "1h",
                    "orientation": "right",
                },
            ],
        },
        "filter": {
            "enabled": True,
            "expression": "user_id = '123'",
        },
        "stateless_transformation": {
            "enabled": True,
            "id": "my_transformation",
            "type": "expr_lang_transform",
            "config": {
                "transform": [
                    {
                        "expression": "upper(user_id)",
                        "output_name": "upper_user_id",
                        "output_type": "string",
                    },
                ],
            },
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
            "source_id": "my_transformation",
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
        "source": {
            "type": "kafka",
            "provider": "aiven",
            "connection_params": {
                "brokers": [
                    "kafka-broker-0:9092",
                    "kafka-broker-1:9092",
                ],
                "protocol": "SASL_SSL",
                "mechanism": "SCRAM-SHA-256",
                "username": "<user>",
                "password": "<password>",
                "root_ca": "<base64 encoded ca>",
            },
            "topics": [
                {
                    "consumer_group_initial_offset": "earliest",
                    "name": "user_logins",
                    "deduplication": {
                        "enabled": True,
                        "key": "session_id",
                        "time_window": "12h",
                    },
                    "schema_fields": [
                        {"name": "session_id", "type": "string"},
                        {"name": "user_id", "type": "string"},
                        {"name": "timestamp", "type": "string"},
                    ],
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
            "source_id": "user_logins",
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
    """Get a valid pipeline configuration including pipeline_resources."""
    config = copy.deepcopy(get_valid_pipeline_config())
    config["pipeline_resources"] = {
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
        "transform": {
            "storage": {"size": "10Gi"},
            "replicas": 1,
            "requests": {"memory": "128Mi", "cpu": "50m"},
            "limits": {"memory": "256Mi", "cpu": "200m"},
        },
        "join": {
            "replicas": 1,
            "requests": {"memory": "64Mi", "cpu": "25m"},
            "limits": {"memory": "128Mi", "cpu": "100m"},
        },
        "ingestor": {
            "base": {
                "replicas": 2,
                "requests": {"memory": "128Mi", "cpu": "50m"},
                "limits": {"memory": "256Mi", "cpu": "200m"},
            },
        },
    }
    return config


def get_valid_config_with_dedup_disabled() -> dict:
    """Get a valid pipeline configuration with deduplication disabled."""
    config = copy.deepcopy(get_valid_pipeline_config())
    for idx, _ in enumerate(config["source"]["topics"]):
        config["source"]["topics"][idx]["deduplication"] = None
    return config


def get_valid_config_without_joins_and_dedup_disabled() -> dict:
    """Get a valid pipeline configuration without joins and deduplication."""
    config = copy.deepcopy(get_valid_config_without_joins())
    for idx, _ in enumerate(config["source"]["topics"]):
        config["source"]["topics"][idx]["deduplication"] = None
    return config


def get_invalid_config() -> dict:
    """Get an invalid pipeline configuration for testing."""
    return {
        "pipeline_id": "",  # Empty pipeline_id should trigger validation error
        "source": {
            "type": "kafka",
            "connection_params": {
                "brokers": ["kafka:9092"],
                "protocol": "SASL_SSL",
                "mechanism": "SCRAM-SHA-256",
                "username": "user",
                "password": "pass",
            },
            "topics": [],  # Empty topics list should trigger validation error
        },
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
    """Get a valid V3 Kafka pipeline configuration (topics have explicit ids,
    deduplication uses 'key', join sources use 'key')."""
    return {
        "version": "v3",
        "pipeline_id": "test-v3-pipeline",
        "source": {
            "type": "kafka",
            "provider": "aiven",
            "connection_params": {
                "brokers": ["kafka-broker-0:9092"],
                "protocol": "SASL_SSL",
                "mechanism": "SCRAM-SHA-256",
                "username": "<user>",
                "password": "<password>",
            },
            "topics": [
                {
                    "consumer_group_initial_offset": "earliest",
                    "name": "user_logins",
                    "id": "src-logins",
                    "deduplication": {
                        "enabled": True,
                        "key": "session_id",
                        "time_window": "12h",
                    },
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
                    "consumer_group_initial_offset": "earliest",
                    "name": "orders",
                    "id": "src-orders",
                    "deduplication": {
                        "enabled": True,
                        "key": "order_id",
                        "time_window": "12h",
                    },
                    "schema_fields": [
                        {"name": "order_id", "type": "string"},
                        {"name": "user_id", "type": "string"},
                    ],
                },
            ],
        },
        "join": {
            "enabled": True,
            "id": "my-join",
            "type": "temporal",
            "sources": [
                {
                    "source_id": "src-logins",
                    "key": "user_id",
                    "time_window": "1h",
                    "orientation": "left",
                },
                {
                    "source_id": "src-orders",
                    "key": "user_id",
                    "time_window": "1h",
                    "orientation": "right",
                },
            ],
            "fields": [
                {
                    "source_id": "src-logins",
                    "name": "session_id",
                    "output_name": "login_session_id",
                },
                {"source_id": "src-orders", "name": "order_id"},
            ],
        },
        "stateless_transformation": {
            "enabled": True,
            "id": "my_transformation",
            "type": "expr_lang_transform",
            "source_id": "src-logins",
            "config": {
                "transform": [
                    {
                        "expression": "upper(user_id)",
                        "output_name": "upper_user_id",
                        "output_type": "string",
                    },
                ],
            },
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
            "source_id": "my_transformation",
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
        "source": {
            "type": "otlp.logs",
            "id": "otlp-src",
            "deduplication": {
                "enabled": False,
            },
        },
        "join": {
            "enabled": False,
        },
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
        "source": {
            "type": "otlp.metrics",
            "id": "metrics-src",
        },
        "join": {
            "enabled": False,
        },
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
        "source": {
            "type": "otlp.traces",
            "id": "traces-src",
        },
        "join": {
            "enabled": False,
        },
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
        "source": {
            "type": "otlp.logs",
            "id": "otlp-src",
        },
        "join": {
            "enabled": False,
        },
        "stateless_transformation": {
            "enabled": True,
            "id": "log_transform",
            "type": "expr_lang_transform",
            "source_id": "otlp-src",
            "config": {
                "transform": [
                    {
                        "expression": "upper(body)",
                        "output_name": "upper_body",
                        "output_type": "string",
                    },
                ],
            },
        },
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
            "source_id": "log_transform",
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
