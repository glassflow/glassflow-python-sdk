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
                        "id_field": "session_id",
                        "id_field_type": "string",
                        "time_window": "12h",
                    },
                },
                {
                    "consumer_group_initial_offset": "earliest",
                    "name": "orders",
                    "replicas": 1,
                    "deduplication": {
                        "enabled": True,
                        "id_field": "order_id",
                        "id_field_type": "string",
                        "time_window": "12h",
                    },
                },
            ],
        },
        "join": {
            "enabled": True,
            "type": "temporal",
            "sources": [
                {
                    "source_id": "user_logins",
                    "join_key": "user_id",
                    "time_window": "1h",
                    "orientation": "left",
                },
                {
                    "source_id": "orders",
                    "join_key": "user_id",
                    "time_window": "1h",
                    "orientation": "right",
                },
            ],
        },
        "filter": {
            "enabled": True,
            "expression": "user_id = '123'",
        },
        "sink": {
            "type": "clickhouse",
            "provider": "aiven",
            "host": "<host>",
            "port": "12753",
            "http_port": "12754",
            "database": "default",
            "username": "<user>",
            "password": "<password>",
            "secure": True,
            "max_batch_size": 1,
            "table": "user_orders",
        },
        "schema": {
            "fields": [
                {
                    "source_id": "user_logins",
                    "name": "session_id",
                    "type": "string",
                    "column_name": "session_id",
                    "column_type": "String",
                },
                {
                    "source_id": "user_logins",
                    "name": "user_id",
                    "type": "string",
                    "column_name": "user_id",
                    "column_type": "String",
                },
                {
                    "source_id": "orders",
                    "name": "order_id",
                    "type": "string",
                    "column_name": "order_id",
                    "column_type": "String",
                },
                {
                    "source_id": "orders",
                    "name": "user_id",
                    "type": "string",
                    "column_name": "user_id",
                    "column_type": "String",
                },
                {
                    "source_id": "user_logins",
                    "name": "timestamp",
                    "type": "string",
                    "column_name": "login_at",
                    "column_type": "DateTime",
                },
                {
                    "source_id": "orders",
                    "name": "timestamp",
                    "type": "string",
                    "column_name": "order_placed_at",
                    "column_type": "DateTime",
                },
                {
                    "source_id": "orders",
                    "name": "skip_sink_field",
                    "type": "string",
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
                        "id_field": "session_id",
                        "id_field_type": "string",
                        "time_window": "12h",
                    },
                },
            ],
        },
        "sink": {
            "type": "clickhouse",
            "provider": "aiven",
            "host": "<host>",
            "port": "12753",
            "http_port": "12754",
            "database": "default",
            "username": "<user>",
            "password": "<password>",
            "secure": True,
            "max_batch_size": 1,
            "table": "user_orders",
        },
        "schema": {
            "fields": [
                {
                    "source_id": "user_logins",
                    "name": "session_id",
                    "type": "string",
                    "column_name": "session_id",
                    "column_type": "String",
                },
                {
                    "source_id": "user_logins",
                    "name": "user_id",
                    "type": "string",
                    "column_name": "user_id",
                    "column_type": "String",
                },
                {
                    "source_id": "user_logins",
                    "name": "timestamp",
                    "type": "string",
                    "column_name": "login_at",
                    "column_type": "DateTime",
                },
            ],
        },
    }


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
            "host": "clickhouse:8443",
            "port": "8443",
            "database": "test",
            "username": "default",
            "password": "pass",
            "table": "test_table",
        },
        "schema": {
            "fields": [],  # Empty schema fields should trigger validation error
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
