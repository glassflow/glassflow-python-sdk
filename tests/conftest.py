import copy
from unittest.mock import MagicMock, patch

import httpx
import pytest


@pytest.fixture
def mock_track(autouse=True):
    """Mock the Mixpanel track method."""
    with patch("glassflow.etl.tracking.mixpanel.Mixpanel.track") as mock:
        yield mock


@pytest.fixture
def valid_pipeline_config() -> dict:
    """Fixture for a valid pipeline configuration."""
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
                    "schema": {
                        "type": "json",
                        "fields": [
                            {
                                "name": "session_id",
                                "type": "string",
                            },
                            {
                                "name": "user_id",
                                "type": "string",
                            },
                            {
                                "name": "timestamp",
                                "type": "String",
                            },
                        ],
                    },
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
                    "schema": {
                        "type": "json",
                        "fields": [
                            {
                                "name": "user_id",
                                "type": "string",
                            },
                            {
                                "name": "order_id",
                                "type": "string",
                            },
                            {
                                "name": "timestamp",
                                "type": "string",
                            },
                        ],
                    },
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
        "sink": {
            "type": "clickhouse",
            "provider": "aiven",
            "host": "<host>",
            "port": "12753",
            "database": "default",
            "username": "<user>",
            "password": "<password>",
            "secure": True,
            "max_batch_size": 1,
            "table": "user_orders",
            "table_mapping": [
                {
                    "source_id": "user_logins",
                    "field_name": "session_id",
                    "column_name": "session_id",
                    "column_type": "string",
                },
                {
                    "source_id": "user_logins",
                    "field_name": "user_id",
                    "column_name": "user_id",
                    "column_type": "STRING",
                },
                {
                    "source_id": "orders",
                    "field_name": "order_id",
                    "column_name": "order_id",
                    "column_type": "string",
                },
                {
                    "source_id": "user_logins",
                    "field_name": "timestamp",
                    "column_name": "login_at",
                    "column_type": "DateTime",
                },
                {
                    "source_id": "orders",
                    "field_name": "timestamp",
                    "column_name": "order_placed_at",
                    "column_type": "DateTime",
                },
            ],
        },
    }


@pytest.fixture
def valid_pipeline_config_without_joins() -> dict:
    """Fixture for a valid pipeline configuration without joins."""
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
                    "schema": {
                        "type": "json",
                        "fields": [
                            {
                                "name": "session_id",
                                "type": "string",
                            },
                            {
                                "name": "user_id",
                                "type": "string",
                            },
                            {
                                "name": "timestamp",
                                "type": "String",
                            },
                        ],
                    },
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
            "database": "default",
            "username": "<user>",
            "password": "<password>",
            "secure": True,
            "max_batch_size": 1,
            "table": "user_orders",
            "table_mapping": [
                {
                    "source_id": "user_logins",
                    "field_name": "session_id",
                    "column_name": "session_id",
                    "column_type": "string",
                },
                {
                    "source_id": "user_logins",
                    "field_name": "user_id",
                    "column_name": "user_id",
                    "column_type": "STRING",
                },
                {
                    "source_id": "user_logins",
                    "field_name": "timestamp",
                    "column_name": "login_at",
                    "column_type": "DateTime",
                },
            ],
        },
    }


@pytest.fixture
def valid_pipeline_config_with_dedup_disabled(valid_pipeline_config) -> dict:
    """Fixture for a valid pipeline configuration with deduplication disabled."""
    config = copy.deepcopy(valid_pipeline_config)
    for idx, _ in enumerate(config["source"]["topics"]):
        config["source"]["topics"][idx]["deduplication"] = None
    return config


@pytest.fixture
def valid_pipeline_config_without_joins_and_dedup_disabled(
    valid_pipeline_config_without_joins,
) -> dict:
    """Fixture for a valid pipeline configuration without joins and deduplication."""
    config = copy.deepcopy(valid_pipeline_config_without_joins)
    for idx, _ in enumerate(config["source"]["topics"]):
        config["source"]["topics"][idx]["deduplication"] = None
    return config


@pytest.fixture
def invalid_pipeline_config() -> dict:
    """Fixture for an invalid pipeline configuration."""
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
            "table_mapping": [],  # Empty table mapping should trigger validation error
        },
    }


@pytest.fixture
def invalid_join_config() -> dict:
    """Fixture for a configuration with invalid join configuration."""
    return {
        "pipeline_id": "test-pipeline",
        "source": {
            "type": "kafka",
            "connection_params": {
                "brokers": ["kafka:9092"],
                "protocol": "SASL_SSL",
                "mechanism": "SCRAM-SHA-256",
                "username": "user",
                "password": "pass",
            },
            "topics": [
                {
                    "name": "test-topic",
                    "consumer_group_initial_offset": "earliest",
                    "schema": {
                        "type": "json",
                        "fields": [{"name": "id", "type": "String"}],
                    },
                    "deduplication": {
                        "enabled": True,
                        "id_field": "id",
                        "time_window": "1h",
                        "id_field_type": "string",
                    },
                },
            ],
        },
        "join": {
            "enabled": True,
            "type": "temporal",
            "sources": [
                {
                    "source_id": "non-existent-topic",  # Invalid source ID
                    "join_key": "id",
                    "time_window": "1h",
                    "orientation": "left",
                },
            ],
        },
        "sink": {
            "type": "clickhouse",
            "host": "clickhouse:8443",
            "port": "8443",
            "database": "test",
            "username": "default",
            "password": "pass",
            "table": "test_table",
            "table_mapping": [],
        },
    }


@pytest.fixture
def mock_success_response():
    """Fixture for a successful HTTP response."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 201
    mock_response.raise_for_status.return_value = None
    return mock_response


@pytest.fixture
def mock_not_found_response():
    """Fixture for a 404 Not Found HTTP response."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 404
    mock_response.text = "No active pipeline"
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Not Found", request=MagicMock(), response=mock_response
    )
    return mock_response


@pytest.fixture
def mock_forbidden_response(valid_pipeline_config):
    """Fixture for a 403 Forbidden HTTP response."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 403
    mock_response.text = (
        f"Pipeline with id {valid_pipeline_config['pipeline_id']} already active"
    )
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Forbidden", request=MagicMock(), response=mock_response
    )
    return mock_response


@pytest.fixture
def mock_bad_request_response():
    """Fixture for a 400 Bad Request HTTP response."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 400
    mock_response.text = "Bad request"
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Bad Request", request=MagicMock(), response=mock_response
    )
    return mock_response


@pytest.fixture
def mock_server_error_response():
    """Fixture for a 500 Server Error HTTP response."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 500
    mock_response.text = "Server error"
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Server Error", request=MagicMock(), response=mock_response
    )
    return mock_response


@pytest.fixture
def mock_connection_error():
    """Fixture for a connection error."""
    return httpx.ConnectError("Connection failed")
