"""Error scenario test data."""

from pydantic import ValidationError

from glassflow.etl import errors, models


def get_validation_error_scenarios():
    """Get validation error test scenarios."""
    return [
        {
            "name": "empty_pipeline_id",
            "config": {"pipeline_id": ""},
            "expected_error": ValueError,
            "error_message": "pipeline_id cannot be empty",
        },
        {
            "name": "invalid_pipeline_id_format",
            "config": {"pipeline_id": "Test_Pipeline"},
            "expected_error": ValueError,
            "error_message": (
                "pipeline_id can only contain lowercase letters, numbers, and hyphens"
            ),
        },
        {
            "name": "pipeline_id_too_long",
            "config": {
                "pipeline_id": "test-pipeline-1234567890123456789012345678901234567890"
            },
            "expected_error": ValueError,
            "error_message": "pipeline_id cannot be longer than 40 characters",
        },
        {
            "name": "pipeline_id_starts_with_hyphen",
            "config": {"pipeline_id": "-test-pipeline"},
            "expected_error": ValueError,
            "error_message": "pipeline_id must start with a lowercase alphanumeric",
        },
        {
            "name": "pipeline_id_ends_with_hyphen",
            "config": {"pipeline_id": "test-pipeline-"},
            "expected_error": ValueError,
            "error_message": "pipeline_id must end with a lowercase alphanumeric",
        },
    ]


def get_http_error_scenarios():
    """Get HTTP error test scenarios."""
    return [
        {
            "name": "not_found",
            "status_code": 404,
            "json_data": {"message": "Pipeline not found"},
            "expected_error": errors.PipelineNotFoundError,
            "error_message": "not found",
        },
        {
            "name": "forbidden",
            "status_code": 403,
            "json_data": {"message": "Pipeline already active"},
            "expected_error": errors.PipelineAlreadyExistsError,
            "error_message": "already exists",
        },
        {
            "name": "bad_request",
            "status_code": 400,
            "json_data": {"message": "Bad request"},
            "expected_error": errors.ValidationError,
            "error_message": "Bad request",
        },
        {
            "name": "server_error",
            "status_code": 500,
            "json_data": {"message": "Internal server error"},
            "expected_error": errors.ServerError,
            "error_message": "Internal server error",
        },
        # Status validation error scenarios for 400 Bad Request responses
        {
            "name": "terminal_state_violation",
            "status_code": 400,
            "json_data": {
                "message": (
                    "Cannot transition from terminal state Terminated to Running"
                ),
                "code": "TERMINAL_STATE_VIOLATION",
                "current_status": "Terminated",
                "requested_status": "Running",
            },
            "expected_error": errors.TerminalStateViolationError,
            "error_message": (
                "Cannot transition from terminal state Terminated to Running"
            ),
        },
        {
            "name": "invalid_status_transition",
            "status_code": 400,
            "json_data": {
                "message": "Invalid status transition from Running to Paused",
                "code": "INVALID_STATUS_TRANSITION",
                "current_status": "Running",
                "requested_status": "Paused",
                "valid_transitions": ["Stopping", "Terminating"],
            },
            "expected_error": errors.InvalidStatusTransitionError,
            "error_message": "Invalid status transition from Running to Paused",
        },
        {
            "name": "unknown_status",
            "status_code": 400,
            "json_data": {
                "message": "Unknown pipeline status: InvalidStatus",
                "code": "UNKNOWN_STATUS",
                "current_status": "InvalidStatus",
            },
            "expected_error": errors.UnknownStatusError,
            "error_message": "Unknown pipeline status: InvalidStatus",
        },
        {
            "name": "pipeline_already_in_state",
            "status_code": 400,
            "json_data": {
                "message": "Pipeline is already in Running state",
                "code": "PIPELINE_ALREADY_IN_STATE",
                "current_status": "Running",
                "requested_status": "Running",
            },
            "expected_error": errors.PipelineAlreadyInStateError,
            "error_message": "Pipeline is already in Running state",
        },
        {
            "name": "pipeline_in_transition",
            "status_code": 400,
            "json_data": {
                "message": (
                    "Pipeline is currently transitioning from Pausing state, "
                    "cannot perform Stopping operation"
                ),
                "code": "PIPELINE_IN_TRANSITION",
                "current_status": "Pausing",
                "requested_status": "Stopping",
            },
            "expected_error": errors.PipelineInTransitionError,
            "error_message": (
                "Pipeline is currently transitioning from Pausing state, "
                "cannot perform Stopping operation"
            ),
        },
        {
            "name": "invalid_json",
            "status_code": 400,
            "json_data": {"message": "invalid json: unexpected end of JSON input"},
            "expected_error": errors.InvalidJsonError,
            "error_message": "invalid json: unexpected end of JSON input",
        },
        {
            "name": "empty_pipeline_id",
            "status_code": 400,
            "json_data": {"message": "pipeline id cannot be empty"},
            "expected_error": errors.EmptyPipelineIdError,
            "error_message": "pipeline id cannot be empty",
        },
        {
            "name": "pipeline_deletion_state_violation",
            "status_code": 400,
            "json_data": {
                "message": (
                    "pipeline can only be deleted if it's stopped or terminated, "
                    "current status: Running"
                ),
                "field": {"current_status": "Running"},
            },
            "expected_error": errors.PipelineDeletionStateViolationError,
            "error_message": (
                "pipeline can only be deleted if it's stopped or terminated, "
                "current status: Running"
            ),
        },
    ]


def get_dlq_error_scenarios():
    """Get DLQ error test scenarios."""
    return [
        {
            "name": "invalid_batch_size_negative",
            "batch_size": -1,
            "expected_error": ValueError,
            "error_message": "batch_size must be an integer between 1 and 100",
        },
        {
            "name": "invalid_batch_size_zero",
            "batch_size": 0,
            "expected_error": ValueError,
            "error_message": "batch_size must be an integer between 1 and 100",
        },
        {
            "name": "invalid_batch_size_too_large",
            "batch_size": 101,
            "expected_error": ValueError,
            "error_message": "batch_size must be an integer between 1 and 100",
        },
        {
            "name": "invalid_batch_size_non_integer",
            "batch_size": "invalid",
            "expected_error": ValueError,
            "error_message": "batch_size must be an integer between 1 and 100",
        },
        {
            "name": "http_error_422_validation_error",
            "status_code": 422,
            "text": "Invalid batch size",
            "expected_error": errors.InvalidBatchSizeError,
            "error_message": "Invalid batch size",
        },
        {
            "name": "http_error_500_server_error",
            "status_code": 500,
            "text": "Internal server error",
            "expected_error": errors.ServerError,
            "error_message": "Internal server error",
        },
    ]


def get_join_validation_error_scenarios():
    """Get join validation error test scenarios."""

    def get_join_with_source_id_not_found(valid_config):
        join = valid_config["join"].copy()
        join["sources"][0]["source_id"] = "non-existent-topic"
        return join

    def get_join_with_join_key_not_found(valid_config):
        join = valid_config["join"].copy()
        join["sources"][0]["join_key"] = "non-existent-field"
        return join

    def get_join_with_same_orientation(valid_config):
        join = valid_config["join"].copy()
        join["sources"][0]["orientation"] = models.JoinOrientation.LEFT
        join["sources"][1]["orientation"] = models.JoinOrientation.LEFT
        return join

    def get_join_with_only_one_source(valid_config):
        join = valid_config["join"].copy()
        join["sources"] = [join["sources"][0]]
        return join

    def get_join_with_invalid_type(valid_config):
        join = valid_config["join"].copy()
        join["type"] = None
        return join

    return [
        {
            "name": "source_id_not_found",
            "join": get_join_with_source_id_not_found,
            "expected_error": ValueError,
            "error_message": "does not exist in any topic",
        },
        {
            "name": "join_key_not_found",
            "join": get_join_with_join_key_not_found,
            "expected_error": ValueError,
            "error_message": "does not exist in source",
        },
        {
            "name": "same_orientation",
            "join": get_join_with_same_orientation,
            "expected_error": ValidationError,
            "error_message": "join sources must have opposite orientations",
        },
        {
            "name": "join_with_only_one_source",
            "join": get_join_with_only_one_source,
            "expected_error": ValueError,
            "error_message": "join must have exactly two sources when enabled",
        },
        {
            "name": "join_with_invalid_type",
            "join": get_join_with_invalid_type,
            "expected_error": ValueError,
            "error_message": "type is required when join is enabled",
        },
    ]


def get_schema_validation_error_scenarios():
    """Get schema validation error test scenarios."""

    def get_schema_with_source_id_not_found(valid_config):
        schema = valid_config["schema"]
        schema["fields"] = schema["fields"] + [
            {
                "source_id": "non-existent-topic",
                "name": "id",
                "type": "string",
                "column_name": "id",
                "column_type": "String",
            }
        ]
        return schema

    def get_schema_with_missing_column_name(valid_config):
        schema = valid_config["schema"]
        schema["fields"] = schema["fields"] + [
            {
                "source_id": valid_config["source"]["topics"][0]["name"],
                "name": "id",
                "type": "string",
                "column_type": "String",
            }
        ]
        return schema

    def get_schema_with_missing_column_type(valid_config):
        schema = valid_config["schema"]
        schema["fields"] = schema["fields"] + [
            {
                "source_id": valid_config["source"]["topics"][0]["name"],
                "name": "id",
                "type": "string",
                "column_name": "id",
            }
        ]
        return schema

    return [
        {
            "name": "source_id_not_found",
            "schema": get_schema_with_source_id_not_found,
            "expected_error": ValueError,
            "error_message": "does not exist in any topic",
        },
        {
            "name": "missing_column_name",
            "schema": get_schema_with_missing_column_name,
            "expected_error": ValueError,
            "error_message": "column_name and column_type must both be provided or both"
            " be None",
        },
        {
            "name": "missing_column_type",
            "schema": get_schema_with_missing_column_type,
            "expected_error": ValueError,
            "error_message": "column_name and column_type must both be provided or both"
            " be None",
        },
    ]
