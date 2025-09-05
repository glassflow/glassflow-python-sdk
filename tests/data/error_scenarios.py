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
            "text": "Pipeline not found",
            "expected_error": errors.PipelineNotFoundError,
            "error_message": "not found",
        },
        {
            "name": "forbidden",
            "status_code": 403,
            "text": "Pipeline already active",
            "expected_error": errors.PipelineAlreadyExistsError,
            "error_message": "already exists",
        },
        {
            "name": "bad_request",
            "status_code": 400,
            "text": "Bad request",
            "expected_error": errors.ValidationError,
            "error_message": "Bad request",
        },
        {
            "name": "server_error",
            "status_code": 500,
            "text": "Internal server error",
            "expected_error": errors.ServerError,
            "error_message": "Internal server error",
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


def get_sink_validation_error_scenarios():
    """Get sink validation error test scenarios."""

    def get_sink_with_source_id_not_found(valid_config):
        sink = valid_config["sink"]
        sink["table_mapping"] = [
            models.TableMapping(
                source_id="non-existent-topic",
                field_name="id",
                column_name="id",
                column_type="String",
            )
        ]
        return sink

    def get_sink_with_field_name_not_found(valid_config):
        sink = valid_config["sink"]
        sink["table_mapping"] = [
            models.TableMapping(
                source_id=valid_config["source"]["topics"][0]["name"],
                field_name="non-existent-field",
                column_name="id",
                column_type="String",
            )
        ]
        return sink

    return [
        {
            "name": "source_id_not_found",
            "sink": get_sink_with_source_id_not_found,
            "expected_error": ValueError,
            "error_message": "does not exist in any topic",
        },
        {
            "name": "field_name_not_found",
            "sink": get_sink_with_field_name_not_found,
            "expected_error": ValueError,
            "error_message": "does not exist in source",
        },
    ]
