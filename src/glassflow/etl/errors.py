class GlassFlowError(Exception):
    """Base exception for all GlassFlow SDK errors."""


# Generic client-side errors
class RequestError(GlassFlowError):
    """A low-level HTTP or network error."""


class ConnectionError(RequestError):
    """Raised when a connection to the server fails."""


# Server/API-level errors
class APIError(GlassFlowError):
    """Base for API response errors."""

    def __init__(self, status_code, message=None, response=None):
        self.status_code = status_code
        self.response = response
        self.message = message
        super().__init__(self.message)


class NotFoundError(APIError):
    """Raised when a resource is not found (404)."""


class ValidationError(APIError):
    """Raised on 400 Bad Request errors due to bad input."""


class ForbiddenError(APIError):
    """Raised on 403 Forbidden errors."""


class UnprocessableContentError(APIError):
    """Raised on 422 Unprocessable Content errors."""


class ServerError(APIError):
    """Raised on 500 Server Error errors."""


class PipelineAlreadyExistsError(APIError):
    """Raised when a pipeline already exists."""


class PipelineNotFoundError(APIError):
    """Raised on 404 when a pipeline is not found."""


class PipelineInvalidConfigurationError(APIError):
    """Raised when a pipeline configuration is invalid."""


class InvalidDataTypeMappingError(GlassFlowError):
    """Exception raised when a data type mapping is invalid."""


class InvalidBatchSizeError(GlassFlowError):
    """Exception raised when a batch size is invalid."""


# Status validation error classes for 400 Bad Request responses
class TerminalStateViolationError(ValidationError):
    """Raised when attempting to transition from a terminal state to another state."""


class InvalidStatusTransitionError(ValidationError):
    """Raised when attempting an invalid status transition."""


class UnknownStatusError(ValidationError):
    """Raised when an unknown pipeline status is encountered."""


class PipelineAlreadyInStateError(ValidationError):
    """Raised when pipeline is already in the requested state."""


class PipelineInTransitionError(ValidationError):
    """
    Raised when pipeline is currently transitioning and cannot perform the
    requested operation.
    """


class InvalidJsonError(ValidationError):
    """Raised when malformed JSON is provided in request body."""


class EmptyPipelineIdError(ValidationError):
    """Raised when pipeline ID parameter is empty or whitespace."""


class PipelineDeletionStateViolationError(ValidationError):
    """Raised when attempting to delete a pipeline that's not in a deletable state."""
