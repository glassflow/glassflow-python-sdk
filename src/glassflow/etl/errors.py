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
