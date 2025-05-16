"""Custom exceptions for the Daily Excel Reports application."""


class BaseError(Exception):
    """Base exception for all application errors."""

    pass


class FileLoadError(BaseError):
    """Exception raised when a file cannot be loaded."""

    pass


class ValidationError(BaseError):
    """Exception raised when data fails validation."""

    pass


class TransformationError(BaseError):
    """Exception raised when data transformation fails."""

    pass


class ExportError(BaseError):
    """Exception raised when data export fails."""

    pass


class ConfigError(BaseError):
    """Exception raised when there is a configuration error."""

    pass
