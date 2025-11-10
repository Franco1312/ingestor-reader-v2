"""Centralized error types."""


class IngestorError(Exception):
    """Base exception for ingestor errors."""
    pass


class ConfigError(IngestorError):
    """Configuration error."""
    pass


class StorageError(IngestorError):
    """Storage operation error."""
    pass
