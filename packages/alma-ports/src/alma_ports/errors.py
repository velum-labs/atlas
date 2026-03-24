"""Atlas error hierarchy — shared exception types for all Atlas packages.

Import from here to get a consistent, structured exception hierarchy that
makes it easy to catch errors at the right level of specificity.
"""

from __future__ import annotations


class AtlasError(Exception):
    """Base error for all Atlas operations."""


class ConfigurationError(AtlasError):
    """Invalid adapter or pipeline configuration."""


class ConnectionError(AtlasError):
    """Failed to connect to a data source."""


class AuthenticationError(ConnectionError):
    """Authentication or authorization failed."""


class ExtractionError(AtlasError):
    """Failed to extract data from a source."""


class TimeoutError(AtlasError):
    """Operation exceeded its timeout."""


class SyncError(AtlasError):
    """Failed to sync with remote server."""


class EnforcementError(AtlasError):
    """Failed to run enforcement checks."""


__all__ = [
    "AtlasError",
    "ConfigurationError",
    "ConnectionError",
    "AuthenticationError",
    "ExtractionError",
    "TimeoutError",
    "SyncError",
    "EnforcementError",
]
