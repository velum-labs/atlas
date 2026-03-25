"""Atlas error hierarchy — shared exception types for all Atlas packages.

Import from here to get a consistent, structured exception hierarchy that
makes it easy to catch errors at the right level of specificity.
"""

from __future__ import annotations


class AtlasError(Exception):
    """Base error for all Atlas operations."""


class ConfigurationError(AtlasError):
    """Invalid adapter or pipeline configuration."""


class AdapterConnectionError(AtlasError):
    """Failed to connect to a data source."""


class AuthenticationError(AdapterConnectionError):
    """Authentication or authorization failed."""


class ExtractionError(AtlasError):
    """Failed to extract data from a source."""


class AdapterTimeoutError(AtlasError):
    """Operation exceeded its timeout."""


class SyncError(AtlasError):
    """Failed to sync with remote server."""


class EnforcementError(AtlasError):
    """Failed to run enforcement checks."""


__all__ = [
    "AtlasError",
    "ConfigurationError",
    "AdapterConnectionError",
    "AuthenticationError",
    "ExtractionError",
    "AdapterTimeoutError",
    "SyncError",
    "EnforcementError",
]
