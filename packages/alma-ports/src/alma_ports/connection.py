"""Connection provider protocol for infrastructure boundaries."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Protocol, runtime_checkable

ConnectionT = Any


@runtime_checkable
class ConnectionProvider(Protocol):
    """Provides database connections without exposing pool internals."""

    @contextmanager
    def get_connection(self) -> ConnectionT:
        """Yield a database connection as a context manager."""
        ...
