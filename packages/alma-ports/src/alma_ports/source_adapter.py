"""Source adapter storage protocols."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable

type SourceAdapterRow = dict[str, Any]
type SourceAdapterSecretRow = dict[str, Any]


@runtime_checkable
class SourceAdapterReader(Protocol):
    """Read-only access to source adapters."""

    def list_source_adapters(
        self,
        *,
        kind: str | None = None,
        target_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[SourceAdapterRow]: ...

    def get_source_adapter(self, *, id: str) -> SourceAdapterRow | None: ...

    def get_source_adapter_by_key(self, *, key: str) -> SourceAdapterRow | None: ...


@runtime_checkable
class SourceAdapterWriter(Protocol):
    """Write access to source adapters."""

    def create_source_adapter(
        self,
        *,
        id: str | None = None,
        key: str,
        display_name: str,
        kind: str,
        target_id: str,
        description: str | None = None,
        metadata: dict[str, str] | None = None,
        config: dict[str, object] | None = None,
        secrets: dict[str, SourceAdapterSecretRow] | None = None,
        status: str = "pending",
        status_message: str | None = None,
    ) -> SourceAdapterRow | None: ...

    def update_source_adapter(
        self,
        *,
        id: str,
        key: str,
        display_name: str,
        kind: str,
        target_id: str,
        description: str | None = None,
        metadata: dict[str, str] | None = None,
        config: dict[str, object] | None = None,
        secrets: dict[str, SourceAdapterSecretRow] | None = None,
        replace_secrets: bool = False,
        status: str | None = None,
        status_message: str | None = None,
    ) -> SourceAdapterRow | None: ...

    def disable_source_adapter(self, *, id: str) -> bool: ...

    def update_source_adapter_status(
        self,
        *,
        id: str,
        status: str,
        status_message: str | None = None,
        last_tested_at: datetime | None = None,
        last_observed_at: datetime | None = None,
        observation_cursor: dict[str, object] | None = None,
    ) -> bool: ...


@runtime_checkable
class SourceAdapterRepository(SourceAdapterReader, SourceAdapterWriter, Protocol):
    """Full source adapter storage."""

    ...
