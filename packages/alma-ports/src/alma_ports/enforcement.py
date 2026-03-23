"""Enforcement and exemption storage protocols."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class EnforcementReader(Protocol):
    """Read-only access to enforcement exemptions and readiness."""

    def list_enforcement_exemptions(
        self,
        *,
        target_id: str | None = None,
        gate_type: str | None = None,
        scope_key: str | None = None,
        active_only: bool = True,
        limit: int = 200,
    ) -> list[dict[str, Any]]: ...

    def list_expired_enforcement_exemptions(
        self,
        *,
        limit: int = 200,
        as_of: datetime | None = None,
        target_id: str | None = None,
        gate_type: str | None = None,
    ) -> list[dict[str, Any]]: ...

    def list_active_non_expiring_enforcement_exemptions(
        self,
        *,
        limit: int = 200,
        target_id: str | None = None,
        gate_type: str | None = None,
    ) -> list[dict[str, Any]]: ...

    def list_stale_active_enforcement_exemptions(
        self,
        *,
        limit: int = 200,
        created_before: datetime,
        active_at: datetime,
        target_id: str | None = None,
        gate_type: str | None = None,
    ) -> list[dict[str, Any]]: ...

    def get_pilot_readiness_summary(
        self, *, target_id: str | None = None
    ) -> dict[str, Any]: ...


@runtime_checkable
class EnforcementWriter(Protocol):
    """Write access to enforcement exemptions."""

    def upsert_enforcement_exemption(
        self,
        *,
        target_id: str,
        gate_type: str,
        reason: str,
        created_by: str,
        scope_key: str | None = None,
        enabled: bool = True,
        expires_at: datetime | None = None,
    ) -> dict[str, Any] | None: ...


@runtime_checkable
class EnforcementRepository(EnforcementReader, EnforcementWriter, Protocol):
    """Full enforcement storage."""

    ...
