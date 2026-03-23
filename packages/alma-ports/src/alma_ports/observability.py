"""Observability (coverage snapshots, deployment events) storage protocols."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@runtime_checkable
class ObservabilityReader(Protocol):
    """Read-only access to coverage and deployment data."""

    def list_coverage_snapshots(
        self, *, target_id: str, limit: int = 100
    ) -> list[dict[str, Any]]: ...

    def list_deployment_events(
        self,
        *,
        target_id: str | None = None,
        contract_id: str | UUID | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]: ...


@runtime_checkable
class ObservabilityWriter(Protocol):
    """Write access to coverage and deployment data."""

    def insert_coverage_snapshot(
        self,
        *,
        target_id: str,
        contract_coverage: float | None = None,
        traffic_coverage: float | None = None,
        interface_coverage: float | None = None,
        ingestion_coverage: float | None = None,
        shadow_test_coverage: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> UUID: ...

    def insert_deployment_event(
        self,
        *,
        target_id: str,
        git_sha: str,
        status: str,
        contract_id: str | UUID | None = None,
        artifact_hash: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> UUID: ...


@runtime_checkable
class ObservabilityRepository(ObservabilityReader, ObservabilityWriter, Protocol):
    """Full observability storage."""

    ...
