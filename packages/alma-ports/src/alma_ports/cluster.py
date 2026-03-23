"""Cluster and lineage storage protocols."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@runtime_checkable
class ClusterReader(Protocol):
    """Read-only access to clusters and contract lineage."""

    def get_cluster(self, cluster_id: str | UUID) -> dict[str, Any] | None: ...

    def get_cluster_queries(
        self, cluster_id: str | UUID, *, limit: int = 100
    ) -> list[dict[str, Any]]: ...

    def get_contract_coverage(self, contract_id: str | UUID) -> list[dict[str, Any]]: ...

    def get_contract_coverage_count(self, contract_id: str | UUID) -> dict[str, int]: ...

    def get_contract_lineage(
        self, contract_id: str | UUID, *, sample_limit: int = 10
    ) -> dict[str, Any] | None: ...


@runtime_checkable
class ClusterWriter(Protocol):
    """Write access to clusters and coverage."""

    def insert_cluster(
        self,
        *,
        cluster_id: str | UUID,
        query_ids: list[str],
        total_weight: float = 0,
        common_relations: list[str] | None = None,
        common_joins: list[str] | None = None,
        common_predicates: list[str] | None = None,
        centroid_signature: dict[str, Any] | None = None,
    ) -> str: ...

    def update_queries_cluster(self, query_ids: list[str], cluster_id: str | UUID) -> int: ...

    def insert_contract_coverage(
        self,
        contract_id: str | UUID,
        query_hashes: list[str],
        coverage_type: str = "full",
        rewrite_cost: float | None = None,
    ) -> int: ...


@runtime_checkable
class ClusterRepository(ClusterReader, ClusterWriter, Protocol):
    """Full cluster storage."""

    ...
