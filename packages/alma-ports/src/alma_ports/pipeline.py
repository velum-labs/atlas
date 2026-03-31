"""Pipeline storage protocol combining query, cluster, and contract operations."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from alma_ports.cluster import ClusterRepository
from alma_ports.contract import ContractRepository
from alma_ports.query import QueryRepository


@runtime_checkable
class PipelineStorage(QueryRepository, ContractRepository, ClusterRepository, Protocol):
    """Storage protocol for the analysis pipeline.

    Combines query reads, contract reads/writes, and cluster writes used by
    pipeline and contract-engine workflows.
    """

    ...
