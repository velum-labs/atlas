"""Pipeline storage protocol combining query, cluster, and contract operations."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from alma_ports.cluster import ClusterWriter
from alma_ports.contract import ContractReader, ContractWriter
from alma_ports.query import QueryReader


@runtime_checkable
class PipelineStorage(QueryReader, ContractReader, ClusterWriter, ContractWriter, Protocol):
    """Storage protocol for the analysis pipeline.

    Combines query reads, contract reads/writes, and cluster writes used by
    pipeline and contract-engine workflows.
    """

    ...
