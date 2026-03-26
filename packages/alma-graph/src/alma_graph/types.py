"""Canonical lineage graph data transfer objects."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class LineageNode:
    """A neutral lineage graph node."""

    id: str
    node_type: str
    name: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LineageEdge:
    """A neutral lineage graph edge."""

    id: str
    source: str
    target: str
    edge_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LineageQuery:
    """A grouped query pattern suitable for overlay or export use."""

    id: str
    fingerprint: str
    project_id: str
    sample_sql: str
    source_asset_ids: list[str]
    execution_count: int
    user_emails: list[str]
    avg_bytes: float | None = None
    avg_slot_ms: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LineageIssue:
    """A lineage extraction issue that should remain auditable."""

    issue_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Provenance:
    """Provenance for graph bundles and evidence overlays."""

    source_adapter: str
    extracted_at: datetime
    source_version: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidenceOverlay:
    """Behavioral or analytical data attached to a lineage graph."""

    overlay_type: str
    entries: list[dict[str, Any]]
    metadata: dict[str, Any] = field(default_factory=dict)
    provenance: Provenance | None = None


@dataclass
class LineageGraph:
    """A lineage graph plus exportable query patterns and issues."""

    nodes: list[LineageNode]
    edges: list[LineageEdge]
    queries: list[LineageQuery] = field(default_factory=list)
    issues: list[LineageIssue] = field(default_factory=list)

    def node_records(self) -> list[dict[str, Any]]:
        return [
            {
                "id": node.id,
                "node_type": node.node_type,
                "name": node.name,
                "metadata": node.metadata,
            }
            for node in self.nodes
        ]

    def edge_records(self) -> list[dict[str, Any]]:
        return [
            {
                "id": edge.id,
                "source": edge.source,
                "target": edge.target,
                "edge_type": edge.edge_type,
                "metadata": edge.metadata,
            }
            for edge in self.edges
        ]

    def query_records(self) -> list[dict[str, Any]]:
        return [
            {
                "id": query.id,
                "fingerprint": query.fingerprint,
                "project_id": query.project_id,
                "sample_sql": query.sample_sql,
                "source_asset_ids": query.source_asset_ids,
                "execution_count": query.execution_count,
                "user_emails": query.user_emails,
                "avg_bytes": query.avg_bytes,
                "avg_slot_ms": query.avg_slot_ms,
                "metadata": query.metadata,
            }
            for query in self.queries
        ]

    def issue_records(self) -> list[dict[str, Any]]:
        return [
            {
                "issue_type": issue.issue_type,
                "metadata": issue.metadata,
            }
            for issue in self.issues
        ]

    def to_dataframes(self) -> tuple[Any, Any, Any, Any]:
        import pandas as pd

        return (
            pd.DataFrame(self.node_records()),
            pd.DataFrame(self.edge_records()),
            pd.DataFrame(self.query_records()),
            pd.DataFrame(self.issue_records()),
        )

    def write_json(self, output_dir: str) -> None:
        from pathlib import Path

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        payloads = {
            "lineage_nodes.json": self.node_records(),
            "lineage_edges.json": self.edge_records(),
            "lineage_queries.json": self.query_records(),
            "lineage_issues.json": self.issue_records(),
        }
        for filename, payload in payloads.items():
            (output_path / filename).write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )


@dataclass(frozen=True)
class LineageSubgraph:
    """A filtered, bounded lineage subgraph for interactive visualization."""

    graph: Any
    seed_node_ids: list[str]
    node_limit: int
    truncated: bool = False
    omitted_nodes: int = 0
    filters: dict[str, Any] = field(default_factory=dict)

    def node_records(self) -> list[dict[str, Any]]:
        return [
            {
                "id": node_id,
                **dict(self.graph.nodes[node_id]),
                "degree": int(self.graph.degree(node_id)),
                "in_degree": int(self.graph.in_degree(node_id)),
                "out_degree": int(self.graph.out_degree(node_id)),
            }
            for node_id in sorted(self.graph.nodes)
        ]

    def edge_records(self) -> list[dict[str, Any]]:
        return [
            {
                "source": source,
                "target": target,
                **dict(data),
            }
            for source, target, data in sorted(self.graph.edges(data=True))
        ]

    def to_dataframes(self) -> tuple[Any, Any]:
        import pandas as pd

        return pd.DataFrame(self.node_records()), pd.DataFrame(self.edge_records())


@dataclass(frozen=True)
class GraphBundle:
    """Structural lineage graph plus overlays and provenance."""

    graph: LineageGraph
    overlays: list[EvidenceOverlay] = field(default_factory=list)
    provenance: Provenance | None = None
    transforms_applied: list[str] = field(default_factory=list)


def dataset_node_id(project_id: str, dataset_id: str) -> str:
    return f"{project_id}.{dataset_id}"


def asset_node_id(project_id: str, dataset_id: str, table_id: str) -> str:
    return f"{project_id}.{dataset_id}.{table_id}"


def consumer_node_id(user_email: str) -> str:
    return f"consumer:{(user_email or 'unknown').lower()}"


def query_pattern_node_id(project_id: str, fingerprint: str) -> str:
    return f"query:{project_id}:{fingerprint}"
