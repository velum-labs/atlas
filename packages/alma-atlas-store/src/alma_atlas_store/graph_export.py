"""Atlas-owned importer/exporter for canonical lineage graphs."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from alma_graph import LineageGraph, LineageNode

_ATLAS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS assets (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    tags TEXT,
    metadata TEXT,
    first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
    last_seen TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS edges (
    id TEXT PRIMARY KEY,
    upstream_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    downstream_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    metadata TEXT,
    first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
    last_seen TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(upstream_id, downstream_id, kind)
);

CREATE TABLE IF NOT EXISTS schema_snapshots (
    id TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    columns TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    captured_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS queries (
    fingerprint TEXT PRIMARY KEY,
    sql_text TEXT NOT NULL,
    tables TEXT NOT NULL,
    source TEXT NOT NULL,
    first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
    last_seen TEXT DEFAULT CURRENT_TIMESTAMP,
    execution_count INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS consumers (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    source TEXT NOT NULL,
    metadata TEXT,
    first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
    last_seen TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS consumer_assets (
    consumer_id TEXT NOT NULL REFERENCES consumers(id) ON DELETE CASCADE,
    asset_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    PRIMARY KEY (consumer_id, asset_id)
);
"""


@dataclass
class AtlasExport:
    """Atlas-compatible export payload."""

    assets: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    queries: list[dict[str, Any]]
    consumers: list[dict[str, Any]]
    consumer_assets: list[dict[str, Any]]
    schema_snapshots: list[dict[str, Any]] = field(default_factory=list)

    def write_json(self, output_dir: str | Path) -> None:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        payloads = {
            "atlas_assets.json": self.assets,
            "atlas_edges.json": self.edges,
            "atlas_queries.json": self.queries,
            "atlas_consumers.json": self.consumers,
            "atlas_consumer_assets.json": self.consumer_assets,
            "atlas_schema_snapshots.json": self.schema_snapshots,
        }
        for filename, payload in payloads.items():
            (output_path / filename).write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )

    def write_sqlite(self, db_path: str | Path) -> None:
        db_file = Path(db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_file) as conn:
            conn.executescript(_ATLAS_SCHEMA_SQL)

            conn.executemany(
                """
                INSERT OR REPLACE INTO assets (id, source, kind, name, description, tags, metadata)
                VALUES (:id, :source, :kind, :name, :description, :tags, :metadata)
                """,
                self.assets,
            )
            conn.executemany(
                """
                INSERT OR REPLACE INTO edges (id, upstream_id, downstream_id, kind, metadata)
                VALUES (:id, :upstream_id, :downstream_id, :kind, :metadata)
                """,
                self.edges,
            )
            conn.executemany(
                """
                INSERT OR REPLACE INTO queries (fingerprint, sql_text, tables, source, execution_count)
                VALUES (:fingerprint, :sql_text, :tables, :source, :execution_count)
                """,
                self.queries,
            )
            conn.executemany(
                """
                INSERT OR REPLACE INTO consumers (id, kind, name, source, metadata)
                VALUES (:id, :kind, :name, :source, :metadata)
                """,
                self.consumers,
            )
            conn.executemany(
                """
                INSERT OR REPLACE INTO consumer_assets (consumer_id, asset_id)
                VALUES (:consumer_id, :asset_id)
                """,
                self.consumer_assets,
            )
            conn.executemany(
                """
                INSERT OR REPLACE INTO schema_snapshots (id, asset_id, columns, fingerprint, captured_at)
                VALUES (:id, :asset_id, :columns, :fingerprint, :captured_at)
                """,
                self.schema_snapshots,
            )


def build_atlas_export(graph: LineageGraph, *, source_id: str = "offline-lineage") -> AtlasExport:
    """Build an Atlas-compatible export payload from a canonical lineage graph."""

    asset_records: dict[str, dict[str, Any]] = {}
    consumer_records: dict[str, dict[str, Any]] = {}
    query_asset_records: dict[str, dict[str, Any]] = {}
    edge_records: dict[str, dict[str, Any]] = {}
    consumer_asset_records: dict[tuple[str, str], dict[str, str]] = {}
    schema_snapshot_records: dict[str, dict[str, Any]] = {}
    node_by_id = {node.id: node for node in graph.nodes}

    for node in graph.nodes:
        if node.node_type == "consumer":
            consumer_records[node.id] = {
                "id": node.id,
                "kind": str(node.metadata.get("consumer_kind") or "user"),
                "name": node.name,
                "source": source_id,
                "metadata": _json(node.metadata),
            }
            query_asset_id = _atlas_query_asset_id(source_id, node.name)
            query_asset_records[query_asset_id] = {
                "id": query_asset_id,
                "source": source_id,
                "kind": "query",
                "name": node.name,
                "description": "",
                "tags": _json([]),
                "metadata": _json(
                    {
                        "consumer_id": node.id,
                        "consumer_kind": node.metadata.get("consumer_kind"),
                        "user_email": node.metadata.get("user_email", node.name),
                    }
                ),
            }
            continue

        if node.node_type not in {"dataset", "table", "view", "materialized_view"}:
            continue

        atlas_asset_id = _atlas_asset_id(source_id, node.id)
        asset_records[atlas_asset_id] = {
            "id": atlas_asset_id,
            "source": source_id,
            "kind": _atlas_kind(node),
            "name": node.name,
            "description": "",
            "tags": _tags_json(node),
            "metadata": _json(node.metadata),
        }

        schema_snapshot = _schema_snapshot_for_node(node, atlas_asset_id)
        if schema_snapshot is not None:
            schema_snapshot_records[schema_snapshot["id"]] = schema_snapshot

    asset_records.update(query_asset_records)

    for edge in graph.edges:
        kind = _atlas_edge_kind(edge.edge_type)
        if edge.edge_type == "reads":
            upstream_id = _atlas_asset_id(source_id, edge.source)
            consumer_node = node_by_id.get(edge.target)
            if consumer_node is None:
                continue
            downstream_id = _atlas_query_asset_id(source_id, consumer_node.name)
            edge_id = f"{upstream_id}:{downstream_id}:{kind}"
            edge_records[edge_id] = {
                "id": edge_id,
                "upstream_id": upstream_id,
                "downstream_id": downstream_id,
                "kind": kind,
                "metadata": _json({**edge.metadata, "original_edge_type": edge.edge_type}),
            }
            consumer_asset_records[(edge.target, upstream_id)] = {
                "consumer_id": edge.target,
                "asset_id": upstream_id,
            }
            continue

        upstream_id = _atlas_asset_id(source_id, edge.source)
        downstream_id = _atlas_asset_id(source_id, edge.target)
        edge_id = f"{upstream_id}:{downstream_id}:{kind}"
        edge_records[edge_id] = {
            "id": edge_id,
            "upstream_id": upstream_id,
            "downstream_id": downstream_id,
            "kind": kind,
            "metadata": _json({**edge.metadata, "original_edge_type": edge.edge_type}),
        }

    query_records: dict[str, dict[str, Any]] = {}
    for query in graph.queries:
        atlas_fingerprint = f"{source_id}:{query.project_id}:{query.fingerprint}"
        tables = [_atlas_asset_id(source_id, asset_id) for asset_id in query.source_asset_ids]
        query_records[atlas_fingerprint] = {
            "fingerprint": atlas_fingerprint,
            "sql_text": query.sample_sql,
            "tables": _json(tables),
            "source": source_id,
            "execution_count": query.execution_count,
        }

    return AtlasExport(
        assets=sorted(asset_records.values(), key=lambda row: row["id"]),
        edges=sorted(edge_records.values(), key=lambda row: row["id"]),
        queries=sorted(query_records.values(), key=lambda row: row["fingerprint"]),
        consumers=sorted(consumer_records.values(), key=lambda row: row["id"]),
        consumer_assets=sorted(
            consumer_asset_records.values(),
            key=lambda row: (row["consumer_id"], row["asset_id"]),
        ),
        schema_snapshots=sorted(schema_snapshot_records.values(), key=lambda row: row["id"]),
    )


def _atlas_asset_id(source_id: str, node_id: str) -> str:
    return f"{source_id}::{node_id}"


def _atlas_query_asset_id(source_id: str, user_email: str) -> str:
    return f"{source_id}::query::{(user_email or 'unknown').lower()}"


def _atlas_kind(node: LineageNode) -> str:
    if node.node_type == "materialized_view":
        return "materialized_view"
    return node.node_type


def _atlas_edge_kind(edge_type: str) -> str:
    if edge_type == "view_depends_on":
        return "depends_on"
    return edge_type


def _schema_snapshot_for_node(node: LineageNode, atlas_asset_id: str) -> dict[str, Any] | None:
    fields = node.metadata.get("schema_fields")
    if not isinstance(fields, list) or not fields:
        return None

    columns = [
        {
            "name": field.get("name", ""),
            "type": field.get("type", ""),
            "nullable": field.get("mode", "NULLABLE") != "REQUIRED",
            "description": field.get("description", ""),
        }
        for field in fields
    ]
    payload = json.dumps(columns, sort_keys=True)
    fingerprint = hashlib.sha256(payload.encode()).hexdigest()
    return {
        "id": f"{atlas_asset_id}:{fingerprint}",
        "asset_id": atlas_asset_id,
        "columns": payload,
        "fingerprint": fingerprint,
        "captured_at": datetime.now(tz=UTC).isoformat(),
    }


def _tags_json(node: LineageNode) -> str:
    tags: list[str] = []
    label_maturity = node.metadata.get("label_maturity")
    label_producer = node.metadata.get("label_producer")
    if label_maturity:
        tags.append(f"maturity:{label_maturity}")
    if label_producer:
        tags.append(f"producer:{label_producer}")
    return _json(tags)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True)
