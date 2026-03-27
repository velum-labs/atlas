"""Atlas analysis snapshot builders and DTOs."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import alma_atlas
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.consumer_repository import Consumer, ConsumerRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import Edge, EdgeRepository
from alma_atlas_store.query_repository import QueryObservation, QueryRepository
from alma_atlas_store.schema_repository import SchemaRepository, SchemaSnapshot


@dataclass(frozen=True)
class AssetRecord:
    id: str
    source: str
    kind: str
    name: str
    description: str | None
    tags: list[str]
    metadata: dict[str, Any]
    first_seen: str | None
    last_seen: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "kind": self.kind,
            "name": self.name,
            "description": self.description,
            "tags": self.tags,
            "metadata": self.metadata,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
        }


@dataclass(frozen=True)
class EdgeRecord:
    upstream_id: str
    downstream_id: str
    kind: str
    metadata: dict[str, Any]
    first_seen: str | None
    last_seen: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "upstream_id": self.upstream_id,
            "downstream_id": self.downstream_id,
            "kind": self.kind,
            "metadata": self.metadata,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
        }


@dataclass(frozen=True)
class QueryRecord:
    fingerprint: str
    sql_text: str
    tables: list[str]
    source: str
    first_seen: str | None
    last_seen: str | None
    execution_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "fingerprint": self.fingerprint,
            "sql_text": self.sql_text,
            "tables": self.tables,
            "source": self.source,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "execution_count": self.execution_count,
        }


@dataclass(frozen=True)
class ConsumerRecord:
    id: str
    kind: str
    name: str
    source: str
    metadata: dict[str, Any]
    first_seen: str | None
    last_seen: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "name": self.name,
            "source": self.source,
            "metadata": self.metadata,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
        }


@dataclass(frozen=True)
class ConsumerAssetRecord:
    consumer_id: str
    asset_id: str

    def to_dict(self) -> dict[str, str]:
        return {
            "consumer_id": self.consumer_id,
            "asset_id": self.asset_id,
        }


@dataclass(frozen=True)
class SchemaSnapshotRecord:
    asset_id: str
    columns: list[dict[str, Any]]
    fingerprint: str
    captured_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "columns": self.columns,
            "fingerprint": self.fingerprint,
            "captured_at": self.captured_at,
        }


@dataclass(frozen=True)
class TopTableSummary:
    asset_id: str
    query_fingerprint_count: int
    execution_count: int

    def to_dict(self) -> dict[str, int | str]:
        return {
            "asset_id": self.asset_id,
            "query_fingerprint_count": self.query_fingerprint_count,
            "execution_count": self.execution_count,
        }


@dataclass(frozen=True)
class TopFingerprintSummary:
    fingerprint: str
    execution_count: int
    source: str
    tables: list[str]
    sample_sql: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "fingerprint": self.fingerprint,
            "execution_count": self.execution_count,
            "source": self.source,
            "tables": self.tables,
            "sample_sql": self.sample_sql,
        }


@dataclass(frozen=True)
class TrafficSummary:
    query_fingerprint_count: int
    total_query_executions: int
    asset_count: int
    edge_count: int
    top_tables_by_execution_count: list[TopTableSummary] = field(default_factory=list)
    top_fingerprints: list[TopFingerprintSummary] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "query_fingerprint_count": self.query_fingerprint_count,
            "total_query_executions": self.total_query_executions,
            "asset_count": self.asset_count,
            "edge_count": self.edge_count,
            "top_tables_by_execution_count": [
                entry.to_dict() for entry in self.top_tables_by_execution_count
            ],
            "top_fingerprints": [entry.to_dict() for entry in self.top_fingerprints],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)


@dataclass(frozen=True)
class AnalysisGraph:
    assets: list[AssetRecord] = field(default_factory=list)
    edges: list[EdgeRecord] = field(default_factory=list)
    queries: list[QueryRecord] = field(default_factory=list)
    consumers: list[ConsumerRecord] = field(default_factory=list)
    consumer_assets: list[ConsumerAssetRecord] = field(default_factory=list)
    schema_snapshots: list[SchemaSnapshotRecord] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "assets": [asset.to_dict() for asset in self.assets],
            "edges": [edge.to_dict() for edge in self.edges],
            "queries": [query.to_dict() for query in self.queries],
            "consumers": [consumer.to_dict() for consumer in self.consumers],
            "consumer_assets": [entry.to_dict() for entry in self.consumer_assets],
            "schema_snapshots": [snapshot.to_dict() for snapshot in self.schema_snapshots],
        }


@dataclass(frozen=True)
class AnalysisSnapshot:
    snapshot_version: str
    exported_at: str
    atlas_version: str
    source_filter: str | None
    graph: AnalysisGraph
    traffic_summary: TrafficSummary
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_version": self.snapshot_version,
            "exported_at": self.exported_at,
            "atlas_version": self.atlas_version,
            "source_filter": self.source_filter,
            "graph": self.graph.to_dict(),
            "traffic_summary": self.traffic_summary.to_dict(),
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)

    def write_json(self, output: str | Path) -> None:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(self.to_json(), encoding="utf-8")


def build_analysis_snapshot(
    db: Database,
    *,
    source: str | None = None,
    top_n: int = 10,
) -> AnalysisSnapshot:
    """Build a machine-readable analysis snapshot from the Atlas store."""

    top_n = max(1, top_n)

    asset_repo = AssetRepository(db)
    edge_repo = EdgeRepository(db)
    query_repo = QueryRepository(db)
    consumer_repo = ConsumerRepository(db)
    schema_repo = SchemaRepository(db)

    assets = asset_repo.list_all()
    if source is not None:
        assets = [asset for asset in assets if asset.source == source]
    asset_ids = {asset.id for asset in assets}

    edges = edge_repo.list_all()
    if source is not None:
        edges = [
            edge
            for edge in edges
            if edge.upstream_id in asset_ids and edge.downstream_id in asset_ids
        ]

    queries = query_repo.list_all(source=source)
    consumers = consumer_repo.list_all()
    if source is not None:
        consumers = [consumer for consumer in consumers if consumer.source == source]

    graph = AnalysisGraph(
        assets=[_asset_record(asset) for asset in assets],
        edges=[_edge_record(edge) for edge in edges],
        queries=[_query_record(query) for query in queries],
        consumers=[_consumer_record(consumer) for consumer in consumers],
        consumer_assets=_consumer_asset_records(consumers, asset_ids),
        schema_snapshots=_schema_snapshot_records(schema_repo, assets),
    )

    traffic_summary = build_analysis_summary(
        db,
        source=source,
        top_n=top_n,
        asset_count=len(graph.assets),
        edge_count=len(graph.edges),
        queries=queries,
    )

    return AnalysisSnapshot(
        snapshot_version="1",
        exported_at=datetime.now(tz=UTC).isoformat(),
        atlas_version=alma_atlas.__version__,
        source_filter=source,
        graph=graph,
        traffic_summary=traffic_summary,
        metadata={},
    )


def build_analysis_summary(
    db: Database,
    *,
    source: str | None = None,
    top_n: int = 10,
    asset_count: int | None = None,
    edge_count: int | None = None,
    queries: list[QueryObservation] | None = None,
) -> TrafficSummary:
    """Build traffic summary metadata from Atlas query observations."""

    top_n = max(1, top_n)
    asset_repo = AssetRepository(db)
    edge_repo = EdgeRepository(db)
    query_repo = QueryRepository(db)

    all_queries = queries if queries is not None else query_repo.list_all(source=source)
    total_query_executions = query_repo.sum_execution_count(source=source)
    query_fingerprint_count = query_repo.count_all(source=source)
    top_queries = query_repo.list_top(limit=top_n, source=source)

    if asset_count is None or edge_count is None:
        assets = asset_repo.list_all()
        if source is not None:
            assets = [asset for asset in assets if asset.source == source]
        asset_ids = {asset.id for asset in assets}
        if asset_count is None:
            asset_count = len(assets)
        if edge_count is None:
            edges = edge_repo.list_all()
            if source is not None:
                edges = [
                    edge
                    for edge in edges
                    if edge.upstream_id in asset_ids and edge.downstream_id in asset_ids
                ]
            edge_count = len(edges)

    table_stats: dict[str, TopTableSummary] = {}
    for query in all_queries:
        for asset_id in set(query.tables):
            current = table_stats.get(asset_id)
            if current is None:
                table_stats[asset_id] = TopTableSummary(
                    asset_id=asset_id,
                    query_fingerprint_count=1,
                    execution_count=query.execution_count,
                )
                continue
            table_stats[asset_id] = TopTableSummary(
                asset_id=asset_id,
                query_fingerprint_count=current.query_fingerprint_count + 1,
                execution_count=current.execution_count + query.execution_count,
            )

    sorted_tables = sorted(
        table_stats.values(),
        key=lambda entry: (-entry.execution_count, entry.asset_id),
    )

    return TrafficSummary(
        query_fingerprint_count=query_fingerprint_count,
        total_query_executions=total_query_executions,
        asset_count=asset_count if asset_count is not None else 0,
        edge_count=edge_count if edge_count is not None else 0,
        top_tables_by_execution_count=sorted_tables[:top_n],
        top_fingerprints=[_top_fingerprint_record(query) for query in top_queries],
    )


def _asset_record(asset: Asset) -> AssetRecord:
    return AssetRecord(
        id=asset.id,
        source=asset.source,
        kind=asset.kind,
        name=asset.name,
        description=asset.description,
        tags=asset.tags,
        metadata=asset.metadata,
        first_seen=asset.first_seen,
        last_seen=asset.last_seen,
    )


def _edge_record(edge: Edge) -> EdgeRecord:
    return EdgeRecord(
        upstream_id=edge.upstream_id,
        downstream_id=edge.downstream_id,
        kind=edge.kind,
        metadata=edge.metadata,
        first_seen=edge.first_seen,
        last_seen=edge.last_seen,
    )


def _query_record(query: QueryObservation) -> QueryRecord:
    return QueryRecord(
        fingerprint=query.fingerprint,
        sql_text=query.sql_text,
        tables=query.tables,
        source=query.source,
        first_seen=query.first_seen,
        last_seen=query.last_seen,
        execution_count=query.execution_count,
    )


def _consumer_record(consumer: Consumer) -> ConsumerRecord:
    return ConsumerRecord(
        id=consumer.id,
        kind=consumer.kind,
        name=consumer.name,
        source=consumer.source,
        metadata=consumer.metadata,
        first_seen=consumer.first_seen,
        last_seen=consumer.last_seen,
    )


def _consumer_asset_records(
    consumers: list[Consumer],
    asset_ids: set[str],
) -> list[ConsumerAssetRecord]:
    records: list[ConsumerAssetRecord] = []
    for consumer in consumers:
        for asset_id in consumer.asset_ids:
            if asset_ids and asset_id not in asset_ids:
                continue
            records.append(
                ConsumerAssetRecord(
                    consumer_id=consumer.id,
                    asset_id=asset_id,
                )
            )
    return sorted(records, key=lambda entry: (entry.consumer_id, entry.asset_id))


def _schema_snapshot_records(
    schema_repo: SchemaRepository,
    assets: list[Asset],
) -> list[SchemaSnapshotRecord]:
    records: list[SchemaSnapshotRecord] = []
    for asset in assets:
        snapshot = schema_repo.get_latest(asset.id)
        if snapshot is None:
            continue
        records.append(_schema_snapshot_record(snapshot))
    return records


def _schema_snapshot_record(snapshot: SchemaSnapshot) -> SchemaSnapshotRecord:
    return SchemaSnapshotRecord(
        asset_id=snapshot.asset_id,
        columns=[
            {
                "name": column.name,
                "type": column.type,
                "nullable": column.nullable,
                "description": column.description,
            }
            for column in snapshot.columns
        ],
        fingerprint=snapshot.fingerprint,
        captured_at=snapshot.captured_at,
    )


def _top_fingerprint_record(query: QueryObservation) -> TopFingerprintSummary:
    return TopFingerprintSummary(
        fingerprint=query.fingerprint,
        execution_count=query.execution_count,
        source=query.source,
        tables=query.tables,
        sample_sql=query.sql_text,
    )
