"""Application-layer query services for the Atlas graph."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from alma_atlas.config import AtlasConfig
from alma_ports.annotation import AnnotationRecord
from alma_ports.asset import Asset
from alma_ports.edge import Edge as GraphEdge
from alma_ports.query import QueryObservation
from alma_ports.schema import SchemaSnapshot
from alma_ports.violation import Violation

_SUGGEST_TABLE_CANDIDATE_MULTIPLIER = 5
_SUGGEST_TABLE_MAX_CANDIDATES = 100


@dataclass(frozen=True)
class GraphStatusSummary:
    asset_count: int
    edge_count: int
    query_count: int
    kind_counts: dict[str, int]
    source_counts: dict[str, int]


@dataclass(frozen=True)
class LineageSummary:
    asset_exists: bool
    related: list[str]


@dataclass(frozen=True)
class ImpactSummary:
    asset_exists: bool
    downstream_assets: list[str]
    query_counts: dict[str, int]


class GraphReadService:
    """Session-scoped read facade over Atlas graph repositories."""

    def __init__(self, db: Any) -> None:
        from alma_atlas_store.session import AtlasStoreSession

        self._db = db
        self._session = AtlasStoreSession.from_db(db)
        self.assets = self._session.assets
        self.annotations = self._session.annotations
        self.edges = self._session.edges
        self.queries = self._session.queries
        self.schemas = self._session.schemas
        self.violations = self._session.violations
        self._lineage_graph = None
        self._edge_rows: list[GraphEdge] | None = None
        self._query_rows: list[QueryObservation] | None = None

    def search_assets(self, query: str, *, limit: int) -> list[Asset]:
        return self.assets.search(query)[:limit]

    def get_asset(self, asset_id: str) -> Asset | None:
        return self.assets.get(asset_id)

    def get_annotations(self, *, asset_id: str | None = None, limit: int = 100) -> list[AnnotationRecord]:
        if asset_id is not None:
            record = self.annotations.get(asset_id)
            return [record] if record is not None else []
        return self.annotations.list_all(limit=limit)

    def get_latest_schema(self, asset_id: str) -> tuple[Asset | None, SchemaSnapshot | None]:
        asset = self.get_asset(asset_id)
        if asset is None:
            return None, None
        return asset, self.schemas.get_latest(asset_id)

    def get_query_patterns(self, *, top_n: int) -> list[QueryObservation]:
        return self.queries.list_top(top_n)

    def suggest_tables(self, query: str, *, limit: int) -> list[tuple[float, Asset, set[str]]]:
        query_tokens = {token.lower() for token in query.split() if token}
        candidate_limit = min(
            max(limit * _SUGGEST_TABLE_CANDIDATE_MULTIPLIER, limit),
            _SUGGEST_TABLE_MAX_CANDIDATES,
        )
        assets = self.assets.search(query)[:candidate_limit]
        latest_snapshots = self.schemas.get_latest_many([asset.id for asset in assets])
        results: list[tuple[float, Asset, set[str]]] = []
        for asset in assets:
            col_names = self._column_names_for_asset(asset, latest_snapshots=latest_snapshots)
            if col_names and query_tokens:
                union = query_tokens | col_names
                jaccard = len(query_tokens & col_names) / len(union)
            else:
                jaccard = 0.0
            name_match = 1.0 if query.lower() in asset.name.lower() else 0.0
            score = 0.5 * name_match + 0.5 * jaccard
            results.append((score, asset, col_names))
        results.sort(key=lambda item: -item[0])
        return results[:limit]

    def list_violations(
        self,
        *,
        asset_id: str | None = None,
        limit: int = 50,
    ) -> list[Violation]:
        if asset_id is not None:
            return self.violations.list_for_asset(asset_id)[:limit]
        return self.violations.list_recent(limit=limit)

    def export_graph(self) -> dict[str, list[dict[str, Any]]]:
        assets = self.assets.list_all()
        edges = self._all_edges()
        return {
            "assets": [
                {
                    "id": asset.id,
                    "source": asset.source,
                    "kind": asset.kind,
                    "name": asset.name,
                    "description": asset.description,
                    "tags": asset.tags,
                    "metadata": asset.metadata,
                }
                for asset in assets
            ],
            "edges": [
                {
                    "upstream_id": edge.upstream_id,
                    "downstream_id": edge.downstream_id,
                    "kind": edge.kind,
                    "metadata": edge.metadata,
                }
                for edge in edges
            ],
        }

    def get_graph_status(self) -> GraphStatusSummary:
        assets = self.assets.list_all()
        edges = self._all_edges()
        queries = self._all_queries()

        kind_counts: dict[str, int] = {}
        source_counts: dict[str, int] = {}
        for asset in assets:
            kind_counts[asset.kind] = kind_counts.get(asset.kind, 0) + 1
            source_counts[asset.source] = source_counts.get(asset.source, 0) + 1

        return GraphStatusSummary(
            asset_count=len(assets),
            edge_count=len(edges),
            query_count=len(queries),
            kind_counts=kind_counts,
            source_counts=source_counts,
        )

    def get_lineage_summary(
        self,
        asset_id: str,
        *,
        direction: Literal["upstream", "downstream"],
        depth: int | None,
    ) -> LineageSummary:
        graph = self._lineage()
        if not graph.has_asset(asset_id):
            return LineageSummary(asset_exists=False, related=[])

        related = (
            graph.upstream(asset_id, depth=depth)
            if direction == "upstream"
            else graph.downstream(asset_id, depth=depth)
        )
        return LineageSummary(asset_exists=True, related=related)

    def get_impact_summary(
        self,
        asset_id: str,
        *,
        depth: int | None,
    ) -> ImpactSummary:
        asset = self.get_asset(asset_id)
        if asset is None:
            return ImpactSummary(asset_exists=False, downstream_assets=[], query_counts={})

        graph = self._lineage()
        if not graph.has_asset(asset_id):
            return ImpactSummary(asset_exists=False, downstream_assets=[], query_counts={})

        downstream = graph.downstream(asset_id, depth=depth)
        query_counts: dict[str, int] = {}
        for query in self._all_queries():
            for table in query.tables:
                if table in downstream:
                    query_counts[table] = query_counts.get(table, 0) + query.execution_count
        return ImpactSummary(asset_exists=True, downstream_assets=downstream, query_counts=query_counts)

    def _all_edges(self) -> list[GraphEdge]:
        if self._edge_rows is None:
            self._edge_rows = self.edges.list_all()
        return self._edge_rows

    def _all_queries(self) -> list[QueryObservation]:
        if self._query_rows is None:
            self._query_rows = self.queries.list_all()
        return self._query_rows

    def _lineage(self):
        if self._lineage_graph is None:
            from alma_analysis.lineage import compute_lineage

            self._lineage_graph = compute_lineage([_to_lineage_edge(edge) for edge in self._all_edges()])
        return self._lineage_graph

    def _column_names_for_asset(
        self,
        asset: Asset,
        *,
        latest_snapshots: dict[str, SchemaSnapshot] | None = None,
    ) -> set[str]:
        snapshot = (
            latest_snapshots.get(asset.id)
            if latest_snapshots is not None
            else self.schemas.get_latest(asset.id)
        )
        if snapshot:
            return {column.name.lower() for column in snapshot.columns}
        columns = asset.metadata.get("columns")
        if isinstance(columns, Sequence):
            return {
                str(column.get("name", "")).lower()
                for column in columns
                if isinstance(column, dict)
            }
        return set()


def require_db_path(cfg: AtlasConfig) -> Path:
    if cfg.db_path is None or not cfg.db_path.exists():
        raise ValueError("No Atlas database found. Run `alma-atlas scan` first.")
    return cfg.db_path


def search_assets(db_path: Path, query: str, *, limit: int) -> list[Asset]:
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return GraphReadService(db).search_assets(query, limit=limit)


def get_asset(db_path: Path, asset_id: str) -> Asset | None:
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return GraphReadService(db).get_asset(asset_id)


def get_annotations(db_path: Path, *, asset_id: str | None = None, limit: int = 100) -> list[AnnotationRecord]:
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return GraphReadService(db).get_annotations(asset_id=asset_id, limit=limit)


def get_latest_schema(db_path: Path, asset_id: str) -> tuple[Asset | None, SchemaSnapshot | None]:
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return GraphReadService(db).get_latest_schema(asset_id)


def get_query_patterns(db_path: Path, *, top_n: int) -> list[QueryObservation]:
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return GraphReadService(db).get_query_patterns(top_n=top_n)


def suggest_tables(db_path: Path, query: str, *, limit: int) -> list[tuple[float, Asset, set[str]]]:
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return GraphReadService(db).suggest_tables(query, limit=limit)


def list_violations(
    db_path: Path,
    *,
    asset_id: str | None = None,
    limit: int = 50,
) -> list[Violation]:
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return GraphReadService(db).list_violations(asset_id=asset_id, limit=limit)


def export_graph(db_path: Path) -> dict[str, list[dict[str, Any]]]:
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return GraphReadService(db).export_graph()


def get_graph_status(db_path: Path) -> GraphStatusSummary:
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return GraphReadService(db).get_graph_status()


def get_lineage_summary(
    db_path: Path,
    asset_id: str,
    *,
    direction: Literal["upstream", "downstream"],
    depth: int | None,
) -> LineageSummary:
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return GraphReadService(db).get_lineage_summary(asset_id, direction=direction, depth=depth)


def get_impact_summary(
    db_path: Path,
    asset_id: str,
    *,
    depth: int | None,
) -> ImpactSummary:
    from alma_atlas_store.db import Database

    with Database(db_path) as db:
        return GraphReadService(db).get_impact_summary(asset_id, depth=depth)


def _to_lineage_edge(edge: GraphEdge):
    from alma_analysis.lineage import Edge

    return Edge(
        upstream_id=edge.upstream_id,
        downstream_id=edge.downstream_id,
        kind=edge.kind,
    )
