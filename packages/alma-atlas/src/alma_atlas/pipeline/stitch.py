"""Stitch pipeline — derives lineage edges from query traffic observations.

For each query observation in a TrafficObservationResult:
    1. Parse the SQL to extract referenced tables (via alma-sqlkit).
    2. Fingerprint the query (via alma-algebrakit).
    3. For each referenced table, create an upstream -> consumer edge.
    4. Upsert query fingerprints and edges into the Atlas store.

Returns the number of new or updated edges written.
"""

from __future__ import annotations

import hashlib
import logging
import re

from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import Edge, EdgeRepository
from alma_atlas_store.query_repository import QueryObservation, QueryRepository
from alma_connectors.source_adapter import TrafficObservationResult

logger = logging.getLogger(__name__)

_WS_RE = re.compile(r"\s+")


def _normalize_ref(ref: str) -> str:
    """Strip SQL identifier quoting (double-quotes) from table references."""
    return ref.replace('"', '')


def _is_system_table(ref: str) -> bool:
    """Return True for BigQuery system tables (INFORMATION_SCHEMA, etc.)."""
    return "INFORMATION_SCHEMA" in ref.upper()


def _fingerprint(sql: str) -> str:
    """Return a 16-char hex fingerprint of normalised SQL."""
    normalised = _WS_RE.sub(" ", sql.strip().lower())
    return hashlib.sha256(normalised.encode()).hexdigest()[:16]


def stitch(
    traffic: TrafficObservationResult,
    db: Database,
    *,
    source_id: str = "",
    source_kind: str = "",
) -> int:
    """Derive and persist lineage edges from a set of query observations.

    Args:
        traffic:     Query observations returned by a source adapter.
        db:          Open Atlas database connection.
        source_id:   Identifier for the source (used as consumer fallback).
        source_kind: Dialect hint for SQL parsing (e.g. "bigquery", "postgres").

    Returns:
        Total number of edges written (new + updated).
    """
    from alma_analysis.edges import Edge as AnalysisEdge
    from alma_analysis.edges import extract_edges
    from alma_sqlkit.dialect import Dialect

    asset_repo = AssetRepository(db)
    edge_repo = EdgeRepository(db)
    query_repo = QueryRepository(db)

    dialect = Dialect.from_name(source_kind) if source_kind else Dialect.postgres()

    edges_written = 0

    for event in traffic.events:
        if not event.sql.strip():
            continue

        consumer_id = f"{source_id}::query::{event.database_user or 'unknown'}"
        # Ensure the consumer (query actor) asset exists so FK constraints are satisfied.
        asset_repo.upsert(Asset(
            id=consumer_id,
            source=source_id,
            kind="query",
            name=event.database_user or "unknown",
        ))

        try:
            derived: list[AnalysisEdge] = extract_edges(
                sql=event.sql,
                consumer_id=consumer_id,
                dialect=dialect,
            )
        except Exception as exc:
            logger.debug("Skipping query edge extraction for %s: %s", consumer_id, exc)
            continue

        for ae in derived:
            upstream_ref = _normalize_ref(ae.upstream_id)
            downstream_ref = _normalize_ref(ae.downstream_id)

            if _is_system_table(upstream_ref) or _is_system_table(downstream_ref):
                continue

            upstream = f"{source_id}::{upstream_ref}"
            downstream = (
                consumer_id
                if ae.downstream_id == consumer_id
                else f"{source_id}::{downstream_ref}"
            )
            # Ensure both endpoints exist as assets (query traffic may reference
            # tables not yet captured during schema introspection).
            asset_repo.upsert(Asset(
                id=upstream,
                source=source_id,
                kind="table",
                name=upstream_ref,
            ))
            if downstream != consumer_id:
                asset_repo.upsert(Asset(
                    id=downstream,
                    source=source_id,
                    kind="table",
                    name=downstream_ref,
                ))
            edge_repo.upsert(
                Edge(
                    upstream_id=upstream,
                    downstream_id=downstream,
                    kind=ae.kind,
                )
            )
            edges_written += 1

        if derived:
            tables = [
                f"{source_id}::{_normalize_ref(ae.upstream_id)}"
                for ae in derived
                if not _is_system_table(ae.upstream_id)
            ]
            query_repo.upsert(
                QueryObservation(
                    fingerprint=_fingerprint(event.sql),
                    sql_text=event.sql,
                    tables=tables,
                    source=source_id,
                )
            )

    return edges_written
