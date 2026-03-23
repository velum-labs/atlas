"""Stitch pipeline — derives lineage edges from query traffic observations.

For each query observation in a TrafficObservationResult:
    1. Parse the SQL to extract referenced tables (via alma-sqlkit).
    2. Fingerprint the query (via alma-algebrakit).
    3. For each referenced table, create an upstream → consumer edge.
    4. Upsert query fingerprints and edges into the Atlas store.

Returns the number of new or updated edges written.
"""

from __future__ import annotations

from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import Edge, EdgeRepository
from alma_atlas_store.query_repository import QueryObservation, QueryRepository
from alma_connectors.domain import TrafficObservationResult


def stitch(traffic: TrafficObservationResult, db: Database) -> int:
    """Derive and persist lineage edges from a set of query observations.

    Args:
        traffic: Query observations returned by a source adapter.
        db:      Open Atlas database connection.

    Returns:
        Total number of edges written (new + updated).
    """
    from alma_analysis.edges import Edge as AnalysisEdge
    from alma_analysis.edges import extract_edges
    from alma_sqlkit.dialect import Dialect

    edge_repo = EdgeRepository(db)
    query_repo = QueryRepository(db)

    dialect_map: dict[str, Dialect] = {
        "bigquery": Dialect.BIGQUERY,
        "snowflake": Dialect.SNOWFLAKE,
        "postgres": Dialect.POSTGRES,
    }
    dialect = dialect_map.get(traffic.source_type, Dialect.ANSI)

    edges_written = 0

    for query in traffic.queries:
        if not query.sql.strip():
            continue

        consumer_id = f"{traffic.source_id}::query::{query.user or 'unknown'}"

        try:
            derived: list[AnalysisEdge] = extract_edges(
                sql=query.sql,
                consumer_id=consumer_id,
                dialect=dialect,
            )
        except Exception:
            continue

        for ae in derived:
            edge_repo.upsert(
                Edge(
                    upstream_id=ae.upstream_id,
                    downstream_id=ae.downstream_id,
                    kind=ae.kind,
                )
            )
            edges_written += 1

        if derived:
            tables = [ae.upstream_id for ae in derived]
            query_repo.upsert(
                QueryObservation(
                    fingerprint=derived[0].query_fingerprint or "",
                    sql_text=query.sql,
                    tables=tables,
                    source=traffic.source_id,
                )
            )

    return edges_written
