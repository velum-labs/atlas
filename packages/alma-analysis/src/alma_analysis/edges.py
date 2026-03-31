"""Edge extraction from SQL for lineage stitching.

Provides a simple Edge dataclass and extract_edges() function that wraps
the lineage_extractor to derive upstream→downstream table edges from SQL.
"""

from __future__ import annotations

from dataclasses import dataclass

from alma_sqlkit.lineage import extract_lineage


@dataclass
class Edge:
    """A directed lineage edge between two data assets."""

    upstream_id: str
    downstream_id: str
    kind: str = "reads"
    query_fingerprint: str | None = None


def extract_edges(sql: str, consumer_id: str, dialect: object = "postgres") -> list[Edge]:
    """Extract lineage edges from a SQL query.

    Args:
        sql:         SQL query string.
        consumer_id: Identifier for the downstream consumer (table or query).
        dialect:     SQL dialect name (str) or a Dialect object with a ``name`` attribute.

    Returns:
        A list of Edge objects where each entry is upstream_table → consumer_id.
        If the SQL declares an explicit write target (INSERT INTO / CREATE TABLE AS),
        that target is used as downstream_id instead of consumer_id.
    """
    dialect_name = getattr(dialect, "name", str(dialect))
    result = extract_lineage(sql, dialect=dialect_name)

    target = result.target_table or consumer_id

    edges: list[Edge] = []
    for table_ref in result.source_tables:
        if table_ref.canonical_name.lower() in result.cte_names:
            continue
        edges.append(
            Edge(
                upstream_id=table_ref.canonical_name,
                downstream_id=target,
                kind="reads",
            )
        )
    return edges
