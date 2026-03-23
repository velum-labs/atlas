"""Edge extraction for alma-analysis.

Derives dependency edges from query observations by parsing the SQL and
mapping referenced tables to known asset IDs.

The core operation: for each query, extract the tables it reads, then for
each table, create an edge from that table (upstream) to the query author
or consuming asset (downstream).
"""

from __future__ import annotations

from dataclasses import dataclass

from alma_algebrakit.bound.fingerprint import fingerprint_sql
from alma_sqlkit.dialect import Dialect
from alma_sqlkit.parse import extract_tables


@dataclass
class Edge:
    """A directed dependency edge between two assets."""

    upstream_id: str
    downstream_id: str
    kind: str = "reads"
    query_fingerprint: str | None = None


def extract_edges(
    sql: str,
    consumer_id: str,
    dialect: Dialect | str = Dialect.ANSI,
    known_asset_ids: set[str] | None = None,
) -> list[Edge]:
    """Extract dependency edges from a SQL query.

    For each table referenced in the SQL, creates an edge from that table
    (upstream) to the consumer (downstream).

    Args:
        sql: Raw SQL string.
        consumer_id: The ID of the asset or consumer that executes this query.
        dialect: SQL dialect for parsing.
        known_asset_ids: Optional set of known asset IDs to filter against.
                         If provided, only tables in this set produce edges.

    Returns:
        List of Edge objects representing the read dependencies.
    """
    tables = extract_tables(sql, dialect=dialect)
    fingerprint = fingerprint_sql(sql, dialect=dialect)

    edges: list[Edge] = []
    for table in tables:
        if known_asset_ids is not None and table not in known_asset_ids:
            continue
        edges.append(
            Edge(
                upstream_id=table,
                downstream_id=consumer_id,
                kind="reads",
                query_fingerprint=fingerprint,
            )
        )
    return edges
