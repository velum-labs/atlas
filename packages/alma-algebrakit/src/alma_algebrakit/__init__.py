"""alma-algebrakit — SQL algebraic fingerprinting for Alma Atlas.

Provides tools for computing stable, comparable fingerprints of SQL queries
by reducing them to their algebraic structure. Two queries that read the same
tables with the same join/filter structure produce the same fingerprint,
regardless of formatting, alias choices, or literal values.

This enables deduplication of query traffic observations, detection of
equivalent queries across different BI tools, and grouping of related queries.
"""

__version__ = "0.1.0"

from alma_algebrakit.algebra import to_algebra
from alma_algebrakit.fingerprint import fingerprint_sql

__all__ = [
    "fingerprint_sql",
    "to_algebra",
]
