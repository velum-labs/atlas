"""alma-sqlkit — SQL parsing and normalization utilities for Alma Atlas.

Provides helpers for parsing SQL statements into ASTs, normalizing queries
for comparison, extracting referenced tables and columns, and formatting
SQL for display.

Built on top of sqlglot for broad dialect support (BigQuery, Snowflake,
Postgres, DuckDB, and more).
"""

__version__ = "0.1.0"

from alma_sqlkit.dialect import Dialect
from alma_sqlkit.normalize import normalize_sql
from alma_sqlkit.parse import extract_tables, parse_sql

__all__ = [
    "Dialect",
    "extract_tables",
    "normalize_sql",
    "parse_sql",
]
