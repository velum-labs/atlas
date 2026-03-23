"""SQL dialect definitions for alma-sqlkit.

Maps Alma Atlas source type names to sqlglot dialect identifiers.
"""

from __future__ import annotations

from enum import StrEnum


class Dialect(StrEnum):
    """Supported SQL dialects, mapped to sqlglot dialect names."""

    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    POSTGRES = "postgres"
    DUCKDB = "duckdb"
    ANSI = "ansi"
