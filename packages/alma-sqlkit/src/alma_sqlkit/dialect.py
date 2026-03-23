"""SQL dialect configuration.

Provides dialect-specific settings for SQL generation, including
identifier quoting, string literals, and formatting options.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class DialectName(StrEnum):
    """Supported SQL dialects."""

    POSTGRES = "postgres"
    DUCKDB = "duckdb"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    MYSQL = "mysql"
    SQLITE = "sqlite"


@dataclass(frozen=True)
class Dialect:
    """SQL dialect configuration for emission.

    Attributes:
        name: The dialect name (used by sqlglot)
        identifier_quote: Character used to quote identifiers (e.g., '"' for postgres)
        string_quote: Character used for string literals (typically "'")
        pretty: Whether to format SQL with newlines and indentation
        normalize_identifiers: Whether to lowercase/uppercase identifiers
        pad: Number of spaces for indentation when pretty=True
    """

    name: str
    identifier_quote: str = '"'
    string_quote: str = "'"
    pretty: bool = True
    normalize_identifiers: bool = False
    pad: int = 2

    @classmethod
    def postgres(cls, pretty: bool = True) -> Dialect:
        """Create PostgreSQL dialect configuration."""
        return cls(
            name="postgres",
            identifier_quote='"',
            string_quote="'",
            pretty=pretty,
        )

    @classmethod
    def duckdb(cls, pretty: bool = True) -> Dialect:
        """Create DuckDB dialect configuration."""
        return cls(
            name="duckdb",
            identifier_quote='"',
            string_quote="'",
            pretty=pretty,
        )

    @classmethod
    def snowflake(cls, pretty: bool = True) -> Dialect:
        """Create Snowflake dialect configuration."""
        return cls(
            name="snowflake",
            identifier_quote='"',
            string_quote="'",
            pretty=pretty,
            normalize_identifiers=True,  # Snowflake uppercases by default
        )

    @classmethod
    def bigquery(cls, pretty: bool = True) -> Dialect:
        """Create BigQuery dialect configuration."""
        return cls(
            name="bigquery",
            identifier_quote="`",
            string_quote="'",
            pretty=pretty,
        )

    @classmethod
    def from_name(cls, name: str, pretty: bool = True) -> Dialect:
        """Create dialect from name string.

        Args:
            name: Dialect name (postgres, duckdb, snowflake, etc.)
            pretty: Whether to format SQL output

        Returns:
            Dialect configuration

        Raises:
            ValueError: If dialect name is not recognized
        """
        factories = {
            "postgres": cls.postgres,
            "postgresql": cls.postgres,
            "duckdb": cls.duckdb,
            "snowflake": cls.snowflake,
            "bigquery": cls.bigquery,
        }

        factory = factories.get(name.lower())
        if factory:
            return factory(pretty=pretty)

        # Default dialect for unknown names
        return cls(name=name, pretty=pretty)


# Common dialect instances
POSTGRES = Dialect.postgres()
DUCKDB = Dialect.duckdb()
SNOWFLAKE = Dialect.snowflake()
BIGQUERY = Dialect.bigquery()

# Default dialect
DEFAULT_DIALECT = POSTGRES
