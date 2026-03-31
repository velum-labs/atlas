"""alma-sqlkit - SQL adapters for alma-algebrakit.

This package provides SQL-specific adapters that work with alma_algebrakit's
pure relational algebra types:

- SQLParser: Parse SQL strings into alma_algebrakit.RAExpression
- SQLBinder: Bind SQL to alma_algebrakit.BoundQuery (uses alma_algebrakit.Scope)
- SQLEmitter: Emit SQL from alma_algebrakit.RAExpression
- SQLBuilder: Build SQL queries with a type-safe fluent API

Import package-owned symbols directly from `alma_sqlkit`. Import algebra or
sqlglot symbols from their owning packages.
"""

from alma_sqlkit.binder import BindingError, SQLBinder
from alma_sqlkit.builder import SQLBuilder, build_sql
from alma_sqlkit.dialect import (
    BIGQUERY,
    DEFAULT_DIALECT,
    DUCKDB,
    POSTGRES,
    SNOWFLAKE,
    Dialect,
    DialectName,
)
from alma_sqlkit.emitter import SQLEmitter, emit_sql
from alma_sqlkit.normalize import normalize_sql
from alma_sqlkit.parse import extract_tables, parse_sql
from alma_sqlkit.parser import ParsingConfig, SQLParser
from alma_sqlkit.table_refs import TableRef, extract_table_names, extract_tables_from_sql

__version__ = "0.2.0"


__all__ = [
    # Version
    "__version__",
    # Parser
    "SQLParser",
    "ParsingConfig",
    # Binder
    "SQLBinder",
    "BindingError",
    # Emitter
    "SQLEmitter",
    "emit_sql",
    # Builder
    "SQLBuilder",
    "build_sql",
    # Dialect
    "Dialect",
    "DialectName",
    "DEFAULT_DIALECT",
    "POSTGRES",
    "DUCKDB",
    "SNOWFLAKE",
    "BIGQUERY",
    # Parsing helpers
    "parse_sql",
    "extract_tables",
    "normalize_sql",
    "TableRef",
    "extract_table_names",
    "extract_tables_from_sql",
]
