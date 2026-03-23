"""alma-sqlkit - SQL adapters for alma-algebrakit.

This package provides SQL-specific adapters that work with alma_algebrakit's
pure relational algebra types:

- SQLParser: Parse SQL strings into alma_algebrakit.RAExpression
- SQLBinder: Bind SQL to alma_algebrakit.BoundQuery (uses alma_algebrakit.Scope)
- SQLEmitter: Emit SQL from alma_algebrakit.RAExpression
- SQLBuilder: Build SQL queries with a type-safe fluent API

Example:
    >>> from alma_sqlkit import SQLParser, SQLEmitter, SQLBinder
    >>> from alma_algebrakit import Catalog
    >>>
    >>> # Parse SQL to RA
    >>> parser = SQLParser()
    >>> ra_expr = parser.parse("SELECT * FROM users WHERE id = 1")
    >>>
    >>> # Emit SQL from RA
    >>> emitter = SQLEmitter(dialect="postgres")
    >>> sql = emitter.emit(ra_expr)
    >>>
    >>> # Bind SQL (resolve column references)
    >>> catalog = Catalog.from_dict({"users": [{"name": "id", "type": "integer"}]})
    >>> binder = SQLBinder(catalog)
    >>> bound = binder.bind("SELECT id FROM users")
"""

# Re-export sqlglot internals for direct AST access
# This allows consumers to use alma_sqlkit as the single entry point for all SQL operations
import sqlglot

# Re-export from alma_algebrakit for convenience
from alma_algebrakit import (
    AttributeRef,
    BoundExpression,
    BoundPredicate,
    # Bound
    BoundQuery,
    # Schema
    Catalog,
    RelationInstance,
    # Scope
    Scope,
    TableSchema,
)
from sqlglot import exp
from sqlglot.errors import ParseError

# Binding (SQL → BoundQuery) - thin adapter using sqlglot
from alma_sqlkit.binder import BindingError, SQLBinder

# Building (fluent SQL)
from alma_sqlkit.builder import SQLBuilder, build_sql

# Dialect config
from alma_sqlkit.dialect import (
    BIGQUERY,
    DEFAULT_DIALECT,
    DUCKDB,
    POSTGRES,
    SNOWFLAKE,
    Dialect,
    DialectName,
)

# Emission (RA → SQL)
from alma_sqlkit.emitter import SQLEmitter, emit_sql

# SQL-specific extensions to alma_algebrakit types
from alma_sqlkit.extensions import (
    # CTEs
    CTEDefinition,
    ExtendedJoin,
    # PostgreSQL extensions
    ExtendedProjection,
    WindowFrameBound,
    WindowFrameSpec,
    # Window functions
    WindowFrameType,
    WindowFunction,
    WindowSpec,
    WithClause,
)

# Parsing (SQL → RA) - thin adapter using sqlglot
from alma_sqlkit.parser import ParsingConfig, SQLParser

__version__ = "0.2.0"


# AST utilities
def parse_ast(sql: str, dialect: str = "postgres") -> exp.Expression:
    """Parse SQL string and return raw sqlglot AST.

    This is a convenience function for direct AST access when you need
    to work with sqlglot's expression types directly.

    Args:
        sql: SQL string to parse
        dialect: SQL dialect (default: "postgres")

    Returns:
        sqlglot.exp.Expression: The parsed AST
    """
    return sqlglot.parse_one(sql, dialect=dialect)


__all__ = [
    # Version
    "__version__",
    # sqlglot re-exports for direct AST access
    "sqlglot",
    "exp",
    "ParseError",
    "parse_ast",
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
    # Re-exported from alma_algebrakit
    "Catalog",
    "TableSchema",
    "Scope",
    "RelationInstance",
    "BoundQuery",
    "AttributeRef",
    "BoundExpression",
    "BoundPredicate",
    # SQL extensions
    "WindowFrameType",
    "WindowFrameBound",
    "WindowFrameSpec",
    "WindowSpec",
    "WindowFunction",
    "CTEDefinition",
    "WithClause",
    "ExtendedProjection",
    "ExtendedJoin",
]
