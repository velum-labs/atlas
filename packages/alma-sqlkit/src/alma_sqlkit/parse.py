"""SQL parsing utilities for alma-sqlkit.

Wraps sqlglot to parse SQL strings into expression trees and extract
structural information such as referenced table names.
"""

from __future__ import annotations

import warnings

import sqlglot
import sqlglot.expressions as exp

from alma_sqlkit.dialect import Dialect
from alma_sqlkit.table_refs import extract_table_names


def parse_sql(sql: str, dialect: Dialect | str | None = None) -> list[exp.Expr]:
    """Parse a SQL string into a list of sqlglot expression trees.

    Args:
        sql: Raw SQL string (may contain multiple statements).
        dialect: SQL dialect for parsing. Defaults to None (sqlglot generic/ANSI mode).

    Returns:
        List of parsed expression trees, one per statement.
    """
    dialect_str = dialect.name if isinstance(dialect, Dialect) else dialect
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*contains unsupported syntax.*")
        parsed = sqlglot.parse(sql, dialect=dialect_str, error_level=sqlglot.ErrorLevel.WARN)
    return [statement for statement in parsed if statement is not None]


def extract_tables(sql: str, dialect: Dialect | str | None = None) -> list[str]:
    """Extract all referenced table/view names from a SQL string.

    Returns fully-qualified names where available (e.g. ``project.dataset.table``).

    Args:
        sql: Raw SQL string.
        dialect: SQL dialect for parsing. Defaults to None (sqlglot generic/ANSI mode).

    Returns:
        Deduplicated list of table name strings in the order they appear.
    """
    dialect_name = dialect.name if isinstance(dialect, Dialect) else (dialect or "postgres")
    return extract_table_names(sql, dialect=dialect_name)
