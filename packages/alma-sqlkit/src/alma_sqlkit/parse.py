"""SQL parsing utilities for alma-sqlkit.

Wraps sqlglot to parse SQL strings into expression trees and extract
structural information such as referenced table names.
"""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from alma_sqlkit.dialect import Dialect


def parse_sql(sql: str, dialect: Dialect | str | None = None) -> list[exp.Expression]:
    """Parse a SQL string into a list of sqlglot expression trees.

    Args:
        sql: Raw SQL string (may contain multiple statements).
        dialect: SQL dialect for parsing. Defaults to None (sqlglot generic/ANSI mode).

    Returns:
        List of parsed expression trees, one per statement.
    """
    dialect_str = dialect.name if isinstance(dialect, Dialect) else dialect
    return sqlglot.parse(sql, dialect=dialect_str, error_level=sqlglot.ErrorLevel.WARN)


def extract_tables(sql: str, dialect: Dialect | str | None = None) -> list[str]:
    """Extract all referenced table/view names from a SQL string.

    Returns fully-qualified names where available (e.g. ``project.dataset.table``).

    Args:
        sql: Raw SQL string.
        dialect: SQL dialect for parsing. Defaults to None (sqlglot generic/ANSI mode).

    Returns:
        Deduplicated list of table name strings in the order they appear.
    """
    seen: set[str] = set()
    result: list[str] = []

    for statement in parse_sql(sql, dialect=dialect):
        if statement is None:
            continue
        for table in statement.find_all(exp.Table):
            name = table.name
            if table.db:
                name = f"{table.db}.{name}"
            if table.catalog:
                name = f"{table.catalog}.{name}"
            if name and name not in seen:
                seen.add(name)
                result.append(name)

    return result
