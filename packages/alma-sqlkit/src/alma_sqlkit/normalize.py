"""SQL normalization utilities for alma-sqlkit.

Normalizes SQL strings into a canonical form suitable for fingerprinting
and deduplication. Normalization strips comments, standardizes whitespace,
uppercases keywords, and replaces literal values with placeholders.
"""

from __future__ import annotations

import sqlglot

from alma_sqlkit.dialect import Dialect


def normalize_sql(sql: str, dialect: Dialect | str = Dialect.ANSI) -> str:
    """Normalize a SQL string to a canonical, comparable form.

    Transformations applied:
    - Parse and re-generate via sqlglot (normalizes casing and whitespace)
    - Replace string and numeric literals with ``?`` placeholders
    - Strip SQL comments

    Args:
        sql: Raw SQL string.
        dialect: SQL dialect for parsing. Defaults to ANSI.

    Returns:
        Normalized SQL string. Returns the original string on parse error.
    """
    try:
        statements = sqlglot.parse(sql, dialect=str(dialect), error_level=sqlglot.ErrorLevel.WARN)
        normalized_parts: list[str] = []
        for stmt in statements:
            if stmt is None:
                continue
            # Anonymize literals
            for node in stmt.walk():
                if isinstance(node, sqlglot.exp.Literal):
                    node.args["this"] = "?"
                    node.args["is_string"] = False
            normalized_parts.append(stmt.sql(dialect=str(dialect), pretty=False))
        return "; ".join(normalized_parts)
    except Exception:
        return sql
