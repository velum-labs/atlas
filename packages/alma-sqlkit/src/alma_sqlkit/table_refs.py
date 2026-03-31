"""Shared SQL table extraction using sqlglot with regex fallback.

This module is the canonical home for table-reference extraction. Higher-level
packages should import from here instead of carrying their own copies.
"""

from __future__ import annotations

import logging
import re
import warnings
from dataclasses import dataclass
from typing import cast

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)

_SKIP_NAMES: frozenset[str] = frozenset(
    {
        "select",
        "where",
        "and",
        "or",
        "on",
        "as",
        "set",
        "values",
        "into",
        "limit",
        "order",
        "group",
        "having",
        "case",
        "when",
        "then",
        "else",
        "end",
        "not",
        "null",
        "true",
        "false",
        "is",
        "in",
        "between",
        "like",
        "exists",
        "all",
        "any",
        "some",
        "lateral",
        "unnest",
        "generate_series",
        "dual",
    }
)

_SKIP_SCHEMAS: frozenset[str] = frozenset(
    {
        "information_schema",
        "pg_catalog",
        "pg_toast",
        "pg_temp",
    }
)

_TRANSACTION_STMTS: frozenset[str] = frozenset({"BEGIN", "COMMIT", "ROLLBACK"})


@dataclass(frozen=True)
class TableRef:
    """A resolved reference to a database table."""

    canonical_name: str
    physical_name: str
    system: str


_POSTGRES_TABLE_RE = re.compile(
    r'(?:FROM|JOIN)\s+(?:(?:"?(?P<schema>[\w-]+)"?)\.)?"?(?P<table>[\w-]+)"?',
    re.IGNORECASE,
)

_BIGQUERY_TABLE_RE = re.compile(
    r"(?:FROM|JOIN)\s+`?(?P<name>[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+){1,2})`?",
    re.IGNORECASE,
)


def _regex_extract_postgres(sql: str) -> list[TableRef]:
    tables: dict[str, TableRef] = {}
    for match in _POSTGRES_TABLE_RE.finditer(sql):
        schema_name = (match.group("schema") or "public").lower().strip()
        table_name = match.group("table").lower().strip()
        if (
            not table_name
            or table_name in _SKIP_NAMES
            or table_name.startswith("_")
            or schema_name in _SKIP_NAMES
            or schema_name in _SKIP_SCHEMAS
        ):
            continue
        canonical = f"{schema_name}.{table_name}"
        tables[canonical] = TableRef(
            canonical_name=canonical,
            physical_name=canonical,
            system="postgres",
        )
    return [tables[key] for key in sorted(tables)]


def _regex_extract_bigquery(sql: str) -> list[TableRef]:
    tables: dict[str, TableRef] = {}
    for match in _BIGQUERY_TABLE_RE.finditer(sql):
        raw_name = match.group("name").strip()
        parts = [part.strip() for part in raw_name.split(".") if part.strip()]
        if len(parts) < 2:
            continue
        if any(part.lower() in _SKIP_NAMES for part in parts):
            continue
        if len(parts) >= 3:
            project_id, dataset_id, table_id = parts[-3], parts[-2], parts[-1]
            physical = f"{project_id}.{dataset_id}.{table_id}"
        else:
            dataset_id, table_id = parts[-2], parts[-1]
            physical = f"{dataset_id}.{table_id}"
        canonical = f"{dataset_id}.{table_id}"
        tables[canonical.lower()] = TableRef(
            canonical_name=canonical,
            physical_name=physical,
            system="bigquery",
        )
    return [tables[key] for key in sorted(tables)]


def _collect_cte_aliases(parsed: exp.Expression) -> frozenset[str]:
    aliases: set[str] = set()
    for cte in parsed.find_all(exp.CTE):
        alias = cte.alias
        if alias:
            aliases.add(alias.lower())
    return frozenset(aliases)


def _ast_extract_postgres(
    parsed: exp.Expression,
    *,
    default_schema: str = "public",
) -> list[TableRef]:
    cte_aliases = _collect_cte_aliases(parsed)
    tables: dict[str, TableRef] = {}
    for table_expr in parsed.find_all(exp.Table):
        name = table_expr.name
        if not name:
            continue
        name_lower = name.lower()
        if name_lower in _SKIP_NAMES or name_lower in cte_aliases or name_lower.startswith("_"):
            continue
        schema = (table_expr.db or "").lower() or default_schema
        if schema in _SKIP_SCHEMAS:
            continue
        canonical = f"{schema}.{name_lower}"
        tables[canonical] = TableRef(
            canonical_name=canonical,
            physical_name=canonical,
            system="postgres",
        )
    return [tables[key] for key in sorted(tables)]


def _ast_extract_bigquery(parsed: exp.Expression) -> list[TableRef]:
    cte_aliases = _collect_cte_aliases(parsed)
    tables: dict[str, TableRef] = {}
    for table_expr in parsed.find_all(exp.Table):
        name = table_expr.name
        if not name:
            continue
        name_lower = name.lower()
        if name_lower in _SKIP_NAMES or name_lower in cte_aliases:
            continue
        catalog = (table_expr.catalog or "").strip()
        db = (table_expr.db or "").strip()
        if catalog and db:
            physical = f"{catalog}.{db}.{name}"
            canonical = f"{db}.{name}"
        elif db:
            physical = f"{db}.{name}"
            canonical = f"{db}.{name}"
        else:
            continue
        tables[canonical.lower()] = TableRef(
            canonical_name=canonical,
            physical_name=physical,
            system="bigquery",
        )
    return [tables[key] for key in sorted(tables)]


def extract_tables_from_sql(
    sql: str,
    *,
    dialect: str = "postgres",
    default_schema: str = "public",
) -> list[TableRef]:
    """Extract table references from a SQL string."""
    if not sql or not sql.strip() or sql.strip().upper() in _TRANSACTION_STMTS:
        return []
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*contains unsupported syntax.*")
            parsed = cast(exp.Expression, sqlglot.parse_one(sql, dialect=dialect))
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        logger.debug("sqlglot parse failed, falling back to regex: %.80s", sql)
        if dialect == "bigquery":
            return _regex_extract_bigquery(sql)
        return _regex_extract_postgres(sql)
    if dialect == "bigquery":
        return _ast_extract_bigquery(parsed)
    return _ast_extract_postgres(parsed, default_schema=default_schema)


def extract_table_names(
    sql: str,
    *,
    dialect: str = "postgres",
    default_schema: str = "public",
) -> list[str]:
    """Return canonical table names for a SQL string."""
    return [
        ref.canonical_name
        for ref in extract_tables_from_sql(
            sql,
            dialect=dialect,
            default_schema=default_schema,
        )
    ]
