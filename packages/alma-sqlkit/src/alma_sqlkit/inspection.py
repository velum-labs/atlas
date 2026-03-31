"""Single front door for SQL statement inspection."""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp

from alma_sqlkit.normalize import normalize_sql
from alma_sqlkit.table_refs import TableRef, extract_tables_from_sql


@dataclass(frozen=True)
class SqlInspection:
    statement_kind: str
    sql: str
    query_sql: str
    normalized_sql: str
    target_table: str | None
    source_tables: list[TableRef]


def _extract_target_table(parsed: Any) -> str | None:
    if isinstance(parsed, exp.Insert):
        table = parsed.find(exp.Table)
        if table and table.name:
            db = table.args.get("db")
            parts = [str(db), table.name] if db else [table.name]
            return ".".join(p for p in parts if p)

    if isinstance(parsed, exp.Create):
        kind = (parsed.args.get("kind") or "").upper()
        if kind == "TABLE":
            schema_or_table = parsed.args.get("this")
            if schema_or_table is not None:
                table = schema_or_table.find(exp.Table) if hasattr(schema_or_table, "find") else schema_or_table
                if isinstance(table, exp.Table) and table.name:
                    db = table.args.get("db")
                    parts = [str(db), table.name] if db else [table.name]
                    return ".".join(p for p in parts if p)
    return None


def _extract_query_sql(parsed: Any, *, dialect: str, raw_sql: str) -> str:
    if isinstance(parsed, (exp.Insert, exp.Create)):
        select_node = parsed.find(exp.Select)
        if select_node is not None:
            return select_node.sql(dialect=dialect)
    return raw_sql


def inspect_sql(sql: str, *, dialect: str = "postgres") -> SqlInspection:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*contains unsupported syntax.*")
        parsed = sqlglot.parse_one(sql, dialect=dialect)

    target_table = _extract_target_table(parsed)
    query_sql = _extract_query_sql(parsed, dialect=dialect, raw_sql=sql)
    return SqlInspection(
        statement_kind=type(parsed).__name__.lower(),
        sql=sql,
        query_sql=query_sql,
        normalized_sql=normalize_sql(query_sql),
        target_table=target_table,
        source_tables=extract_tables_from_sql(query_sql, dialect=dialect),
    )
