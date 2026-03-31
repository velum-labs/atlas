"""Canonical SQL-derived lineage extraction."""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from typing import Literal

import sqlglot
from sqlglot import exp

from alma_sqlkit.inspection import inspect_sql
from alma_sqlkit.parser import ParsingConfig
from alma_sqlkit.parser import SQLParser as _SQLParser
from alma_sqlkit.table_refs import TableRef, extract_tables_from_sql

logger = logging.getLogger(__name__)

try:
    from alma_algebrakit.models.algebra import (
        Aggregation,
        BinarySetOperationMixin,
        ColumnRef,
        Expression,
        Join,
        Limit,
        Projection,
        RAExpression,
        Relation,
        Sort,
        UnaryOperationMixin,
        WithExpression,
    )

    _SQLKIT_AVAILABLE = True
except ImportError:  # pragma: no cover
    _SQLKIT_AVAILABLE = False


@dataclass(frozen=True)
class ColumnEdge:
    source_table: str
    source_column: str
    output_column: str


@dataclass
class LineageResult:
    source_tables: list[TableRef]
    target_table: str | None
    column_edges: list[ColumnEdge]
    cte_names: frozenset[str]
    extraction_method: Literal["column", "table"]


def _extract_target_table(sql: str, dialect: str) -> str | None:
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*contains unsupported syntax.*")
            parsed = sqlglot.parse_one(sql, dialect=dialect)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return None

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


def _extract_select_sql(sql: str, dialect: str) -> str | None:
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*contains unsupported syntax.*")
            parsed = sqlglot.parse_one(sql, dialect=dialect)
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return None

    if isinstance(parsed, (exp.Insert, exp.Create)):
        select_node = parsed.find(exp.Select)
        if select_node is not None:
            return select_node.sql(dialect=dialect)

    return None


def _collect_cte_names(ra: RAExpression) -> frozenset[str]:
    if isinstance(ra, WithExpression):
        return frozenset(cte.name.lower() for cte in ra.ctes)
    return frozenset()


def _collect_relations(
    ra: RAExpression,
    result: dict[str, str],
    *,
    in_cte_def: bool = False,
) -> None:
    if isinstance(ra, Relation):
        if not in_cte_def:
            canonical = f"{ra.schema_name}.{ra.name}" if ra.schema_name else ra.name
            for key in (ra.alias, ra.name):
                if key:
                    result[key.lower()] = canonical
        return

    if isinstance(ra, WithExpression):
        for cte in ra.ctes:
            _collect_relations(cte.query, result, in_cte_def=True)
        _collect_relations(ra.main_query, result, in_cte_def=False)
        return

    if isinstance(ra, Join):
        _collect_relations(ra.left, result, in_cte_def=in_cte_def)
        _collect_relations(ra.right, result, in_cte_def=in_cte_def)
        return

    if isinstance(ra, UnaryOperationMixin):
        _collect_relations(ra.input, result, in_cte_def=in_cte_def)
        return

    if isinstance(ra, BinarySetOperationMixin):
        _collect_relations(ra.left, result, in_cte_def=in_cte_def)
        _collect_relations(ra.right, result, in_cte_def=in_cte_def)


def _find_output_node(ra: RAExpression) -> Projection | Aggregation | None:
    node: RAExpression | None = ra
    while node is not None:
        if isinstance(node, (Projection, Aggregation)):
            return node
        if isinstance(node, (Sort, Limit)):
            node = node.input
        elif isinstance(node, WithExpression):
            node = node.main_query
        else:
            return None
    return None


def _resolve_source(
    table_alias: str | None,
    col: str,
    alias_map: dict[str, str],
    cte_names: frozenset[str],
    single_table: str | None,
) -> str | None:
    if col == "*":
        return None

    if table_alias:
        key = table_alias.lower()
        if key in cte_names:
            return None
        resolved = alias_map.get(key)
        if resolved is not None and resolved.lower() in cte_names:
            return None
        return resolved

    if single_table is not None and single_table.lower() in cte_names:
        return None
    return single_table


def _edges_from_refs(
    refs: set[str],
    output_col: str,
    alias_map: dict[str, str],
    cte_names: frozenset[str],
    single_table: str | None,
) -> list[ColumnEdge]:
    edges: list[ColumnEdge] = []
    for ref in refs:
        if "." in ref:
            table_alias, _, col = ref.partition(".")
        else:
            table_alias, col = "", ref

        source = _resolve_source(table_alias or None, col, alias_map, cte_names, single_table)
        if source:
            edges.append(ColumnEdge(source_table=source, source_column=col, output_column=output_col))
    return edges


def _output_col_name(expr: Expression, alias: str | None) -> str | None:
    if alias:
        return alias
    if isinstance(expr, ColumnRef) and expr.column != "*":
        return expr.column
    for ref in expr.referenced_columns():
        col = ref.split(".")[-1] if "." in ref else ref
        if col != "*":
            return col
    return None


def _column_edges_from_projection(
    proj: Projection,
    alias_map: dict[str, str],
    cte_names: frozenset[str],
) -> list[ColumnEdge]:
    unique_tables = set(alias_map.values())
    single_table = next(iter(unique_tables)) if len(unique_tables) == 1 else None

    edges: list[ColumnEdge] = []
    for expr, alias in proj.columns:
        output_col = _output_col_name(expr, alias)
        if not output_col:
            continue
        refs = expr.referenced_columns()
        edges.extend(_edges_from_refs(refs, output_col, alias_map, cte_names, single_table))
    return edges


def _column_edges_from_aggregation(
    agg: Aggregation,
    alias_map: dict[str, str],
    cte_names: frozenset[str],
) -> list[ColumnEdge]:
    unique_tables = set(alias_map.values())
    single_table = next(iter(unique_tables)) if len(unique_tables) == 1 else None

    edges: list[ColumnEdge] = []
    for expr in agg.group_by:
        output_col = _output_col_name(expr, None)
        if output_col:
            edges.extend(_edges_from_refs(expr.referenced_columns(), output_col, alias_map, cte_names, single_table))

    for agg_spec in agg.aggregates:
        if agg_spec.argument is not None:
            edges.extend(
                _edges_from_refs(
                    agg_spec.argument.referenced_columns(),
                    agg_spec.alias,
                    alias_map,
                    cte_names,
                    single_table,
                )
            )

    return edges


def _extract_column_edges(ra: RAExpression) -> list[ColumnEdge]:
    alias_map: dict[str, str] = {}
    _collect_relations(ra, alias_map)
    cte_names = _collect_cte_names(ra)

    output_node = _find_output_node(ra)
    if output_node is None:
        return []

    if isinstance(output_node, Projection):
        return _column_edges_from_projection(output_node, alias_map, cte_names)
    if isinstance(output_node, Aggregation):
        return _column_edges_from_aggregation(output_node, alias_map, cte_names)
    return []


def extract_lineage(sql: str, *, dialect: str = "postgres") -> LineageResult:
    if not sql.strip():
        return LineageResult(
            source_tables=[],
            target_table=None,
            column_edges=[],
            cte_names=frozenset(),
            extraction_method="table",
        )

    try:
        inspected = inspect_sql(sql, dialect=dialect)
        target_table = inspected.target_table
        select_sql = inspected.query_sql
        source_tables = inspected.source_tables
    except Exception:
        target_table = None
        select_sql = sql
        source_tables = extract_tables_from_sql(sql, dialect=dialect)

    if not _SQLKIT_AVAILABLE:  # pragma: no cover
        return LineageResult(
            source_tables=source_tables,
            target_table=target_table,
            column_edges=[],
            cte_names=frozenset(),
            extraction_method="table",
        )

    try:
        parser = _SQLParser(ParsingConfig(dialect=dialect))
        ra = parser.parse(select_sql)
        column_edges = _extract_column_edges(ra)
        cte_names = _collect_cte_names(ra)
        return LineageResult(
            source_tables=source_tables,
            target_table=target_table,
            column_edges=column_edges,
            cte_names=cte_names,
            extraction_method="column" if column_edges else "table",
        )
    except Exception:
        logger.debug("sqlkit lineage extraction failed, using table-level fallback: %.80s", sql)
        return LineageResult(
            source_tables=source_tables,
            target_table=target_table,
            column_edges=[],
            cte_names=frozenset(),
            extraction_method="table",
        )
