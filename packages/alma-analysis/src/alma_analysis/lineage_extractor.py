"""Column-level and table-level lineage extraction from SQL queries.

Uses alma_sqlkit.SQLParser (algebrakit RA types) for column-level extraction when
the SQL is parseable as a SELECT statement.  Falls back to
``extract_tables_from_sql()`` from alma_analysis.extract_tables for unparseable SQL or
statement types that SQLParser does not support (DDL, DML wrappers, etc.).

Supported patterns
------------------
- Plain SELECT with qualified columns: ``SELECT t.col FROM table AS t``
- GROUP BY / aggregation: upstream columns tracked per aggregate alias
- CTEs: CTE-originated columns skipped (they are intermediate nodes)
- INSERT INTO ... SELECT: target table extracted; SELECT part analysed
- CREATE TABLE ... AS SELECT: same as INSERT for target extraction
- Fallback: any SQL that fails algebrakit parsing gets table-level lineage

Not in scope for this version
------------------------------
- SELECT *  (star expansions require a catalog; skipped silently)
- Unqualified columns in multi-table queries (ambiguous without catalog)
- Recursive CTEs
- Cross-database column tracking (table refs still work via extract_tables_from_sql)
"""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from typing import Literal

import sqlglot
from sqlglot import exp

from alma_analysis.extract_tables import TableRef, extract_tables_from_sql

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional algebrakit / alma_sqlkit import
# ---------------------------------------------------------------------------
# alma_sqlkit is listed as a dependency but guard against import errors so the
# module degrades gracefully in environments where the package is absent.

try:
    from alma_algebrakit.models.algebra import (  # type: ignore[import-untyped]
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
    from alma_sqlkit.parser import SQLParser as _SQLParser  # type: ignore[import-untyped]

    _SQLKIT_AVAILABLE = True
except ImportError:  # pragma: no cover
    _SQLKIT_AVAILABLE = False


# ---------------------------------------------------------------------------
# Public data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnEdge:
    """A directed data-flow edge from a source column to a query output column."""

    source_table: str
    """Canonical table name (e.g. ``public.users``)."""

    source_column: str
    """Column name in the source table."""

    output_column: str
    """Name in the query output (alias, or the original column name)."""


@dataclass
class LineageResult:
    """All lineage information extracted from a single SQL query."""

    source_tables: list[TableRef]
    """Base tables read by the query."""

    target_table: str | None
    """Write target for INSERT INTO / CREATE TABLE AS SELECT; ``None`` otherwise."""

    column_edges: list[ColumnEdge]
    """Column-level data-flow edges.  Empty when extraction falls back to table-level."""

    cte_names: frozenset[str]
    """Lower-cased names of CTEs defined in the query."""

    extraction_method: Literal["column", "table"]
    """``"column"`` when algebrakit succeeded; ``"table"`` for fallback."""


# ---------------------------------------------------------------------------
# INSERT / CREATE target extraction  (sqlglot AST — no algebrakit needed)
# ---------------------------------------------------------------------------


def _extract_target_table(sql: str, dialect: str) -> str | None:
    """Return the canonical name of the write target, or ``None`` for plain SELECT."""
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
                table = (
                    schema_or_table.find(exp.Table)
                    if hasattr(schema_or_table, "find")
                    else schema_or_table
                )
                if isinstance(table, exp.Table) and table.name:
                    db = table.args.get("db")
                    parts = [str(db), table.name] if db else [table.name]
                    return ".".join(p for p in parts if p)

    return None


def _extract_select_sql(sql: str, dialect: str) -> str | None:
    """For INSERT / CREATE AS SELECT, return just the SELECT part as SQL text."""
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


# ---------------------------------------------------------------------------
# RA tree helpers  (only reached when _SQLKIT_AVAILABLE is True)
# ---------------------------------------------------------------------------


def _collect_cte_names(ra: RAExpression) -> frozenset[str]:
    """Return lower-cased names of all CTEs defined at the top level."""
    if isinstance(ra, WithExpression):
        return frozenset(cte.name.lower() for cte in ra.ctes)
    return frozenset()


def _collect_relations(
    ra: RAExpression,
    result: dict[str, str],
    *,
    in_cte_def: bool = False,
) -> None:
    """Populate *result* with alias → canonical_name mappings.

    CTE definition sub-trees are traversed but their ``Relation`` nodes are
    *not* added to the map — CTE-local aliases must not leak into the outer
    query scope.
    """
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

    # UnaryOperationMixin covers Projection, Aggregation, Selection, Sort, Limit
    if isinstance(ra, UnaryOperationMixin):
        _collect_relations(ra.input, result, in_cte_def=in_cte_def)
        return

    # BinarySetOperationMixin covers Union, Difference, Intersect
    if isinstance(ra, BinarySetOperationMixin):
        _collect_relations(ra.left, result, in_cte_def=in_cte_def)
        _collect_relations(ra.right, result, in_cte_def=in_cte_def)


def _find_output_node(ra: RAExpression) -> Projection | Aggregation | None:
    """Find the outermost Projection or Aggregation, unwrapping Sort/Limit/With."""
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
    """Resolve a column reference to its canonical source table, or ``None``.

    Returns ``None`` when the reference cannot be resolved to a real base table
    (star column, CTE alias, or ambiguous unqualified column).
    """
    if col == "*":
        return None  # star expansions require a catalog; skip

    if table_alias:
        key = table_alias.lower()
        if key in cte_names:
            return None  # alias is the CTE name itself
        resolved = alias_map.get(key)
        # The alias may resolve to a CTE name (e.g. FROM active AS a → resolved="active")
        if resolved is not None and resolved.lower() in cte_names:
            return None
        return resolved

    # Unqualified column: only resolvable when exactly one table is in scope
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
    """Build ColumnEdges from a set of ``referenced_columns()`` strings."""
    edges: list[ColumnEdge] = []
    for ref in refs:
        if "." in ref:
            table_alias, _, col = ref.partition(".")
        else:
            table_alias, col = "", ref

        source = _resolve_source(table_alias or None, col, alias_map, cte_names, single_table)
        if source:
            edges.append(
                ColumnEdge(source_table=source, source_column=col, output_column=output_col)
            )
    return edges


def _output_col_name(expr: Expression, alias: str | None) -> str | None:
    """Derive the output column name for a SELECT item."""
    if alias:
        return alias
    if isinstance(expr, ColumnRef) and expr.column != "*":
        return expr.column
    # For complex expressions without an alias, pick the first non-star column ref
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
    """Extract ColumnEdges from a Projection (SELECT list) RA node."""
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
    """Extract ColumnEdges from an Aggregation (GROUP BY) RA node."""
    unique_tables = set(alias_map.values())
    single_table = next(iter(unique_tables)) if len(unique_tables) == 1 else None

    edges: list[ColumnEdge] = []

    # GROUP BY pass-through columns
    for expr in agg.group_by:
        output_col = _output_col_name(expr, None)
        if output_col:
            refs = expr.referenced_columns()
            edges.extend(_edges_from_refs(refs, output_col, alias_map, cte_names, single_table))

    # Aggregate function arguments (SUM(col) → col feeds into alias)
    for agg_spec in agg.aggregates:
        if agg_spec.argument is not None:
            refs = agg_spec.argument.referenced_columns()
            edges.extend(_edges_from_refs(refs, agg_spec.alias, alias_map, cte_names, single_table))

    return edges


def _extract_column_edges(ra: RAExpression) -> list[ColumnEdge]:
    """Extract column-level lineage edges from a parsed RA tree."""
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_lineage(sql: str, *, dialect: str = "postgres") -> LineageResult:
    """Extract lineage from a SQL string.

    Attempts column-level extraction via alma_sqlkit / algebrakit when the SQL
    parses as a SELECT.  Falls back to table-level extraction via
    ``extract_tables_from_sql()`` on any parse failure, unsupported statement
    type, or missing alma_sqlkit installation.

    For INSERT / CREATE TABLE AS SELECT statements the write target is always
    extracted (via sqlglot) regardless of whether column-level succeeds.
    """
    target_table = _extract_target_table(sql, dialect)

    # For DML wrappers, analyse the inner SELECT for source tables / column edges
    select_sql = sql
    if target_table is not None:
        inner = _extract_select_sql(sql, dialect)
        if inner:
            select_sql = inner

    source_tables = extract_tables_from_sql(select_sql, dialect=dialect)

    if not _SQLKIT_AVAILABLE:  # pragma: no cover
        return LineageResult(
            source_tables=source_tables,
            target_table=target_table,
            column_edges=[],
            cte_names=frozenset(),
            extraction_method="table",
        )

    try:
        parser = _SQLParser()
        ra = parser.parse(select_sql)
        column_edges = _extract_column_edges(ra)
        cte_names = _collect_cte_names(ra)
        extraction_method: Literal["column", "table"] = "column" if column_edges else "table"
        return LineageResult(
            source_tables=source_tables,
            target_table=target_table,
            column_edges=column_edges,
            cte_names=cte_names,
            extraction_method=extraction_method,
        )
    except Exception:
        logger.debug("algebrakit column extraction failed, using table-level fallback: %.80s", sql)
        return LineageResult(
            source_tables=source_tables,
            target_table=target_table,
            column_edges=[],
            cte_names=frozenset(),
            extraction_method="table",
        )
