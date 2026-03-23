"""Column reference utilities for relational algebra rewriting.

This module provides generic functions for collecting and rewriting column
references during query rewriting operations.
"""

from __future__ import annotations

from alma_algebrakit.models.algebra import (
    Aggregation,
    Join,
    Projection,
    RAExpression,
    RelationRef,
    Selection,
)
from alma_algebrakit.naming import parse_parts


def collect_column_references(expr: RAExpression) -> set[str]:
    """Collect all qualified column references from an RA expression.

    This traverses the entire expression tree and collects column references
    from predicates, projections, aggregations, and join conditions.

    Args:
        expr: RA expression to collect column references from

    Returns:
        Set of qualified column references (e.g., {"t.col1", "u.col2"})
    """
    refs: set[str] = set()
    _collect_column_refs_recursive(expr, refs)
    return refs


def _collect_column_refs_recursive(expr: RAExpression, refs: set[str]) -> None:
    """Recursively collect column references from all parts of RA expression."""
    if isinstance(expr, Selection):
        # Get columns from predicate
        refs.update(expr.predicate.referenced_columns())
        _collect_column_refs_recursive(expr.input, refs)
    elif isinstance(expr, Projection):
        # Get columns from projection list
        for col_expr, _ in expr.columns:
            refs.update(col_expr.referenced_columns())
        _collect_column_refs_recursive(expr.input, refs)
    elif isinstance(expr, Join):
        if expr.condition:
            refs.update(expr.condition.referenced_columns())
        _collect_column_refs_recursive(expr.left, refs)
        _collect_column_refs_recursive(expr.right, refs)
    elif isinstance(expr, Aggregation):
        for agg in expr.aggregates:
            if agg.argument:
                refs.update(agg.argument.referenced_columns())
        for group_col in expr.group_by:
            refs.update(group_col.referenced_columns())
        if expr.having:
            refs.update(expr.having.referenced_columns())
        _collect_column_refs_recursive(expr.input, refs)


def build_column_rewrite_map(
    query_ra: RAExpression,
    covered_aliases: set[str],
    lineage: dict[tuple[str, str], str],
    is_select_star: bool = False,
    view_alias: str = "cv",
) -> dict[str, str]:
    """Build mapping from original qualified columns to rewritten columns.

    For SELECT * views, columns map to themselves (passthrough).
    For explicit column views, columns map via the lineage dictionary.

    Args:
        query_ra: Query RA expression
        covered_aliases: Set of table aliases that are covered by the view
        lineage: Maps (table_or_alias, column) -> view_column
        is_select_star: Whether view is SELECT * (passthrough)
        view_alias: Alias to use for the view (default: "cv")

    Returns:
        Mapping from original qualified column to rewritten qualified column
        (e.g., {"t.col" -> "cv.mapped_col"})
    """
    column_rewrite_map: dict[str, str] = {}

    if is_select_star:
        # For SELECT *, every column from covered tables maps to view_alias.column (same name)
        for col_ref in collect_column_references(query_ra):
            if "." in col_ref:
                # Parse potentially schema-qualified name (schema.table.column or table.column)
                _, schema, table, col = parse_parts(col_ref)
                # The alias to check is the table part (or schema.table if schema present)
                alias = (
                    f"{schema}.{table}"
                    if schema and table
                    else (table or col_ref.rsplit(".", 1)[0])
                )
                if alias in covered_aliases:
                    column_rewrite_map[col_ref] = f"{view_alias}.{col}"
        return column_rewrite_map

    # For non-SELECT * views, use lineage
    for (table_or_alias, col), view_col in lineage.items():
        # Check if this matches a covered alias
        if table_or_alias in covered_aliases:
            original = f"{table_or_alias}.{col}"
            rewritten = f"{view_alias}.{view_col}"
            column_rewrite_map[original] = rewritten

    return column_rewrite_map


def check_column_availability(
    required_columns: set[str],
    covered_aliases: set[str],
    covered_rels: set[RelationRef],
    lineage: dict[tuple[str, str], str],
    is_select_star: bool = False,
) -> tuple[bool, set[str]]:
    """Check that all required columns are available in the view.

    For SELECT * views, all columns from covered tables are assumed available.
    For explicit column views, columns must be present in the lineage.

    Args:
        required_columns: Set of qualified column references needed
        covered_aliases: Set of table aliases that are covered
        covered_rels: Set of RelationRef for covered relations (for physical table lookup)
        lineage: Maps (table_or_alias, column) -> view_column
        is_select_star: Whether view is SELECT * (passthrough)

    Returns:
        (all_available, missing_columns) tuple
    """
    if is_select_star:
        # All columns from covered tables are available
        return True, set()

    missing: set[str] = set()

    # Build alias to physical table mapping
    alias_to_physical: dict[str, str] = {}
    for rel in covered_rels:
        alias_to_physical[rel.effective_name()] = rel.physical_table

    for col_ref in required_columns:
        if "." not in col_ref:
            continue

        # Parse potentially schema-qualified name (schema.table.column or table.column)
        _, schema, table, col = parse_parts(col_ref)
        # The alias to check is the table part (or schema.table if schema present)
        alias = f"{schema}.{table}" if schema and table else (table or col_ref.rsplit(".", 1)[0])

        if alias not in covered_aliases:
            continue

        # Find physical table for this alias
        physical_table = alias_to_physical.get(alias)

        # Check if column is in lineage
        if (alias, col) not in lineage:
            if physical_table is None or (physical_table, col) not in lineage:
                missing.add(col_ref)

    return len(missing) == 0, missing
