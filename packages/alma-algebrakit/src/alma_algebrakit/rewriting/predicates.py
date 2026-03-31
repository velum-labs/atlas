"""Predicate classification and extraction utilities for relational algebra rewriting.

This module provides generic functions for working with predicates during
query rewriting operations. These are the core primitives used by higher-level
rewriting systems.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from alma_algebrakit.models.algebra import (
    Aggregation,
    Join,
    Predicate,
    Projection,
    RAExpression,
    RelationRef,
    Selection,
)


@dataclass
class PredicateClassification:
    """Classification of query predicates for rewriting.

    Predicates are classified as:
    - keep: Must remain in WHERE (either not proven redundant, or reference remaining tables)
    - drop: Proven redundant by view predicates

    Note: We do NOT "absorb" predicates just because they reference only covered tables.
    """

    keep: list[Predicate] = field(default_factory=list)
    drop: list[Predicate] = field(default_factory=list)
    required_covered_cols: set[str] = field(default_factory=set)  # Qualified column refs


_IDENTIFIER_BOUNDARY = r"[A-Za-z0-9_]"


def _replace_qualified_identifier(text: str, original: str, replacement: str) -> str:
    pattern = rf"(?<!{_IDENTIFIER_BOUNDARY}){re.escape(original)}(?!{_IDENTIFIER_BOUNDARY})"
    return re.sub(pattern, replacement, text)


def extract_selection_predicates(expr: RAExpression) -> list[Predicate]:
    """Extract only selection (WHERE/HAVING) predicates, not join conditions.

    This is critical for partial coverage rewriting: join predicates from
    covered-to-covered joins are ABSORBED into the contract view and should
    NOT require their columns to be exposed. Only selection predicates
    (WHERE/HAVING) need their columns exposed.

    Join conditions are handled separately - covered-to-covered joins
    are absorbed, and boundary join columns are tracked via the join graph.

    Args:
        expr: RA expression to extract predicates from

    Returns:
        List of Selection and HAVING predicates only (no JOIN ON conditions)
    """
    predicates: list[Predicate] = []
    _extract_selection_recursive(expr, predicates)
    return predicates


def _extract_selection_recursive(
    expr: RAExpression,
    predicates: list[Predicate],
) -> None:
    """Recursively extract only selection predicates."""
    if isinstance(expr, Selection):
        predicates.append(expr.predicate)
        _extract_selection_recursive(expr.input, predicates)
    elif isinstance(expr, Projection):
        _extract_selection_recursive(expr.input, predicates)
    elif isinstance(expr, Join):
        # Skip join.condition - it's handled separately
        # Only boundary join columns need to be exposed
        # Covered-to-covered join columns are absorbed
        _extract_selection_recursive(expr.left, predicates)
        _extract_selection_recursive(expr.right, predicates)
    elif isinstance(expr, Aggregation):
        if expr.having:
            predicates.append(expr.having)
        _extract_selection_recursive(expr.input, predicates)


def classify_predicates(
    query_predicates: list[Predicate],
    view_predicates: list[Predicate] | None,
    covered_aliases: set[str],
    remaining_aliases: set[str],
) -> PredicateClassification:
    """Classify predicates as keep vs drop based on redundancy.

    Key principle: predicates are only dropped if view predicates imply them.
    We do NOT absorb predicates just because they reference covered tables.

    Args:
        query_predicates: List of predicates from the query
        view_predicates: List of predicates from the view (for redundancy checking)
        covered_aliases: Set of alias names for covered relations
        remaining_aliases: Set of alias names for remaining relations

    Returns:
        PredicateClassification with keep/drop lists and required columns
    """
    classification = PredicateClassification()

    # Get fingerprints of view predicates for redundancy checking
    view_pred_fps = {p.fingerprint() for p in view_predicates} if view_predicates else set()

    for pred in query_predicates:
        cols = pred.referenced_columns()

        # Get tables referenced by this predicate
        tables_referenced: set[str] = set()
        for col in cols:
            if "." in col:
                table = col.split(".")[0]
                tables_referenced.add(table)

        # Check if predicate references any remaining tables
        references_remaining = bool(tables_referenced & remaining_aliases)

        if references_remaining:
            # Must keep - references remaining tables
            classification.keep.append(pred)
            # Track covered columns needed
            for col in cols:
                if "." in col:
                    table = col.split(".")[0]
                    if table in covered_aliases:
                        classification.required_covered_cols.add(col)
        else:
            # Only references covered tables - check if redundant
            pred_fp = pred.fingerprint()

            # Quick check: if predicate exactly matches a view predicate, it's redundant
            if pred_fp in view_pred_fps:
                classification.drop.append(pred)
                continue

            # Conservatively keep predicates that aren't exact matches
            # Full implementation would use PredicateImplicationChecker
            classification.keep.append(pred)
            for col in cols:
                classification.required_covered_cols.add(col)

    return classification


def rewrite_predicate_columns(
    predicate: Predicate,
    column_mapping: dict[str, str],
) -> str:
    """Rewrite predicate column references using a mapping.

    This performs string-based replacement on the predicate fingerprint.
    For proper AST rewriting, use the SQL-level rewriting in sqlkit.

    Args:
        predicate: The predicate to rewrite
        column_mapping: Maps original qualified columns (e.g., "t.col")
                       to new qualified columns (e.g., "cv.mapped_col")

    Returns:
        SQL string with rewritten column references
    """
    fp = predicate.fingerprint()

    for original, rewritten in column_mapping.items():
        fp = _replace_qualified_identifier(fp, original, rewritten)

    return fp


def rewrite_on_predicate(
    predicate: Predicate | None,
    covered_rels: set[RelationRef],
    lineage: dict[tuple[str, str], str],
    view_alias: str = "cv",
) -> str:
    """Rewrite an ON predicate to use view alias for covered columns.

    Args:
        predicate: The ON predicate to rewrite (can be None)
        covered_rels: Set of RelationRef for covered relations
        lineage: Maps (table_or_alias, column) -> view_column
        view_alias: Alias to use for the view (default: "cv")

    Returns:
        Rewritten predicate as SQL string, or empty string if None
    """
    if not predicate:
        return ""

    covered_aliases = {r.effective_name() for r in covered_rels}

    # Get the fingerprint and do string replacement
    fp = predicate.fingerprint()

    for (table, col), view_col in lineage.items():
        # Replace table.col with view_alias.view_col
        if table in covered_aliases:
            fp = _replace_qualified_identifier(fp, f"{table}.{col}", f"{view_alias}.{view_col}")

    return fp
