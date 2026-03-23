"""Validation functions for RA-level query folding.

This module provides SQL-agnostic validation for folding operations,
particularly checking whether views satisfy the SPJ (Select-Project-Join)
constraint required for partial coverage rewrites.
"""

from __future__ import annotations

from dataclasses import dataclass

from alma_algebrakit.folding.config import FoldingConfig
from alma_algebrakit.folding.types import BoundaryAnalysis
from alma_algebrakit.models.algebra import (
    Aggregation,
    Difference,
    ExistsExpression,
    InSubqueryExpression,
    Intersect,
    Join,
    Projection,
    RAExpression,
    Relation,
    Selection,
    SubqueryExpression,
    Union,
)


@dataclass
class SPJValidationResult:
    """Result of SPJ validation for a view.

    SPJ (Select-Project-Join) views are row-preserving and can be safely
    used for partial coverage rewrites. Non-SPJ views can only be used
    for full coverage.

    Attributes:
        is_spj: Whether the view is SPJ
        reason: Human-readable reason if not SPJ
        has_distinct: True if view has DISTINCT
        has_group_by: True if view has GROUP BY/aggregation
        has_union: True if view has UNION
        has_except: True if view has EXCEPT
        has_intersect: True if view has INTERSECT
        has_subquery: True if view has subqueries
    """

    is_spj: bool
    reason: str | None = None
    has_distinct: bool = False
    has_group_by: bool = False
    has_union: bool = False
    has_except: bool = False
    has_intersect: bool = False
    has_subquery: bool = False


def check_view_is_spj(
    view_ra: RAExpression | None,
    config: FoldingConfig | None = None,
) -> SPJValidationResult:
    """Check if a view RA expression is SPJ (Select-Project-Join).

    For partial coverage rewriting to be semantically correct, the view
    must be row-preserving. This means it cannot contain:
    - DISTINCT (changes row multiplicity)
    - GROUP BY / aggregation (changes grain)
    - UNION / EXCEPT / INTERSECT (changes row set)
    - Subqueries in SELECT (complex semantics)

    This function performs RA-level inspection, making it SQL-agnostic.

    Args:
        view_ra: The view as an RA expression
        config: Optional configuration

    Returns:
        SPJValidationResult with is_spj flag and details
    """
    if view_ra is None:
        return SPJValidationResult(
            is_spj=False,
            reason="View RA expression is None - cannot validate",
        )

    result = SPJValidationResult(is_spj=True)

    # Check recursively for non-SPJ constructs
    _check_spj_recursive(view_ra, result)

    return result


def _check_spj_recursive(
    expr: RAExpression,
    result: SPJValidationResult,
) -> None:
    """Recursively check for non-SPJ constructs in an RA expression."""
    if not result.is_spj:
        # Already found a violation, no need to continue
        return

    if isinstance(expr, Union):
        result.is_spj = False
        result.has_union = True
        result.reason = "View contains UNION"
        return

    if isinstance(expr, Difference):
        result.is_spj = False
        result.has_except = True
        result.reason = "View contains EXCEPT"
        return

    if isinstance(expr, Intersect):
        result.is_spj = False
        result.has_intersect = True
        result.reason = "View contains INTERSECT"
        return

    if isinstance(expr, Aggregation):
        result.is_spj = False
        result.has_group_by = True
        result.reason = "View contains GROUP BY / aggregation"
        return

    if isinstance(expr, Projection):
        # Check for DISTINCT
        if expr.distinct or expr.distinct_on:
            result.is_spj = False
            result.has_distinct = True
            result.reason = "View contains DISTINCT"
            return

        # Check for subqueries in projection columns
        for col_expr, _ in expr.columns:
            if _contains_subquery(col_expr):
                result.is_spj = False
                result.has_subquery = True
                result.reason = "View contains subquery in SELECT"
                return

        # Continue checking input
        _check_spj_recursive(expr.input, result)

    elif isinstance(expr, Selection):
        # Check for subqueries in predicate
        if _predicate_contains_subquery(expr.predicate):
            result.is_spj = False
            result.has_subquery = True
            result.reason = "View contains subquery in WHERE"
            return

        # Continue checking input
        _check_spj_recursive(expr.input, result)

    elif isinstance(expr, Join):
        # Joins are allowed in SPJ
        _check_spj_recursive(expr.left, result)
        if result.is_spj:
            _check_spj_recursive(expr.right, result)

    elif isinstance(expr, Relation):
        # Base relations are fine
        pass


def _contains_subquery(expr) -> bool:
    """Check if an expression contains a subquery."""
    if isinstance(expr, (SubqueryExpression, ExistsExpression, InSubqueryExpression)):
        return True

    # Check nested expressions
    if hasattr(expr, "operands"):
        return any(_contains_subquery(op) for op in expr.operands)
    if hasattr(expr, "arguments"):
        return any(_contains_subquery(arg) for arg in expr.arguments)
    if hasattr(expr, "left") and _contains_subquery(expr.left):
        return True
    if hasattr(expr, "right") and _contains_subquery(expr.right):
        return True

    return False


def _predicate_contains_subquery(predicate) -> bool:
    """Check if a predicate contains a subquery."""
    if isinstance(predicate, (ExistsExpression, InSubqueryExpression)):
        return True

    if hasattr(predicate, "operands"):
        return any(_predicate_contains_subquery(op) for op in predicate.operands)
    if hasattr(predicate, "left") and _predicate_contains_subquery(predicate.left):
        return True
    if hasattr(predicate, "right") and _predicate_contains_subquery(predicate.right):
        return True

    return False


def validate_partial_fold(
    query_ra: RAExpression,
    analysis: BoundaryAnalysis,
    config: FoldingConfig | None = None,
) -> tuple[bool, list[str]]:
    """Validate that a partial fold is semantically safe.

    Runs through all validation gates to ensure the partial rewrite
    will preserve query semantics.

    Validation gates (in order):
    1. Column availability - All required columns mapped via lineage
    2. No dangling relations - All remaining relations reachable via joins
    3. Outer join safety - Covered on preserved side only
    4. No correlated subqueries - No correlation to covered tables

    Note: SPJ and self-join checks are done earlier in the folding process.

    Args:
        query_ra: The original query RA expression
        analysis: The boundary analysis result
        config: Optional folding configuration

    Returns:
        (is_valid, rejection_reasons) tuple
    """
    reasons: list[str] = []

    # Gate 1: Check column availability
    if analysis.missing_cv_columns:
        reasons.append(f"Missing columns in view: {sorted(analysis.missing_cv_columns)}")

    # Gate 2: Check for dangling remaining relations
    # This is checked during boundary analysis via join reachability

    # Gate 3: Outer join safety
    # This is checked via check_outer_join_safety in algebrakit.rewriting.joins

    # Gate 4: Correlated subqueries
    subquery_safe, subquery_reason = check_correlated_subqueries_safe(query_ra, analysis)
    if not subquery_safe:
        reasons.append(subquery_reason)

    # Combine with any reasons from boundary analysis
    reasons.extend(analysis.rejection_reasons)

    return len(reasons) == 0, reasons


def check_correlated_subqueries_safe(
    query_ra: RAExpression,
    analysis: BoundaryAnalysis,
) -> tuple[bool, str]:
    """Check that correlated subqueries don't reference covered tables.

    For partial coverage rewriting, correlated subqueries that reference
    covered tables cannot be safely rewritten. The subquery would need
    the original table rows, not the view rows.

    This uses a conservative table-overlap heuristic: if any covered table
    appears in a subquery, it's assumed to be correlated.

    Args:
        query_ra: Original query RA expression
        analysis: Boundary analysis with covered relations

    Returns:
        (is_safe, reason) tuple where reason is empty string if safe
    """
    covered_tables = {r.physical_table.lower() for r in analysis.covered_rels}

    # Check all subquery expressions in the query
    unsafe, reason = _check_ra_for_correlated_subqueries(query_ra, covered_tables)
    if unsafe:
        return False, reason

    return True, ""


def _check_ra_for_correlated_subqueries(
    expr: RAExpression,
    covered_tables: set[str],
) -> tuple[bool, str]:
    """Recursively check RA expression for correlated subqueries.

    Returns:
        (is_unsafe, reason) - True if found correlated subquery
    """
    if isinstance(expr, Selection):
        # Check predicate for subqueries
        unsafe, reason = _check_expr_for_correlated_subquery(expr.predicate, covered_tables)
        if unsafe:
            return True, reason
        return _check_ra_for_correlated_subqueries(expr.input, covered_tables)

    elif isinstance(expr, Projection):
        # Check projection columns for scalar subqueries
        for col_expr, _ in expr.columns:
            unsafe, reason = _check_expr_for_correlated_subquery(col_expr, covered_tables)
            if unsafe:
                return True, reason
        return _check_ra_for_correlated_subqueries(expr.input, covered_tables)

    elif isinstance(expr, Join):
        # Check join condition
        if expr.condition:
            unsafe, reason = _check_expr_for_correlated_subquery(expr.condition, covered_tables)
            if unsafe:
                return True, reason
        unsafe, reason = _check_ra_for_correlated_subqueries(expr.left, covered_tables)
        if unsafe:
            return True, reason
        return _check_ra_for_correlated_subqueries(expr.right, covered_tables)

    elif isinstance(expr, Aggregation):
        # Check HAVING clause
        if expr.having:
            unsafe, reason = _check_expr_for_correlated_subquery(expr.having, covered_tables)
            if unsafe:
                return True, reason
        return _check_ra_for_correlated_subqueries(expr.input, covered_tables)

    return False, ""


def _check_expr_for_correlated_subquery(
    expr,
    covered_tables: set[str],
) -> tuple[bool, str]:
    """Check an expression for correlated subqueries referencing covered tables.

    Returns:
        (is_unsafe, reason) - True if found correlated subquery
    """
    if isinstance(expr, (SubqueryExpression, ExistsExpression, InSubqueryExpression)):
        # Get tables referenced by the subquery
        if hasattr(expr, "query") and expr.query:
            subquery_tables = {t.lower() for t in expr.query.referenced_tables()}
            overlap = subquery_tables & covered_tables
            if overlap:
                return True, (
                    f"Correlated subquery references covered tables: {sorted(overlap)}. "
                    "Cannot safely rewrite partial coverage with correlated subqueries."
                )

    # Check nested expressions
    if hasattr(expr, "operands"):
        for op in expr.operands:
            unsafe, reason = _check_expr_for_correlated_subquery(op, covered_tables)
            if unsafe:
                return True, reason
    if hasattr(expr, "left"):
        unsafe, reason = _check_expr_for_correlated_subquery(expr.left, covered_tables)
        if unsafe:
            return True, reason
    if hasattr(expr, "right"):
        unsafe, reason = _check_expr_for_correlated_subquery(expr.right, covered_tables)
        if unsafe:
            return True, reason

    return False, ""
