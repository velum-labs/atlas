"""Canonical fingerprinting for bound predicates and expressions.

This module provides the **single canonical** fingerprinting implementation
for all BoundPredicate and BoundExprLike types. All other modules that need
to fingerprint bound types should use these functions instead of implementing
their own.

The fingerprint format is human-readable SQL-like syntax:
- Comparisons: "table.col = 5"
- IS NULL:     "table.col IS NULL"
- IN:          "table.col IN (...)"
- BETWEEN:     "table.col BETWEEN 1 AND 10"
- LIKE:        "table.col LIKE ..."
- Logical:     "(A AND B)"

These fingerprints are used for:
- Cache keys in PredicateImplicationChecker
- Predicate deduplication in QueryGeneralizer
- Equivalence comparison in ContainmentChecker
"""

from __future__ import annotations

from typing import Any

from alma_algebrakit.bound.types import (
    AttributeRef,
    BoundBetween,
    BoundBinaryOp,
    BoundComparison,
    BoundExpression,
    BoundFunctionCall,
    BoundIn,
    BoundIsNull,
    BoundLike,
    BoundLiteral,
    BoundLogical,
    BoundPredicate,
)


def bound_predicate_fingerprint(pred: BoundPredicate) -> str:
    """Generate a canonical fingerprint string for a bound predicate.

    Uses isinstance-based dispatch on algebrakit's own types.
    Falls back to duck typing for types from other packages that
    share the same interface (e.g., query_analyzer types).

    Args:
        pred: A bound predicate to fingerprint

    Returns:
        A canonical string representation suitable for comparison and caching
    """
    if isinstance(pred, BoundComparison):
        left = bound_expr_fingerprint(pred.left)
        right = bound_expr_fingerprint(pred.right)
        return f"{left} {pred.operator} {right}"

    if isinstance(pred, BoundIsNull):
        expr = bound_expr_fingerprint(pred.expression)
        neg = "NOT " if pred.negated else ""
        return f"{expr} IS {neg}NULL"

    if isinstance(pred, BoundIn):
        expr = bound_expr_fingerprint(pred.expression)
        vals = ", ".join(bound_expr_fingerprint(v) for v in pred.values)
        neg = "NOT " if pred.negated else ""
        return f"{expr} {neg}IN ({vals})"

    if isinstance(pred, BoundBetween):
        expr = bound_expr_fingerprint(pred.expression)
        low = bound_expr_fingerprint(pred.low)
        high = bound_expr_fingerprint(pred.high)
        neg = "NOT " if getattr(pred, "negated", False) else ""
        return f"{expr} {neg}BETWEEN {low} AND {high}"

    if isinstance(pred, BoundLike):
        expr = bound_expr_fingerprint(pred.expression)
        pattern = bound_expr_fingerprint(pred.pattern)
        neg = "NOT " if pred.negated else ""
        case = "I" if pred.case_insensitive else ""
        return f"{expr} {neg}{case}LIKE {pattern}"

    if isinstance(pred, BoundLogical):
        parts = [bound_predicate_fingerprint(op) for op in pred.operands]
        if pred.operator == "NOT" and len(parts) == 1:
            return f"NOT ({parts[0]})"
        return f"({f' {pred.operator} '.join(parts)})"

    # Duck-type fallback for types from other packages
    return _duck_type_predicate_fingerprint(pred)


def bound_expr_fingerprint(expr: Any) -> str:
    """Generate a canonical fingerprint string for a bound expression.

    Args:
        expr: A bound expression (AttributeRef, BoundLiteral, BoundExpression, etc.)

    Returns:
        A canonical string representation
    """
    if isinstance(expr, AttributeRef):
        return expr.qualified_id()

    if isinstance(expr, BoundLiteral):
        return repr(expr.value)

    if isinstance(expr, BoundFunctionCall):
        args_str = ", ".join(bound_expr_fingerprint(a) for a in expr.args)
        distinct_str = "DISTINCT " if expr.distinct else ""
        return f"{expr.function_name}({distinct_str}{args_str})"

    if isinstance(expr, BoundBinaryOp):
        left = bound_expr_fingerprint(expr.left)
        right = bound_expr_fingerprint(expr.right)
        return f"({left} {expr.operator} {right})"

    if isinstance(expr, BoundExpression):
        return expr.sql_text or "expr"

    # Duck-type fallback
    return _duck_type_expr_fingerprint(expr)


def _duck_type_predicate_fingerprint(p: Any) -> str:
    """Fallback fingerprinting using duck typing for non-algebrakit types.

    This handles predicate types from query_analyzer or other packages that
    share the same interface but aren't isinstance-compatible with algebrakit types.
    """
    # Comparison: has left, operator, right
    if hasattr(p, "left") and hasattr(p, "operator") and hasattr(p, "right"):
        left = bound_expr_fingerprint(p.left)
        right = bound_expr_fingerprint(p.right)
        return f"{left} {p.operator} {right}"

    # IS NULL: has expression, negated, but not values/low/pattern
    if (
        hasattr(p, "expression")
        and hasattr(p, "negated")
        and not hasattr(p, "values")
        and not hasattr(p, "low")
        and not hasattr(p, "pattern")
    ):
        expr = bound_expr_fingerprint(p.expression)
        neg = "NOT " if p.negated else ""
        return f"{expr} IS {neg}NULL"

    # LIKE: has expression, pattern (check before IN since LIKE also lacks 'values')
    if hasattr(p, "expression") and hasattr(p, "pattern") and not hasattr(p, "values"):
        expr = bound_expr_fingerprint(p.expression)
        pattern = bound_expr_fingerprint(p.pattern)
        neg = "NOT " if getattr(p, "negated", False) else ""
        case = "I" if getattr(p, "case_insensitive", False) else ""
        return f"{expr} {neg}{case}LIKE {pattern}"

    # IN: has expression, values
    if hasattr(p, "expression") and hasattr(p, "values"):
        expr = bound_expr_fingerprint(p.expression)
        vals = ", ".join(bound_expr_fingerprint(v) for v in p.values)
        neg = "NOT " if getattr(p, "negated", False) else ""
        return f"{expr} {neg}IN ({vals})"

    # BETWEEN: has expression, low, high
    if hasattr(p, "expression") and hasattr(p, "low") and hasattr(p, "high"):
        expr = bound_expr_fingerprint(p.expression)
        low = bound_expr_fingerprint(p.low)
        high = bound_expr_fingerprint(p.high)
        neg = "NOT " if getattr(p, "negated", False) else ""
        return f"{expr} {neg}BETWEEN {low} AND {high}"

    # Logical: has operator, operands
    if hasattr(p, "operator") and hasattr(p, "operands"):
        parts = [bound_predicate_fingerprint(op) for op in p.operands]
        if str(p.operator).upper() in ("NOT",) and len(parts) == 1:
            return f"NOT ({parts[0]})"
        return f"({f' {p.operator} '.join(parts)})"

    return str(type(p).__name__)


def _duck_type_expr_fingerprint(e: Any) -> str:
    """Fallback fingerprinting using duck typing for non-algebrakit expressions."""
    # Column/attribute reference: has qualified_id()
    if hasattr(e, "qualified_id") and callable(getattr(e, "qualified_id", None)):
        return e.qualified_id()

    # Literal: has value and data_type
    if hasattr(e, "value") and hasattr(e, "data_type"):
        return repr(e.value)

    # Expression with upstream columns
    if hasattr(e, "upstream_columns") and e.upstream_columns:
        cols = ", ".join(c.qualified_id() for c in e.upstream_columns)
        return f"expr({cols})"

    # SQL text
    if hasattr(e, "sql_text") and e.sql_text:
        return e.sql_text

    return "?"
