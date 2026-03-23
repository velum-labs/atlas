"""Outer join safety inference using schema constraints.

This module provides algorithms for determining when outer joins can be
safely treated as inner joins based on schema constraints (FK, NOT NULL).

Theoretical Foundation:
    Theorem: L LEFT JOIN R ON L.a = R.b ≡ L INNER JOIN R ON L.a = R.b if:
    1. R.b IS NOT NULL (no NULLs in the join column)
    2. FK: L.a → R.b exists (referential integrity)

    Proof:
    - NOT NULL on R.b: Every row in R has a non-NULL value for b
    - FK L.a → R.b: Every non-NULL value in L.a exists in R.b
    - Together: Every row in L with non-NULL L.a finds a match in R
    - If L.a can be NULL: Those rows get NULL-extended regardless
    - If L.a IS NOT NULL: Every row in L finds a match → no NULL extension

    The key insight is that the LEFT JOIN's NULL-extension behavior only
    activates when no match is found. With FK + NOT NULL, a match is
    guaranteed for all rows.

This enables safe partial coverage folding in cases that would otherwise
be rejected due to covered tables being on the "null-extended" side.

Example:
    Schema:
    - orders.user_id FK → users.id
    - users.id NOT NULL (PK)

    Query: users LEFT JOIN orders ON users.id = orders.user_id

    Without inference: If view covers 'orders', this would be rejected
    (orders is on null-extended side)

    With inference: The FK + NOT NULL means every users.id has a match,
    so this is effectively INNER JOIN → safe to fold
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from alma_algebrakit.models.algebra import JoinType, RelationRef
from alma_algebrakit.normalizer import JoinEdge
from alma_algebrakit.schema.constraints import SchemaConstraints


class JoinInferenceResult(Enum):
    """Result of join type inference."""

    INNER = "inner"  # Can be treated as inner join
    UNCHANGED = "unchanged"  # Must keep original join type
    UNKNOWN = "unknown"  # Cannot determine (missing info)


@dataclass
class OuterJoinInference:
    """Result of analyzing an outer join for potential inference to inner.

    Attributes:
        original_type: The original join type
        effective_type: The effective join type after inference
        can_infer_inner: Whether join can be safely treated as inner
        reason: Explanation of the inference (or why not)
        confidence: Confidence level (1.0 = proven, 0.5 = heuristic)
    """

    original_type: JoinType
    effective_type: JoinType
    can_infer_inner: bool
    reason: str
    confidence: float = 1.0


def infer_join_type(
    join_type: JoinType,
    left_table: str,
    left_col: str,
    right_table: str,
    right_col: str,
    constraints: SchemaConstraints,
) -> OuterJoinInference:
    """Infer the effective join type given schema constraints.

    Uses FK and NOT NULL constraints to determine if an outer join
    can be safely treated as an inner join.

    Args:
        join_type: Original join type
        left_table: Left table name (or alias)
        left_col: Join column from left table
        right_table: Right table name (or alias)
        right_col: Join column from right table
        constraints: Schema constraints to use for inference

    Returns:
        OuterJoinInference with effective type and explanation
    """
    # Inner and cross joins don't need inference
    if join_type in (JoinType.INNER, JoinType.CROSS):
        return OuterJoinInference(
            original_type=join_type,
            effective_type=join_type,
            can_infer_inner=True,
            reason="Already inner/cross join",
        )

    if join_type == JoinType.LEFT:
        # LEFT JOIN: Right side is null-extended
        # Can infer INNER if: FK L→R and R.col NOT NULL
        can_infer, reason = constraints.can_infer_inner_join(
            left_table, left_col, right_table, right_col
        )

        if can_infer:
            return OuterJoinInference(
                original_type=JoinType.LEFT,
                effective_type=JoinType.INNER,
                can_infer_inner=True,
                reason=reason,
            )
        else:
            return OuterJoinInference(
                original_type=JoinType.LEFT,
                effective_type=JoinType.LEFT,
                can_infer_inner=False,
                reason=reason,
            )

    if join_type == JoinType.RIGHT:
        # RIGHT JOIN: Left side is null-extended
        # Can infer INNER if: FK R→L and L.col NOT NULL
        can_infer, reason = constraints.can_infer_inner_from_right_join(
            left_table, left_col, right_table, right_col
        )

        if can_infer:
            return OuterJoinInference(
                original_type=JoinType.RIGHT,
                effective_type=JoinType.INNER,
                can_infer_inner=True,
                reason=reason,
            )
        else:
            return OuterJoinInference(
                original_type=JoinType.RIGHT,
                effective_type=JoinType.RIGHT,
                can_infer_inner=False,
                reason=reason,
            )

    if join_type == JoinType.FULL:
        # FULL OUTER: Both sides can be null-extended
        # Can only infer INNER if BOTH directions have FK + NOT NULL
        left_to_right, reason_lr = constraints.can_infer_inner_join(
            left_table, left_col, right_table, right_col
        )
        right_to_left, reason_rl = constraints.can_infer_inner_from_right_join(
            left_table, left_col, right_table, right_col
        )

        if left_to_right and right_to_left:
            return OuterJoinInference(
                original_type=JoinType.FULL,
                effective_type=JoinType.INNER,
                can_infer_inner=True,
                reason="Symmetric FK + NOT NULL allows INNER inference",
            )
        else:
            reasons = []
            if not left_to_right:
                reasons.append(f"L→R: {reason_lr}")
            if not right_to_left:
                reasons.append(f"R→L: {reason_rl}")
            return OuterJoinInference(
                original_type=JoinType.FULL,
                effective_type=JoinType.FULL,
                can_infer_inner=False,
                reason=f"FULL OUTER requires symmetric FKs: {'; '.join(reasons)}",
            )

    # Unknown join type
    return OuterJoinInference(
        original_type=join_type,
        effective_type=join_type,
        can_infer_inner=False,
        reason=f"Unknown join type: {join_type}",
        confidence=0.0,
    )


def infer_outer_join_safety(
    join_edge: JoinEdge,
    covered_rels: set[RelationRef],
    remaining_rels: set[RelationRef],
    constraints: SchemaConstraints,
) -> tuple[bool, str, JoinType | None]:
    """Infer if an outer join at the boundary is safe for partial coverage.

    This is the main entry point for outer join inference during boundary
    analysis. It determines whether an outer join that crosses the
    covered/remaining boundary can be safely folded.

    An outer join at the boundary is safe if either:
    1. The covered side is on the preserved (non-null-extended) side
    2. The outer join can be inferred as inner join via constraints

    Args:
        join_edge: The join edge to analyze
        covered_rels: Relations covered by the view
        remaining_rels: Relations not covered
        constraints: Schema constraints for inference

    Returns:
        (is_safe, reason, effective_join_type) tuple
        effective_join_type is the type to use for rewriting (may be INNER)
    """
    if not join_edge.crosses_partition(covered_rels, remaining_rels):
        return True, "Join does not cross covered/remaining boundary", None

    # Determine which side has covered vs remaining
    left_has_covered = bool(join_edge.left_rels & covered_rels)
    right_has_covered = bool(join_edge.right_rels & covered_rels)
    # Note: _left_has_remaining and _right_has_remaining computed for potential future use
    _left_has_remaining = bool(join_edge.left_rels & remaining_rels)
    _right_has_remaining = bool(join_edge.right_rels & remaining_rels)

    # Extract join columns from the predicate
    join_cols = _extract_join_columns(join_edge)
    if not join_cols:
        return False, "Cannot extract join columns from predicate", None

    left_table, left_col, right_table, right_col = join_cols

    # Try constraint-based inference
    inference = infer_join_type(
        join_edge.join_type,
        left_table,
        left_col,
        right_table,
        right_col,
        constraints,
    )

    if inference.can_infer_inner:
        # Outer join effectively becomes inner - always safe
        return True, inference.reason, JoinType.INNER

    # Check positional safety based on original join type
    if join_edge.join_type == JoinType.LEFT:
        # LEFT: Left is preserved, right is null-extended
        if left_has_covered and not right_has_covered:
            # Covered on preserved side - safe
            return True, "Covered on preserved (left) side of LEFT JOIN", JoinType.LEFT
        elif right_has_covered and not left_has_covered:
            # Covered on null-extended side - unsafe
            return (
                False,
                (
                    f"Covered on null-extended (right) side of LEFT JOIN. "
                    f"Inference failed: {inference.reason}"
                ),
                None,
            )
        else:
            # Covered spans both sides
            return (
                False,
                (
                    f"Covered tables span both sides of LEFT JOIN. "
                    f"Inference failed: {inference.reason}"
                ),
                None,
            )

    elif join_edge.join_type == JoinType.RIGHT:
        # RIGHT: Right is preserved, left is null-extended
        if right_has_covered and not left_has_covered:
            # Covered on preserved side - safe
            return True, "Covered on preserved (right) side of RIGHT JOIN", JoinType.RIGHT
        elif left_has_covered and not right_has_covered:
            # Covered on null-extended side - unsafe
            return (
                False,
                (
                    f"Covered on null-extended (left) side of RIGHT JOIN. "
                    f"Inference failed: {inference.reason}"
                ),
                None,
            )
        else:
            return (
                False,
                (
                    f"Covered tables span both sides of RIGHT JOIN. "
                    f"Inference failed: {inference.reason}"
                ),
                None,
            )

    elif join_edge.join_type == JoinType.FULL:
        # FULL: Both sides can be null-extended
        # Only safe if inference to INNER succeeded (already handled above)
        return (
            False,
            (
                f"FULL OUTER JOIN at boundary requires symmetric FK + NOT NULL. "
                f"Inference failed: {inference.reason}"
            ),
            None,
        )

    # Inner join is always safe at boundary
    return True, "Inner join at boundary is always safe", JoinType.INNER


def _extract_join_columns(
    join_edge: JoinEdge,
) -> tuple[str, str, str, str] | None:
    """Extract (left_table, left_col, right_table, right_col) from join predicate.

    Returns None if the predicate is not a simple equi-join or if
    table/column information cannot be extracted.
    """
    if not join_edge.on_predicate:
        return None

    pred = join_edge.on_predicate

    # Handle atomic predicate (simple case)
    if hasattr(pred, "left") and hasattr(pred, "right"):
        left_expr = pred.left
        right_expr = pred.right

        # Check if both sides are column references
        if hasattr(left_expr, "table") and hasattr(left_expr, "column"):
            if hasattr(right_expr, "table") and hasattr(right_expr, "column"):
                return (
                    left_expr.table or "",
                    left_expr.column,
                    right_expr.table or "",
                    right_expr.column,
                )

    # For compound predicates, try to find the first equi-join condition
    if hasattr(pred, "operands"):
        for operand in pred.operands:
            result = _extract_join_columns_from_predicate(operand)
            if result:
                return result

    return None


def _extract_join_columns_from_predicate(pred) -> tuple[str, str, str, str] | None:
    """Recursively extract join columns from a predicate."""
    if hasattr(pred, "left") and hasattr(pred, "right"):
        left_expr = pred.left
        right_expr = pred.right

        if hasattr(left_expr, "table") and hasattr(left_expr, "column"):
            if hasattr(right_expr, "table") and hasattr(right_expr, "column"):
                return (
                    left_expr.table or "",
                    left_expr.column,
                    right_expr.table or "",
                    right_expr.column,
                )

    return None


def analyze_boundary_joins_with_constraints(
    join_edges: list[JoinEdge],
    covered_rels: set[RelationRef],
    remaining_rels: set[RelationRef],
    constraints: SchemaConstraints | None,
) -> tuple[bool, list[str], dict[str, JoinType]]:
    """Analyze all boundary joins using constraint-based inference.

    This function analyzes each join that crosses the covered/remaining
    boundary and determines:
    1. Whether all boundary joins are safe for partial coverage
    2. The effective join types after inference

    Args:
        join_edges: All join edges in the query
        covered_rels: Relations covered by the view
        remaining_rels: Relations not covered
        constraints: Schema constraints (optional, enables inference)

    Returns:
        (all_safe, reasons, effective_types) tuple where:
        - all_safe: True if all boundary joins are safe
        - reasons: List of rejection reasons (if any)
        - effective_types: Map of join fingerprint to effective type
    """
    if constraints is None:
        # Without constraints, fall back to positional analysis only
        return _analyze_boundary_joins_positional(join_edges, covered_rels, remaining_rels)

    reasons: list[str] = []
    effective_types: dict[str, JoinType] = {}

    for edge in join_edges:
        if not edge.crosses_partition(covered_rels, remaining_rels):
            continue

        is_safe, reason, effective_type = infer_outer_join_safety(
            edge, covered_rels, remaining_rels, constraints
        )

        if not is_safe:
            reasons.append(reason)
        elif effective_type:
            # Store the effective type for this join
            edge_key = edge.on_predicate.fingerprint() if edge.on_predicate else str(id(edge))
            effective_types[edge_key] = effective_type

    return len(reasons) == 0, reasons, effective_types


def _analyze_boundary_joins_positional(
    join_edges: list[JoinEdge],
    covered_rels: set[RelationRef],
    remaining_rels: set[RelationRef],
) -> tuple[bool, list[str], dict[str, JoinType]]:
    """Analyze boundary joins using positional analysis only (no constraints).

    This is the fallback when no schema constraints are available.
    Uses the conservative rules: covered must be on preserved side.
    """
    reasons: list[str] = []
    effective_types: dict[str, JoinType] = {}

    for edge in join_edges:
        if not edge.crosses_partition(covered_rels, remaining_rels):
            continue

        left_has_covered = bool(edge.left_rels & covered_rels)
        right_has_covered = bool(edge.right_rels & covered_rels)

        if edge.join_type == JoinType.LEFT:
            if right_has_covered and not left_has_covered:
                reasons.append("Covered on null-extended (right) side of LEFT JOIN")
            elif left_has_covered and right_has_covered:
                reasons.append("Covered spans both sides of LEFT JOIN")

        elif edge.join_type == JoinType.RIGHT:
            if left_has_covered and not right_has_covered:
                reasons.append("Covered on null-extended (left) side of RIGHT JOIN")
            elif left_has_covered and right_has_covered:
                reasons.append("Covered spans both sides of RIGHT JOIN")

        elif edge.join_type == JoinType.FULL:
            reasons.append("FULL OUTER JOIN at covered/remaining boundary not supported")

    return len(reasons) == 0, reasons, effective_types
