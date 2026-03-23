"""Join safety checks for relational algebra rewriting.

This module provides utilities for checking whether join rewrites are safe,
particularly for outer joins where the position of relations matters for
semantics.
"""

from __future__ import annotations

from alma_algebrakit.models.algebra import (
    JoinType,
    RAExpression,
    RelationRef,
)
from alma_algebrakit.normalizer import extract_detailed_join_graph


def check_outer_join_safety(
    query_ra: RAExpression,
    covered_rels: set[RelationRef],
    remaining_rels: set[RelationRef],
) -> tuple[bool, str]:
    """Check that outer joins are safe for partial rewriting.

    For outer joins, covered relations must form a contiguous subtree that
    occupies the same side (preserved/null-extended) it did in the original
    join tree.

    Cases:
    - LEFT JOIN from covered -> remaining: OK (covered is preserved side)
    - LEFT JOIN from remaining -> covered: NOT OK (preserved side would move)
    - covered tables split across both sides: NOT OK
    - FULL OUTER: NOT OK (unless special handling)

    Args:
        query_ra: Original query RA expression
        covered_rels: Relations that are covered by the view
        remaining_rels: Relations that remain in the query

    Returns:
        (is_safe, reason) tuple where reason is empty string if safe
    """
    join_edges = extract_detailed_join_graph(query_ra)

    for edge in join_edges:
        if not edge.crosses_partition(covered_rels, remaining_rels):
            continue

        # This is a boundary join - check safety based on type
        if edge.join_type == JoinType.FULL:
            return False, "FULL OUTER JOIN across covered/remaining boundary not supported"

        if edge.join_type == JoinType.LEFT:
            # LEFT JOIN: left side is preserved (gets all rows)
            # Safe if covered is on the left (preserved) side
            left_has_covered = bool(edge.left_rels & covered_rels)
            right_has_covered = bool(edge.right_rels & covered_rels)

            if right_has_covered and not left_has_covered:
                # Covered on right (null-extended) side of LEFT JOIN
                return False, (
                    "Covered tables on null-extended side of LEFT JOIN. "
                    "This would change semantics when folded into contract view."
                )

            if left_has_covered and right_has_covered:
                # Covered spans both sides
                return False, (
                    "Covered tables span both sides of LEFT JOIN. "
                    "Cannot safely fold partial coverage."
                )

        if edge.join_type == JoinType.RIGHT:
            # RIGHT JOIN: right side is preserved
            # Safe if covered is on the right (preserved) side
            left_has_covered = bool(edge.left_rels & covered_rels)
            right_has_covered = bool(edge.right_rels & covered_rels)

            if left_has_covered and not right_has_covered:
                # Covered on left (null-extended) side of RIGHT JOIN
                return False, (
                    "Covered tables on null-extended side of RIGHT JOIN. "
                    "This would change semantics when folded into contract view."
                )

            if left_has_covered and right_has_covered:
                # Covered spans both sides
                return False, (
                    "Covered tables span both sides of RIGHT JOIN. "
                    "Cannot safely fold partial coverage."
                )

    return True, ""
