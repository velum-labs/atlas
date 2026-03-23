"""Equivalence checking for query rewrites.

Verifies that rewritten queries are semantically equivalent to originals
under specified assumptions (set/bag semantics, null handling, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from alma_algebrakit.models.algebra import (
    Aggregation,
    Join,
    JoinType,
    Projection,
    RAExpression,
    Selection,
)
from alma_algebrakit.normalizer import RANormalizer


class EquivalenceLevel(StrEnum):
    """Level of equivalence verification."""

    STRUCTURAL = "structural"  # Same structure after normalization
    CONTAINMENT = "containment"  # One query contains the other
    EMPIRICAL = "empirical"  # Verified by running on sample data
    SMT = "smt"  # Verified by SMT solver (for restricted fragments)


@dataclass
class EquivalenceResult:
    """Result of equivalence checking."""

    equivalent: bool
    level: EquivalenceLevel
    confidence: float
    explanation: str
    counterexample: str | None = None


class EquivalenceChecker:
    """Checks equivalence of query rewrites."""

    def __init__(self) -> None:
        self.normalizer = RANormalizer()

    def check_structural(
        self,
        expr1: RAExpression,
        expr2: RAExpression,
    ) -> EquivalenceResult:
        """Check structural equivalence after normalization.

        Two expressions are structurally equivalent if they have the same
        canonical form after applying normalization transformations.
        """
        # Normalize both expressions
        norm1 = self.normalizer.normalize(expr1)
        norm2 = self.normalizer.normalize(expr2)

        # Compare fingerprints
        fp1 = norm1.fingerprint()
        fp2 = norm2.fingerprint()

        if fp1 == fp2:
            return EquivalenceResult(
                equivalent=True,
                level=EquivalenceLevel.STRUCTURAL,
                confidence=1.0,
                explanation="Expressions have identical canonical forms",
            )

        return EquivalenceResult(
            equivalent=False,
            level=EquivalenceLevel.STRUCTURAL,
            confidence=0.0,
            explanation=f"Canonical forms differ:\n  Query 1: {fp1}\n  Query 2: {fp2}",
        )

    def check_containment(
        self,
        container: RAExpression,
        contained: RAExpression,
    ) -> EquivalenceResult:
        """Check if one expression is contained in another.

        Container ⊇ contained if every tuple in contained is also in container.
        This is useful for checking if a view can be used to answer a query.
        """
        # Check if contained's tables are subset of container's tables
        container_tables = container.referenced_tables()
        contained_tables = contained.referenced_tables()

        if not contained_tables <= container_tables:
            return EquivalenceResult(
                equivalent=False,
                level=EquivalenceLevel.CONTAINMENT,
                confidence=1.0,
                explanation=f"Tables {contained_tables - container_tables} not in container",
            )

        # Check attributes (simplified)
        container_attrs = container.output_columns()
        contained_attrs = contained.output_columns()

        # Normalize attribute names
        container_cols = {a.split(".")[-1] for a in container_attrs}
        contained_cols = {a.split(".")[-1] for a in contained_attrs if a != "*"}

        if contained_cols and not contained_cols <= container_cols:
            missing = contained_cols - container_cols
            return EquivalenceResult(
                equivalent=False,
                level=EquivalenceLevel.CONTAINMENT,
                confidence=1.0,
                explanation=f"Attributes {missing} not in container",
            )

        # For a complete implementation, we'd also check:
        # 1. Predicate containment (φ_contained ⇒ φ_container)
        # 2. Join graph containment

        return EquivalenceResult(
            equivalent=True,
            level=EquivalenceLevel.CONTAINMENT,
            confidence=0.8,  # Not fully verified
            explanation="Basic containment checks passed (tables, attributes)",
        )

    def check_join_equivalence(
        self,
        expr1: RAExpression,
        expr2: RAExpression,
    ) -> EquivalenceResult:
        """Check if two expressions have equivalent join structures.

        Only valid for inner equi-joins (outer joins cannot be freely reordered).
        """
        # Extract join graphs
        joins1 = self.normalizer.extract_join_graph(expr1)
        joins2 = self.normalizer.extract_join_graph(expr2)

        # Check if both have only inner joins
        if self._has_outer_join(expr1) or self._has_outer_join(expr2):
            return EquivalenceResult(
                equivalent=False,
                level=EquivalenceLevel.STRUCTURAL,
                confidence=0.5,
                explanation="Outer joins present; join reordering not guaranteed safe",
            )

        # Normalize join edges to sets (order-independent)
        edges1 = {self._normalize_join_edge(e) for e in joins1}
        edges2 = {self._normalize_join_edge(e) for e in joins2}

        if edges1 == edges2:
            return EquivalenceResult(
                equivalent=True,
                level=EquivalenceLevel.STRUCTURAL,
                confidence=0.9,
                explanation="Join graphs are equivalent (same edges)",
            )

        return EquivalenceResult(
            equivalent=False,
            level=EquivalenceLevel.STRUCTURAL,
            confidence=0.9,
            explanation=f"Join graphs differ:\n  Only in Q1: {edges1 - edges2}\n  Only in Q2: {edges2 - edges1}",
        )

    def _has_outer_join(self, expr: RAExpression) -> bool:
        """Check if expression contains outer joins."""
        if isinstance(expr, Join):
            if expr.join_type in (JoinType.LEFT, JoinType.RIGHT, JoinType.FULL):
                return True
            return self._has_outer_join(expr.left) or self._has_outer_join(expr.right)
        if isinstance(expr, (Selection, Projection, Aggregation)):
            return self._has_outer_join(expr.input)
        return False

    def _normalize_join_edge(
        self,
        edge: tuple[str, str, object],
    ) -> tuple[str, str]:
        """Normalize a join edge to canonical form (sorted table names)."""
        t1, t2, _ = edge
        return tuple(sorted([t1, t2]))

    def generate_empirical_test(
        self,
        expr1: RAExpression,
        expr2: RAExpression,
    ) -> str:
        """Generate documentation for empirically testing equivalence.

        Note: To actually emit SQL, use sqlkit.emit_sql().
        """
        fp1 = expr1.fingerprint()
        fp2 = expr2.fingerprint()

        return f"""
-- Empirical equivalence test
-- Expression 1: {fp1}
-- Expression 2: {fp2}
--
-- To test equivalence, run both queries and compare:
-- 1. Row counts should match
-- 2. EXCEPT in both directions should return 0 rows
-- 3. Checksums should match (for large results)

-- Test 1: Compare row counts
-- Run each query with COUNT(*) and compare

-- Test 2: Symmetric difference
-- (Q1 EXCEPT Q2) UNION ALL (Q2 EXCEPT Q1)
-- Should return 0 rows if equivalent
""".strip()
