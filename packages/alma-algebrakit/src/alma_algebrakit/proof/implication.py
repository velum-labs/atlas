"""Predicate implication checking with SQL NULL semantics.

Implements the key correctness criterion for query folding:
    φ_Q ⇒ φ_V  (query predicate implies view predicate)

Under SQL 3-valued logic with WHERE semantics:
- passes(p) = predicate evaluates to TRUE (not FALSE, not UNKNOWN)
- Prove: passes(p1) ⇒ passes(p2)
- By checking unsat of: passes(p1) ∧ ¬passes(p2)

Two-layer approach:
1. Cheap syntactic/algebraic checks (fast, handles common cases)
2. SMT solver (Z3) for complex cases with timeout

Z3 Type Handling:
- Numeric types (INTEGER, BIGINT, etc.) use z3.IntSort() or z3.RealSort()
- String types (VARCHAR, TEXT, CHAR) use z3.StringSort() with str.contains, etc.
- Boolean types use z3.BoolSort()
- Date/Time types are encoded as integers (epoch seconds/days)
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections import OrderedDict
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from alma_algebrakit.bound.types import (
    BoundBetween,
    BoundComparison,
    BoundIn,
    BoundIsNull,
    BoundLike,
    BoundLogical,
    BoundPredicate,
)
from alma_algebrakit.schema.types import DataType, SQLDataType

logger = logging.getLogger(__name__)


# =============================================================================
# Z3 Type Mapping
# =============================================================================


def _get_z3_sort(col_id: str, type_env: dict[str, DataType] | None, z3: Any) -> Any:
    """Map SQL types to Z3 sorts based on type_env.

    Args:
        col_id: Column identifier
        type_env: Type environment mapping column IDs to DataType
        z3: The z3 module

    Returns:
        Z3 sort (StringSort, IntSort, RealSort, or BoolSort)
    """
    if type_env is None:
        # Default to Real for backward compatibility
        return z3.RealSort()

    data_type = type_env.get(col_id)
    if data_type is None:
        return z3.RealSort()

    base_type = data_type.base_type if isinstance(data_type, DataType) else data_type

    # String types -> StringSort
    if base_type in (SQLDataType.VARCHAR, SQLDataType.TEXT, SQLDataType.CHAR):
        return z3.StringSort()

    # Integer types -> IntSort
    if base_type in (SQLDataType.INTEGER, SQLDataType.BIGINT, SQLDataType.SMALLINT):
        return z3.IntSort()

    # Floating point types -> RealSort
    if base_type in (
        SQLDataType.DECIMAL,
        SQLDataType.NUMERIC,
        SQLDataType.REAL,
        SQLDataType.DOUBLE,
    ):
        return z3.RealSort()

    # Boolean -> BoolSort
    if base_type == SQLDataType.BOOLEAN:
        return z3.BoolSort()

    # Date/Time -> IntSort (epoch representation)
    if base_type in (
        SQLDataType.DATE,
        SQLDataType.TIME,
        SQLDataType.TIMESTAMP,
        SQLDataType.TIMESTAMPTZ,
    ):
        return z3.IntSort()

    # UUID -> StringSort (string representation)
    if base_type == SQLDataType.UUID:
        return z3.StringSort()

    # Default to RealSort for unknown types
    return z3.RealSort()


def _is_string_type(col_id: str, type_env: dict[str, DataType] | None) -> bool:
    """Check if a column is a string type based on type_env."""
    if type_env is None:
        return False

    data_type = type_env.get(col_id)
    if data_type is None:
        return False

    base_type = data_type.base_type if isinstance(data_type, DataType) else data_type
    return base_type in (SQLDataType.VARCHAR, SQLDataType.TEXT, SQLDataType.CHAR, SQLDataType.UUID)


def _is_integer_type(col_id: str, type_env: dict[str, DataType] | None) -> bool:
    """Check if a column is an integer type based on type_env."""
    if type_env is None:
        return False

    data_type = type_env.get(col_id)
    if data_type is None:
        return False

    base_type = data_type.base_type if isinstance(data_type, DataType) else data_type
    return base_type in (SQLDataType.INTEGER, SQLDataType.BIGINT, SQLDataType.SMALLINT)


def _get_compatible_z3_sort_and_vars(
    left_sort: Any,
    right_sort: Any,
    left_var: Any,
    right_val: Any,
    z3: Any,
) -> tuple[Any, Any, Any]:
    """Get compatible Z3 sort and converted values for mixed-type comparisons.

    SQL type promotion rules (simplified):
    - INTEGER + DECIMAL -> DECIMAL (both promoted to Real)
    - INTEGER + REAL -> REAL
    - Strings are not promoted

    Args:
        left_sort: Z3 sort of the left operand (column)
        right_sort: Z3 sort for the right operand (literal), or None for auto-detect
        left_var: Z3 variable for the column
        right_val: Python value for the literal
        z3: The z3 module

    Returns:
        (common_sort, converted_left_var, converted_right_val) tuple
    """
    # String types are not mixed with numeric
    if left_sort == z3.StringSort():
        return left_sort, left_var, z3.StringVal(str(right_val))

    # Both integers - keep as int
    if left_sort == z3.IntSort():
        try:
            int_val = int(right_val)
            # Check if value fits in int without loss
            if int_val == float(right_val):
                return z3.IntSort(), left_var, int_val
            # Value has decimal part - promote both to Real
            return z3.RealSort(), z3.ToReal(left_var), float(right_val)
        except (ValueError, TypeError):
            # Can't convert - try Real
            return z3.RealSort(), z3.ToReal(left_var), float(right_val)

    # Left is Real - convert right to float
    if left_sort == z3.RealSort():
        return z3.RealSort(), left_var, float(right_val)

    # Bool comparison
    if left_sort == z3.BoolSort():
        bool_val = bool(right_val) if not isinstance(right_val, bool) else right_val
        return z3.BoolSort(), left_var, bool_val

    # Fallback - use Real
    return z3.RealSort(), left_var, float(right_val)


def _like_pattern_to_z3_regex(pattern: str, z3: Any) -> Any:
    """Convert SQL LIKE pattern to Z3 regex.

    SQL LIKE wildcards:
    - % matches any sequence of characters (including empty)
    - _ matches any single character
    - \\ escapes the next character

    Args:
        pattern: SQL LIKE pattern
        z3: The z3 module

    Returns:
        Z3 regex expression
    """
    # Build regex pattern
    regex_parts = []
    i = 0
    while i < len(pattern):
        char = pattern[i]
        if char == "%":
            # Match any sequence
            regex_parts.append(z3.Star(z3.Re(z3.Full(z3.StringSort()))))
        elif char == "_":
            # Match any single character
            regex_parts.append(z3.Re(z3.Full(z3.StringSort())))
        elif char == "\\" and i + 1 < len(pattern):
            # Escaped character - match literally
            next_char = pattern[i + 1]
            regex_parts.append(z3.Re(z3.StringVal(next_char)))
            i += 1
        else:
            # Match literal character
            regex_parts.append(z3.Re(z3.StringVal(char)))
        i += 1

    # Concatenate all parts
    if not regex_parts:
        return z3.Re(z3.StringVal(""))

    result = regex_parts[0]
    for part in regex_parts[1:]:
        result = z3.Concat(result, part)

    return result


class ImplicationMethod(StrEnum):
    """Method used to prove implication."""

    SYNTACTIC = "syntactic"  # Fast syntactic check
    ALGEBRAIC = "algebraic"  # Algebraic reasoning (ranges, sets)
    LINEAR_ARITHMETIC = "linear_arithmetic"  # Fourier-Motzkin elimination
    SMT = "smt"  # Z3 SMT solver
    UNKNOWN = "unknown"  # Could not prove


class ImplicationResult(BaseModel):
    """Result of implication checking."""

    implies: bool | None = Field(
        description="True if proven to imply, False if disproven, None if unknown"
    )
    method: ImplicationMethod = Field(description="Method that produced the result")
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    explanation: str = Field(default="")
    solver_time_ms: float | None = Field(default=None)
    cached: bool = Field(default=False)

    def is_proven(self) -> bool:
        """Check if implication was proven true."""
        return self.implies is True

    def is_disproven(self) -> bool:
        """Check if implication was proven false."""
        return self.implies is False

    def is_unknown(self) -> bool:
        """Check if result is unknown."""
        return self.implies is None


class LRUCache:
    """Simple LRU cache implementation using OrderedDict.

    Evicts least recently used items when capacity is reached.
    """

    def __init__(self, maxsize: int = 10000):
        self._cache: OrderedDict[str, ImplicationResult] = OrderedDict()
        self._maxsize = maxsize
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> ImplicationResult | None:
        """Get item from cache, updating LRU order."""
        if key in self._cache:
            self._hits += 1
            # Move to end (most recently used)
            self._cache.move_to_end(key)
            return self._cache[key]
        self._misses += 1
        return None

    def put(self, key: str, value: ImplicationResult) -> None:
        """Add item to cache, evicting LRU if necessary."""
        if key in self._cache:
            # Update and move to end
            self._cache.move_to_end(key)
            self._cache[key] = value
        else:
            # Evict if at capacity
            while len(self._cache) >= self._maxsize:
                self._cache.popitem(last=False)  # Remove oldest (first)
            self._cache[key] = value

    def __len__(self) -> int:
        return len(self._cache)

    def clear(self) -> None:
        """Clear the cache."""
        self._cache.clear()

    @property
    def hit_rate(self) -> float:
        """Return cache hit rate."""
        total = self._hits + self._misses
        if total == 0:
            return 0.0
        return self._hits / total

    def stats(self) -> dict[str, Any]:
        """Return cache statistics."""
        return {
            "size": len(self._cache),
            "maxsize": self._maxsize,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self.hit_rate,
        }


class PredicateImplicationChecker:
    """Checks predicate implication under SQL WHERE semantics.

    Usage:
        checker = PredicateImplicationChecker()
        result = checker.check_implication(query_pred, view_pred)
        if result.is_proven():
            # Safe to fold query into view

    Args:
        smt_timeout_ms: Timeout for SMT solver in milliseconds
        use_smt: Whether to use SMT solver for complex cases
        cache_size: Maximum number of results to cache (LRU eviction)
        config: Optional ProofConfig to use (overrides other parameters)
    """

    @staticmethod
    def _z3_available() -> bool:
        """Check if Z3 SMT solver is available (class-level check for skipif)."""
        try:
            import z3  # noqa: F401

            return True
        except ImportError:
            return False

    def __init__(
        self,
        smt_timeout_ms: int = 1000,
        use_smt: bool = True,
        use_linear_arithmetic: bool = True,
        cache_size: int = 10000,
        config: Any = None,  # ProofConfig if provided
    ):
        # Use config if provided, otherwise use explicit parameters
        if config is not None:
            self.smt_timeout_ms = getattr(config, "smt_timeout_ms", smt_timeout_ms)
            self.use_smt = getattr(config, "use_smt", use_smt)
            self.use_linear_arithmetic = getattr(
                config, "use_linear_arithmetic", use_linear_arithmetic
            )
            cache_size = getattr(config, "cache_size", cache_size)
        else:
            self.smt_timeout_ms = smt_timeout_ms
            self.use_smt = use_smt
            self.use_linear_arithmetic = use_linear_arithmetic

        # Use LRU cache for better memory management
        self._cache = LRUCache(maxsize=cache_size)
        self._z3_available = False

        # Try to import Z3
        if self.use_smt:
            import importlib.util

            if importlib.util.find_spec("z3") is not None:
                self._z3_available = True
                logger.debug("Z3 SMT solver available")
            else:
                self._z3_available = False
                logger.debug("Z3 not available, SMT proofs disabled")

    def check_implication(
        self,
        p1: BoundPredicate,
        p2: BoundPredicate,
        type_env: dict[str, DataType] | None = None,
    ) -> ImplicationResult:
        """Check if p1 implies p2 under SQL WHERE semantics.

        Args:
            p1: Antecedent predicate (e.g., query WHERE clause)
            p2: Consequent predicate (e.g., view WHERE clause)
            type_env: Optional type information for columns

        Returns:
            ImplicationResult indicating whether p1 ⇒ p2
        """
        # Check cache (LRU)
        cache_key = self._cache_key(p1, p2)
        cached_result = self._cache.get(cache_key)
        if cached_result is not None:
            return ImplicationResult(
                implies=cached_result.implies,
                method=cached_result.method,
                confidence=cached_result.confidence,
                explanation=cached_result.explanation,
                cached=True,
            )

        # Layer 1: Cheap syntactic checks
        result = self._check_syntactic(p1, p2)
        if result.implies is not None:
            self._cache.put(cache_key, result)
            return result

        # Layer 2: Algebraic reasoning
        result = self._check_algebraic(p1, p2, type_env)
        if result.implies is not None:
            self._cache.put(cache_key, result)
            return result

        # Layer 3: Linear arithmetic (Fourier-Motzkin) - complete for linear predicates
        if self.use_linear_arithmetic:
            result = self._check_linear_arithmetic(p1, p2)
            if result.implies is not None:
                self._cache.put(cache_key, result)
                return result

        # Layer 4: SMT solver
        if self.use_smt and self._z3_available:
            result = self._check_smt(p1, p2, type_env)
            self._cache.put(cache_key, result)
            return result

        # Cannot prove
        return ImplicationResult(
            implies=None,
            method=ImplicationMethod.UNKNOWN,
            explanation="Could not prove implication (SMT not available or disabled)",
        )

    def cache_stats(self) -> dict[str, Any]:
        """Return cache statistics for monitoring."""
        return self._cache.stats()

    def clear_cache(self) -> None:
        """Clear the implication cache."""
        self._cache.clear()

    def _cache_key(self, p1: BoundPredicate, p2: BoundPredicate) -> str:
        """Generate cache key for predicate pair."""
        from alma_algebrakit.bound.fingerprint import bound_predicate_fingerprint

        s1 = bound_predicate_fingerprint(p1)
        s2 = bound_predicate_fingerprint(p2)
        return hashlib.sha256(f"{s1}||{s2}".encode()).hexdigest()[:32]

    # =========================================================================
    # Layer 1: Syntactic checks (fast)
    # =========================================================================

    def _check_syntactic(self, p1: BoundPredicate, p2: BoundPredicate) -> ImplicationResult:
        """Fast syntactic implication checks.

        Uses duck typing to support both algebrakit and query_analyzer types.
        """
        # Same predicate
        if self._predicates_equal(p1, p2):
            return ImplicationResult(
                implies=True,
                method=ImplicationMethod.SYNTACTIC,
                explanation="Predicates are syntactically equal",
            )

        # Duck type: logical predicate (has operator, operands)
        p1_is_logical = hasattr(p1, "operator") and hasattr(p1, "operands")
        p2_is_logical = hasattr(p2, "operator") and hasattr(p2, "operands")

        # Duck type: IS NULL predicate (has expression, negated, but not values/low)
        p2_is_null = (
            hasattr(p2, "expression")
            and hasattr(p2, "negated")
            and not hasattr(p2, "values")
            and not hasattr(p2, "low")
        )

        # p1 is AND containing p2
        if p1_is_logical and p1.operator == "AND":
            for operand in p1.operands:
                if self._predicates_equal(operand, p2):
                    return ImplicationResult(
                        implies=True,
                        method=ImplicationMethod.SYNTACTIC,
                        explanation="p2 is a conjunct of p1",
                    )
                # Recursive check
                result = self._check_syntactic(operand, p2)
                if result.is_proven():
                    return result

        # p2 is OR - p1 implies p2 if p1 implies any disjunct
        if p2_is_logical and p2.operator == "OR":
            for operand in p2.operands:
                if self._predicates_equal(p1, operand):
                    return ImplicationResult(
                        implies=True,
                        method=ImplicationMethod.SYNTACTIC,
                        explanation="p1 equals a disjunct of p2",
                    )

        # IS NOT NULL implied by comparison (col = lit, col > lit, etc.)
        if p2_is_null and p2.negated:  # IS NOT NULL
            if self._implies_not_null(p1, p2.expression):
                return ImplicationResult(
                    implies=True,
                    method=ImplicationMethod.SYNTACTIC,
                    explanation="Comparison implies IS NOT NULL",
                )

        return ImplicationResult(implies=None, method=ImplicationMethod.SYNTACTIC)

    def _predicates_equal(self, p1: BoundPredicate, p2: BoundPredicate) -> bool:
        """Check if two predicates are syntactically equal."""
        from alma_algebrakit.bound.fingerprint import bound_predicate_fingerprint

        return bound_predicate_fingerprint(p1) == bound_predicate_fingerprint(p2)

    def _implies_not_null(self, p: BoundPredicate, expr: Any) -> bool:
        """Check if predicate p implies expr IS NOT NULL.

        Uses duck typing to support both algebrakit and query_analyzer types.
        """
        # Any comparison with a column implies it's not null
        # (because NULL comparisons yield UNKNOWN, filtered by WHERE)

        # Duck type: comparison predicate (has left, operator, right)
        if hasattr(p, "left") and hasattr(p, "operator") and hasattr(p, "right"):
            if self._exprs_overlap(p.left, expr) or self._exprs_overlap(p.right, expr):
                return True

        # Duck type: IN predicate (has expression, values)
        if hasattr(p, "expression") and hasattr(p, "values"):
            if self._exprs_overlap(p.expression, expr):
                return True

        # Duck type: BETWEEN predicate (has expression, low, high)
        if hasattr(p, "expression") and hasattr(p, "low") and hasattr(p, "high"):
            if self._exprs_overlap(p.expression, expr):
                return True

        # Duck type: logical AND (has operator, operands)
        if hasattr(p, "operator") and hasattr(p, "operands") and p.operator == "AND":
            for operand in p.operands:
                if self._implies_not_null(operand, expr):
                    return True

        return False

    def _exprs_overlap(self, e1: Any, e2: Any) -> bool:
        """Check if expressions reference the same columns."""
        cols1 = self._get_columns(e1)
        cols2 = self._get_columns(e2)
        return bool(cols1 & cols2)

    def _get_columns(self, e: Any) -> set[str]:
        """Get column IDs from an expression.

        Uses duck typing to support both algebrakit and query_analyzer types.
        """
        # Duck type: column/attribute reference (has qualified_id method)
        if hasattr(e, "qualified_id") and callable(getattr(e, "qualified_id", None)):
            return {e.qualified_id()}
        # Duck type: expression with column dependencies
        if hasattr(e, "upstream_columns"):
            return {c.qualified_id() for c in e.upstream_columns}
        return set()

    # =========================================================================
    # Layer 2: Algebraic reasoning
    # =========================================================================

    def _check_algebraic(
        self,
        p1: BoundPredicate,
        p2: BoundPredicate,
        type_env: dict[str, DataType] | None,
    ) -> ImplicationResult:
        """Algebraic implication checks (ranges, sets).

        Uses duck typing to support both algebrakit and query_analyzer types.
        """
        # Duck type: comparison predicate (has left, operator, right)
        p1_is_comparison = hasattr(p1, "left") and hasattr(p1, "operator") and hasattr(p1, "right")
        p2_is_comparison = hasattr(p2, "left") and hasattr(p2, "operator") and hasattr(p2, "right")

        # Duck type: IN predicate (has expression, values)
        p1_is_in = hasattr(p1, "expression") and hasattr(p1, "values")
        p2_is_in = hasattr(p2, "expression") and hasattr(p2, "values")

        # Duck type: BETWEEN predicate (has expression, low, high)
        p1_is_between = hasattr(p1, "expression") and hasattr(p1, "low") and hasattr(p1, "high")

        # Range containment: (x > 5) implies (x > 3)
        if p1_is_comparison and p2_is_comparison:
            result = self._check_range_containment(p1, p2)
            if result.implies is not None:
                return result

        # IN subset: (x IN (1,2)) implies (x IN (1,2,3))
        if p1_is_in and p2_is_in:
            result = self._check_in_subset(p1, p2)
            if result.implies is not None:
                return result

        # BETWEEN implies range comparisons
        if p1_is_between and p2_is_comparison:
            result = self._check_between_implies_comparison(p1, p2)
            if result.implies is not None:
                return result

        # BETWEEN containment: (x BETWEEN 5 AND 8) implies (x BETWEEN 3 AND 10)
        p2_is_between = hasattr(p2, "expression") and hasattr(p2, "low") and hasattr(p2, "high")
        if p1_is_between and p2_is_between:
            result = self._check_between_containment(p1, p2)
            if result.implies is not None:
                return result

        return ImplicationResult(implies=None, method=ImplicationMethod.ALGEBRAIC)

    def _check_range_containment(
        self, p1: BoundComparison, p2: BoundComparison
    ) -> ImplicationResult:
        """Check if range comparison p1 implies p2."""
        # Must be on same column
        cols1 = self._get_columns(p1.left)
        cols2 = self._get_columns(p2.left)

        if cols1 != cols2 or not cols1:
            return ImplicationResult(implies=None, method=ImplicationMethod.ALGEBRAIC)

        # Get literal values
        val1 = self._get_literal_value(p1.right)
        val2 = self._get_literal_value(p2.right)

        if val1 is None or val2 is None:
            return ImplicationResult(implies=None, method=ImplicationMethod.ALGEBRAIC)

        # Compare operators and values
        try:
            op1, op2 = p1.operator, p2.operator
            v1, v2 = float(val1), float(val2)

            # Greater-than family
            if op1 in (">", ">=") and op2 in (">", ">="):
                if op1 == ">" and op2 == ">":
                    implies = v1 >= v2
                elif op1 == ">=" and op2 == ">=":
                    implies = v1 >= v2
                elif op1 == ">" and op2 == ">=":
                    implies = v1 >= v2
                elif op1 == ">=" and op2 == ">":
                    implies = v1 > v2
                else:
                    implies = False

                if implies:
                    return ImplicationResult(
                        implies=True,
                        method=ImplicationMethod.ALGEBRAIC,
                        explanation=f"Range containment: {op1} {v1} implies {op2} {v2}",
                    )

            # Less-than family
            if op1 in ("<", "<=") and op2 in ("<", "<="):
                if op1 == "<" and op2 == "<":
                    implies = v1 <= v2
                elif op1 == "<=" and op2 == "<=":
                    implies = v1 <= v2
                elif op1 == "<" and op2 == "<=":
                    implies = v1 <= v2
                elif op1 == "<=" and op2 == "<":
                    implies = v1 < v2
                else:
                    implies = False

                if implies:
                    return ImplicationResult(
                        implies=True,
                        method=ImplicationMethod.ALGEBRAIC,
                        explanation=f"Range containment: {op1} {v1} implies {op2} {v2}",
                    )

            # Equality implies inequality
            if op1 == "=" and op2 in (">", ">=", "<", "<=", "!="):
                if op2 == ">" and v1 > v2:
                    return ImplicationResult(
                        implies=True,
                        method=ImplicationMethod.ALGEBRAIC,
                        explanation=f"= {v1} implies > {v2}",
                    )
                if op2 == ">=" and v1 >= v2:
                    return ImplicationResult(
                        implies=True,
                        method=ImplicationMethod.ALGEBRAIC,
                        explanation=f"= {v1} implies >= {v2}",
                    )
                if op2 == "<" and v1 < v2:
                    return ImplicationResult(
                        implies=True,
                        method=ImplicationMethod.ALGEBRAIC,
                        explanation=f"= {v1} implies < {v2}",
                    )
                if op2 == "<=" and v1 <= v2:
                    return ImplicationResult(
                        implies=True,
                        method=ImplicationMethod.ALGEBRAIC,
                        explanation=f"= {v1} implies <= {v2}",
                    )
                if op2 == "!=" and v1 != v2:
                    return ImplicationResult(
                        implies=True,
                        method=ImplicationMethod.ALGEBRAIC,
                        explanation=f"= {v1} implies != {v2}",
                    )

        except (ValueError, TypeError) as e:
            logger.debug("Range comparison failed (likely type mismatch): %s", e)

        return ImplicationResult(implies=None, method=ImplicationMethod.ALGEBRAIC)

    def _check_in_subset(self, p1: BoundIn, p2: BoundIn) -> ImplicationResult:
        """Check if IN predicate p1 implies p2 (subset relationship)."""
        # Must be on same column
        cols1 = self._get_columns(p1.expression)
        cols2 = self._get_columns(p2.expression)

        if cols1 != cols2 or not cols1:
            return ImplicationResult(implies=None, method=ImplicationMethod.ALGEBRAIC)

        # Get value sets
        vals1 = {self._get_literal_value(v) for v in p1.values}
        vals2 = {self._get_literal_value(v) for v in p2.values}

        # Remove None values
        vals1 = {v for v in vals1 if v is not None}
        vals2 = {v for v in vals2 if v is not None}

        if not vals1 or not vals2:
            return ImplicationResult(implies=None, method=ImplicationMethod.ALGEBRAIC)

        # x IN (1,2) implies x IN (1,2,3) if {1,2} ⊆ {1,2,3}
        if vals1 <= vals2:
            return ImplicationResult(
                implies=True,
                method=ImplicationMethod.ALGEBRAIC,
                explanation=f"IN subset: {vals1} ⊆ {vals2}",
            )

        return ImplicationResult(implies=None, method=ImplicationMethod.ALGEBRAIC)

    def _check_between_implies_comparison(
        self, p1: BoundBetween, p2: BoundComparison
    ) -> ImplicationResult:
        """Check if BETWEEN implies a comparison."""
        cols1 = self._get_columns(p1.expression)
        cols2 = self._get_columns(p2.left)

        if cols1 != cols2 or not cols1:
            return ImplicationResult(implies=None, method=ImplicationMethod.ALGEBRAIC)

        low = self._get_literal_value(p1.low)
        high = self._get_literal_value(p1.high)
        val = self._get_literal_value(p2.right)

        if low is None or high is None or val is None:
            return ImplicationResult(implies=None, method=ImplicationMethod.ALGEBRAIC)

        try:
            low_f, high_f, val_f = float(low), float(high), float(val)
            op = p2.operator

            if op == ">=" and low_f >= val_f:
                return ImplicationResult(
                    implies=True,
                    method=ImplicationMethod.ALGEBRAIC,
                    explanation=f"BETWEEN {low}..{high} implies >= {val}",
                )

            if op == "<=" and high_f <= val_f:
                return ImplicationResult(
                    implies=True,
                    method=ImplicationMethod.ALGEBRAIC,
                    explanation=f"BETWEEN {low}..{high} implies <= {val}",
                )

            if op == ">" and low_f > val_f:
                return ImplicationResult(
                    implies=True,
                    method=ImplicationMethod.ALGEBRAIC,
                    explanation=f"BETWEEN {low}..{high} implies > {val}",
                )

            if op == "<" and high_f < val_f:
                return ImplicationResult(
                    implies=True,
                    method=ImplicationMethod.ALGEBRAIC,
                    explanation=f"BETWEEN {low}..{high} implies < {val}",
                )

        except (ValueError, TypeError) as e:
            logger.debug("BETWEEN comparison failed (likely type mismatch): %s", e)

        return ImplicationResult(implies=None, method=ImplicationMethod.ALGEBRAIC)

    def _check_between_containment(self, p1: Any, p2: Any) -> ImplicationResult:
        """Check if BETWEEN predicate p1 implies p2 (containment).

        (x BETWEEN 5 AND 8) implies (x BETWEEN 3 AND 10) if [5,8] ⊆ [3,10].
        """
        cols1 = self._get_columns(p1.expression)
        cols2 = self._get_columns(p2.expression)

        if cols1 != cols2 or not cols1:
            return ImplicationResult(implies=None, method=ImplicationMethod.ALGEBRAIC)

        low1 = self._get_literal_value(p1.low)
        high1 = self._get_literal_value(p1.high)
        low2 = self._get_literal_value(p2.low)
        high2 = self._get_literal_value(p2.high)

        if low1 is None or high1 is None or low2 is None or high2 is None:
            return ImplicationResult(implies=None, method=ImplicationMethod.ALGEBRAIC)

        try:
            low1_f, high1_f = float(low1), float(high1)
            low2_f, high2_f = float(low2), float(high2)

            # [low1, high1] ⊆ [low2, high2] if low2 <= low1 and high1 <= high2
            if low2_f <= low1_f and high1_f <= high2_f:
                return ImplicationResult(
                    implies=True,
                    method=ImplicationMethod.ALGEBRAIC,
                    explanation=f"BETWEEN containment: [{low1}..{high1}] ⊆ [{low2}..{high2}]",
                )

        except (ValueError, TypeError) as e:
            logger.debug("BETWEEN containment check failed (likely type mismatch): %s", e)

        return ImplicationResult(implies=None, method=ImplicationMethod.ALGEBRAIC)

    def _get_literal_value(self, e: Any) -> Any:
        """Extract literal value from expression.

        Uses duck typing to support both algebrakit and query_analyzer types.
        """
        # Duck type: literal (has value attribute)
        if hasattr(e, "value"):
            return e.value
        return None

    # =========================================================================
    # Layer 3: Linear Arithmetic (Fourier-Motzkin)
    # =========================================================================

    def _check_linear_arithmetic(
        self,
        p1: BoundPredicate,
        p2: BoundPredicate,
    ) -> ImplicationResult:
        """Use Fourier-Motzkin elimination for linear arithmetic predicates.

        This is a complete decision procedure for linear predicates:
        - For linear predicates, returns definitive implies=True or implies=False
        - For non-linear predicates, falls through (implies=None)

        Fourier-Motzkin elimination:
        1. Convert p1 and ¬p2 to linear inequalities
        2. Iteratively eliminate variables
        3. Check for contradictions (0 ≤ negative)

        Supported: x OP c, x OP y, a*x + b*y OP c where OP ∈ {<, ≤, =, ≥, >}
        Not supported: !=, LIKE, IS NULL, OR, multiplication of variables
        """
        from alma_algebrakit.proof.linear_arithmetic import (
            LinearCheckResult,
            check_linear_implication,
        )

        result = check_linear_implication(p1, p2)

        if result.result == LinearCheckResult.IMPLIES:
            return ImplicationResult(
                implies=True,
                method=ImplicationMethod.LINEAR_ARITHMETIC,
                explanation=result.explanation,
                confidence=1.0,  # Complete decision procedure
            )

        elif result.result == LinearCheckResult.NOT_IMPLIES:
            return ImplicationResult(
                implies=False,
                method=ImplicationMethod.LINEAR_ARITHMETIC,
                explanation=result.explanation,
                confidence=1.0,  # Complete decision procedure
            )

        # NOT_LINEAR or ERROR - fall through to SMT
        return ImplicationResult(
            implies=None,
            method=ImplicationMethod.LINEAR_ARITHMETIC,
            explanation=result.explanation,
        )

    # =========================================================================
    # Layer 4: SMT solver (Z3)
    # =========================================================================

    def _check_smt(
        self,
        p1: BoundPredicate,
        p2: BoundPredicate,
        type_env: dict[str, DataType] | None,
    ) -> ImplicationResult:
        """Use Z3 SMT solver for complex implication.

        Uses proper Z3 sorts based on type_env:
        - String columns use StringSort() with Z3's string theory
        - Integer columns use IntSort()
        - Floating-point columns use RealSort()
        - Boolean columns use BoolSort()
        """
        import time

        try:
            import z3
        except ImportError:
            return ImplicationResult(
                implies=None,
                method=ImplicationMethod.SMT,
                explanation="Z3 not available",
            )

        start = time.time()

        try:
            # Create Z3 context with string solver
            solver = z3.Solver()
            solver.set("timeout", self.smt_timeout_ms)

            # Collect all columns
            all_cols = set()
            for col in p1.upstream_columns + p2.upstream_columns:
                all_cols.add(col.qualified_id())

            # Create Z3 variables for each column with proper types
            col_vars: dict[str, tuple[Any, Any, Any]] = {}  # (val_var, null_var, sort)

            for col_id in all_cols:
                sort = _get_z3_sort(col_id, type_env, z3)
                val_var = z3.Const(f"val_{col_id}", sort)
                null_var = z3.Bool(f"null_{col_id}")
                col_vars[col_id] = (val_var, null_var, sort)

            # Encode "passes(p1)" and "passes(p2)"
            passes_p1 = self._encode_passes(p1, col_vars, z3, type_env)
            passes_p2 = self._encode_passes(p2, col_vars, z3, type_env)

            if passes_p1 is None or passes_p2 is None:
                return ImplicationResult(
                    implies=None,
                    method=ImplicationMethod.SMT,
                    explanation="Could not encode predicates for SMT",
                )

            # Check: passes(p1) ∧ ¬passes(p2) is unsatisfiable
            solver.add(passes_p1)
            solver.add(z3.Not(passes_p2))

            result = solver.check()
            elapsed_ms = (time.time() - start) * 1000

            if result == z3.unsat:
                return ImplicationResult(
                    implies=True,
                    method=ImplicationMethod.SMT,
                    explanation="SMT proved: passes(p1) ∧ ¬passes(p2) is unsat",
                    solver_time_ms=elapsed_ms,
                )
            elif result == z3.sat:
                model = solver.model()
                return ImplicationResult(
                    implies=False,
                    method=ImplicationMethod.SMT,
                    explanation=f"SMT found counterexample: {model}",
                    solver_time_ms=elapsed_ms,
                )
            else:
                return ImplicationResult(
                    implies=None,
                    method=ImplicationMethod.SMT,
                    explanation="SMT returned unknown (timeout or resource limit)",
                    solver_time_ms=elapsed_ms,
                )

        except Exception as e:
            elapsed_ms = (time.time() - start) * 1000
            return ImplicationResult(
                implies=None,
                method=ImplicationMethod.SMT,
                explanation=f"SMT error: {e}",
                solver_time_ms=elapsed_ms,
            )

    def _encode_passes(
        self,
        p: BoundPredicate,
        col_vars: dict[str, tuple[Any, Any, Any]],
        z3: Any,
        type_env: dict[str, DataType] | None = None,
    ) -> Any:
        """Encode passes(p) - predicate evaluates TRUE under SQL WHERE semantics.

        Uses duck typing to support both algebrakit and query_analyzer types.

        Args:
            p: The predicate to encode
            col_vars: Dict mapping column IDs to (val_var, null_var, sort) tuples
            z3: The z3 module
            type_env: Type environment for type-aware encoding
        """
        # Duck type: LIKE predicate (has expression, pattern)
        if hasattr(p, "expression") and hasattr(p, "pattern") and not hasattr(p, "values"):
            return self._encode_like_passes(p, col_vars, z3, type_env)

        # Duck type: comparison predicate (has left, operator, right)
        if hasattr(p, "left") and hasattr(p, "operator") and hasattr(p, "right"):
            return self._encode_comparison_passes(p, col_vars, z3, type_env)

        # Duck type: IS NULL predicate (has expression, negated, but not values/low/pattern)
        if (
            hasattr(p, "expression")
            and hasattr(p, "negated")
            and not hasattr(p, "values")
            and not hasattr(p, "low")
            and not hasattr(p, "pattern")
        ):
            return self._encode_is_null_passes(p, col_vars, z3)

        # Duck type: IN predicate (has expression, values)
        if hasattr(p, "expression") and hasattr(p, "values"):
            return self._encode_in_passes(p, col_vars, z3, type_env)

        # Duck type: BETWEEN predicate (has expression, low, high)
        if hasattr(p, "expression") and hasattr(p, "low") and hasattr(p, "high"):
            return self._encode_between_passes(p, col_vars, z3, type_env)

        # Duck type: logical predicate (has operator, operands)
        if hasattr(p, "operator") and hasattr(p, "operands"):
            return self._encode_logical_passes(p, col_vars, z3, type_env)

        return None

    def _encode_comparison_passes(
        self,
        p: BoundComparison,
        col_vars: dict[str, tuple[Any, Any, Any]],
        z3: Any,
        type_env: dict[str, DataType] | None = None,
    ) -> Any:
        """Encode passes(col op lit) for comparison.

        Uses proper Z3 sorts based on type_env with type coercion:
        - String columns use StringSort with z3.StringVal
        - Integer columns use IntSort with integer values
        - Floating-point columns use RealSort with float values
        - Mixed INTEGER/DECIMAL comparisons are promoted to RealSort
        """
        left_cols = self._get_columns(p.left)
        right_val = self._get_literal_value(p.right)

        if not left_cols or right_val is None:
            return None

        col_id = next(iter(left_cols))
        if col_id not in col_vars:
            return None

        val_var, null_var, sort = col_vars[col_id]
        op = p.operator

        # Use type coercion helper for compatible sorts
        try:
            coerced_sort, coerced_var, lit_val = _get_compatible_z3_sort_and_vars(
                sort, None, val_var, right_val, z3
            )
        except (ValueError, TypeError):
            # Fallback for unparseable values
            return z3.Not(null_var)

        # Check if this is a string type based on coerced sort
        is_string = coerced_sort == z3.StringSort()

        if is_string:
            # String comparison using Z3 string theory
            str_lit = (
                z3.StringVal(str(right_val))
                if not isinstance(lit_val, type(z3.StringVal("")))
                else lit_val
            )

            if op == "=":
                cmp = coerced_var == str_lit
            elif op == "!=":
                cmp = coerced_var != str_lit
            elif op == "<":
                # Z3 supports lexicographic comparison with str.<
                cmp = z3.StrLT(coerced_var, str_lit)
            elif op == "<=":
                cmp = z3.StrLE(coerced_var, str_lit)
            elif op == ">":
                cmp = z3.StrLT(str_lit, coerced_var)
            elif op == ">=":
                cmp = z3.StrLE(str_lit, coerced_var)
            else:
                # Unknown operator
                return z3.Not(null_var)

            return z3.And(z3.Not(null_var), cmp)

        # Numeric comparison (coerced_var and lit_val are now type-compatible)
        if op == "=":
            cmp = coerced_var == lit_val
        elif op == "!=":
            cmp = coerced_var != lit_val
        elif op == ">":
            cmp = coerced_var > lit_val
        elif op == ">=":
            cmp = coerced_var >= lit_val
        elif op == "<":
            cmp = coerced_var < lit_val
        elif op == "<=":
            cmp = coerced_var <= lit_val
        else:
            return None

        return z3.And(z3.Not(null_var), cmp)

    def _encode_is_null_passes(
        self, p: BoundIsNull, col_vars: dict[str, tuple[Any, Any, Any]], z3: Any
    ) -> Any:
        """Encode passes(IS NULL) or passes(IS NOT NULL)."""
        cols = self._get_columns(p.expression)
        if not cols:
            return None

        col_id = next(iter(cols))
        if col_id not in col_vars:
            return None

        _, null_var, _ = col_vars[col_id]

        if p.negated:
            return z3.Not(null_var)
        else:
            return null_var

    def _encode_in_passes(
        self,
        p: BoundIn,
        col_vars: dict[str, tuple[Any, Any, Any]],
        z3: Any,
        type_env: dict[str, DataType] | None = None,
    ) -> Any:
        """Encode passes(col IN (v1, v2, ...)).

        Properly handles both string and numeric IN predicates based on type_env.
        """
        cols = self._get_columns(p.expression)
        if not cols:
            return None

        col_id = next(iter(cols))
        if col_id not in col_vars:
            return None

        val_var, null_var, sort = col_vars[col_id]

        # Check if this is a string type
        is_string = sort == z3.StringSort()
        is_int = sort == z3.IntSort()

        values = []
        for v in p.values:
            lit_val = self._get_literal_value(v)
            if lit_val is not None:
                if is_string:
                    # String IN - use StringVal
                    values.append(z3.StringVal(str(lit_val)))
                elif is_int:
                    # Integer IN
                    try:
                        values.append(int(lit_val))
                    except (ValueError, TypeError):
                        pass
                else:
                    # Numeric IN (Real)
                    try:
                        values.append(float(lit_val))
                    except (ValueError, TypeError):
                        # Try as string if numeric conversion fails
                        pass

        if not values:
            return z3.Not(null_var)

        # Build IN clause as OR of equalities
        in_clause = z3.Or([val_var == v for v in values])

        # Handle NOT IN
        if getattr(p, "negated", False):
            return z3.And(z3.Not(null_var), z3.Not(in_clause))

        return z3.And(z3.Not(null_var), in_clause)

    def _encode_between_passes(
        self,
        p: BoundBetween,
        col_vars: dict[str, tuple[Any, Any, Any]],
        z3: Any,
        type_env: dict[str, DataType] | None = None,
    ) -> Any:
        """Encode passes(col BETWEEN low AND high).

        Handles both numeric and string BETWEEN based on type_env.
        """
        cols = self._get_columns(p.expression)
        if not cols:
            return None

        col_id = next(iter(cols))
        if col_id not in col_vars:
            return None

        val_var, null_var, sort = col_vars[col_id]

        low_val = self._get_literal_value(p.low)
        high_val = self._get_literal_value(p.high)

        if low_val is None or high_val is None:
            return z3.Not(null_var)

        # Check if this is a string type
        is_string = sort == z3.StringSort()
        is_int = sort == z3.IntSort()

        if is_string:
            # String BETWEEN uses lexicographic comparison
            low_str = z3.StringVal(str(low_val))
            high_str = z3.StringVal(str(high_val))

            between_cond = z3.And(
                z3.StrLE(low_str, val_var),  # low <= val
                z3.StrLE(val_var, high_str),  # val <= high
            )

            if getattr(p, "negated", False):
                return z3.And(z3.Not(null_var), z3.Not(between_cond))
            return z3.And(z3.Not(null_var), between_cond)

        # Numeric BETWEEN
        try:
            if is_int:
                low_f, high_f = int(low_val), int(high_val)
            else:
                low_f, high_f = float(low_val), float(high_val)
        except (ValueError, TypeError):
            return z3.Not(null_var)

        between_cond = z3.And(val_var >= low_f, val_var <= high_f)

        if getattr(p, "negated", False):
            return z3.And(z3.Not(null_var), z3.Not(between_cond))

        return z3.And(z3.Not(null_var), between_cond)

    def _encode_like_passes(
        self,
        p: BoundLike,
        col_vars: dict[str, tuple[Any, Any, Any]],
        z3: Any,
        type_env: dict[str, DataType] | None = None,
    ) -> Any:
        """Encode passes(col LIKE pattern) using Z3 regex.

        SQL LIKE wildcards are converted to regex:
        - % matches any sequence (including empty) -> .*
        - _ matches any single character -> .
        - Escape sequences are handled

        Uses Z3's InRe (membership in regular expression) for matching.
        """
        cols = self._get_columns(p.expression)
        if not cols:
            return None

        col_id = next(iter(cols))
        if col_id not in col_vars:
            return None

        val_var, null_var, sort = col_vars[col_id]

        # LIKE requires string type - if column isn't string, we need a string variable
        if sort != z3.StringSort():
            # Create ad-hoc string variable
            val_var = z3.String(f"str_{col_id}")

        # Get pattern value
        pattern_val = self._get_literal_value(p.pattern)
        if pattern_val is None:
            # Pattern is not a literal - can't encode
            return z3.Not(null_var)

        pattern_str = str(pattern_val)

        # Build Z3 regex using proper Z3 regex AST construction
        try:
            # Use _like_pattern_to_z3_regex which builds proper Z3 regex AST
            regex_expr = _like_pattern_to_z3_regex(pattern_str, z3)
            like_cond = z3.InRe(val_var, regex_expr)
        except Exception:
            # Fallback for complex patterns
            return z3.Not(null_var)

        # Handle NOT LIKE and case sensitivity
        if getattr(p, "negated", False):
            return z3.And(z3.Not(null_var), z3.Not(like_cond))

        return z3.And(z3.Not(null_var), like_cond)

    def _sql_like_to_regex(self, pattern: str) -> str:
        """Convert SQL LIKE pattern to Python regex pattern.

        SQL LIKE:
        - % matches zero or more characters
        - _ matches exactly one character
        - \\ escapes the next character
        """
        result = []
        i = 0
        while i < len(pattern):
            char = pattern[i]
            if char == "%":
                result.append(".*")
            elif char == "_":
                result.append(".")
            elif char == "\\" and i + 1 < len(pattern):
                # Escaped character
                next_char = pattern[i + 1]
                result.append(re.escape(next_char))
                i += 1
            else:
                # Regular character - escape regex special chars
                result.append(re.escape(char))
            i += 1

        # Anchor the pattern (SQL LIKE is implicitly anchored)
        return "^" + "".join(result) + "$"

    def _encode_logical_passes(
        self,
        p: BoundLogical,
        col_vars: dict[str, tuple[Any, Any, Any]],
        z3: Any,
        type_env: dict[str, DataType] | None = None,
    ) -> Any:
        """Encode passes(p AND q), passes(p OR q), passes(NOT p).

        SQL 3-valued logic semantics:
        - passes(P) means P evaluates to TRUE (not FALSE, not UNKNOWN)
        - passes(P AND Q) = passes(P) AND passes(Q)
        - passes(P OR Q) = passes(P) OR passes(Q)
        - passes(NOT P) = evaluates_to_false(P) (P is FALSE, not TRUE, not UNKNOWN)

        For OR under SQL 3VL:
        - TRUE OR x = TRUE (for any x including UNKNOWN)
        - FALSE OR TRUE = TRUE
        - FALSE OR FALSE = FALSE
        - FALSE OR UNKNOWN = UNKNOWN
        - UNKNOWN OR UNKNOWN = UNKNOWN

        So passes(P OR Q) = TRUE iff at least one of P,Q is TRUE.
        The simple encoding z3.Or(passes(P), passes(Q)) is correct because:
        - If passes(P) = TRUE, then P=TRUE, so P OR Q = TRUE
        - If passes(Q) = TRUE, then Q=TRUE, so P OR Q = TRUE
        - If both are FALSE in Z3 (meaning P and Q are FALSE or UNKNOWN in SQL),
          then P OR Q is FALSE or UNKNOWN, neither of which passes WHERE.

        For NOT, we need to encode "P is FALSE" which is different from "NOT passes(P)".
        passes(P) = P is TRUE
        NOT passes(P) = P is FALSE OR P is UNKNOWN
        evaluates_to_false(P) = P is FALSE (what we need for passes(NOT P))

        For comparisons involving non-NULL values:
        - evaluates_to_false(col op val) = NOT null_col AND NOT (col op val)
        """
        if p.operator == "AND":
            encoded_operands = []
            for operand in p.operands:
                encoded = self._encode_passes(operand, col_vars, z3, type_env)
                if encoded is None:
                    return None
                encoded_operands.append(encoded)
            return z3.And(encoded_operands)

        elif p.operator == "OR":
            encoded_operands = []
            for operand in p.operands:
                encoded = self._encode_passes(operand, col_vars, z3, type_env)
                if encoded is None:
                    return None
                encoded_operands.append(encoded)
            # passes(P OR Q) = passes(P) OR passes(Q)
            # This correctly handles SQL 3VL: TRUE iff at least one operand is TRUE
            # NULL tracking is already embedded in each passes() encoding
            return z3.Or(encoded_operands)

        elif p.operator == "NOT":
            if len(p.operands) == 1:
                inner = p.operands[0]
                # passes(NOT P) = P evaluates to FALSE
                # We need to encode "P is FALSE" under SQL semantics
                return self._encode_evaluates_to_false(inner, col_vars, z3, type_env)

        return None

    def _encode_evaluates_to_false(
        self,
        p: BoundPredicate,
        col_vars: dict[str, tuple[Any, Any, Any]],
        z3: Any,
        type_env: dict[str, DataType] | None = None,
    ) -> Any:
        """Encode that predicate P evaluates to FALSE (not TRUE, not UNKNOWN).

        Under SQL 3-valued logic:
        - FALSE means the predicate is definitively false
        - This requires no NULL values involved AND the condition being false

        Uses duck typing to support both algebrakit and query_analyzer types.
        """
        # Duck type: comparison predicate (has left, operator, right)
        if hasattr(p, "left") and hasattr(p, "operator") and hasattr(p, "right"):
            # P is FALSE means: not null AND comparison is false
            return self._encode_comparison_false(p, col_vars, z3, type_env)

        # Duck type: IS NULL predicate (has expression, negated, but not values/low/pattern)
        if (
            hasattr(p, "expression")
            and hasattr(p, "negated")
            and not hasattr(p, "values")
            and not hasattr(p, "low")
            and not hasattr(p, "pattern")
        ):
            # IS NULL is FALSE when column is not null
            # IS NOT NULL is FALSE when column is null
            cols = self._get_columns(p.expression)
            if not cols:
                return None
            col_id = next(iter(cols))
            if col_id not in col_vars:
                return None
            _, null_var, _ = col_vars[col_id]

            if p.negated:
                # IS NOT NULL is FALSE means column is NULL
                return null_var
            else:
                # IS NULL is FALSE means column is NOT NULL
                return z3.Not(null_var)

        # Duck type: logical predicate (has operator, operands)
        if hasattr(p, "operator") and hasattr(p, "operands"):
            if p.operator == "AND":
                # (P AND Q) is FALSE means P is FALSE OR Q is FALSE (and no nulls)
                false_parts = []
                for operand in p.operands:
                    encoded = self._encode_evaluates_to_false(operand, col_vars, z3, type_env)
                    if encoded is not None:
                        false_parts.append(encoded)
                if false_parts:
                    return z3.Or(false_parts)
            elif p.operator == "OR":
                # (P OR Q) is FALSE means P is FALSE AND Q is FALSE
                false_parts = []
                for operand in p.operands:
                    encoded = self._encode_evaluates_to_false(operand, col_vars, z3, type_env)
                    if encoded is None:
                        return None
                    false_parts.append(encoded)
                return z3.And(false_parts)
            elif p.operator == "NOT":
                # NOT P is FALSE means P is TRUE
                if len(p.operands) == 1:
                    return self._encode_passes(p.operands[0], col_vars, z3, type_env)

        # Duck type: IN predicate (has expression, values)
        if hasattr(p, "expression") and hasattr(p, "values"):
            # IN is FALSE when: not null AND not in any value
            cols = self._get_columns(p.expression)
            if not cols:
                return None
            col_id = next(iter(cols))
            if col_id not in col_vars:
                return None
            val_var, null_var, sort = col_vars[col_id]

            # Check if this is a string type
            is_string = sort == z3.StringSort()
            is_int = sort == z3.IntSort()

            values = []
            for v in p.values:
                lit_val = self._get_literal_value(v)
                if lit_val is not None:
                    if is_string:
                        values.append(z3.StringVal(str(lit_val)))
                    elif is_int:
                        try:
                            values.append(int(lit_val))
                        except (ValueError, TypeError):
                            pass
                    else:
                        try:
                            values.append(float(lit_val))
                        except (ValueError, TypeError):
                            pass

            if not values:
                return z3.Not(null_var)

            # FALSE means: not null AND not equal to any value
            not_in = z3.And([val_var != v for v in values])
            return z3.And(z3.Not(null_var), not_in)

        # Duck type: BETWEEN predicate (has expression, low, high)
        if hasattr(p, "expression") and hasattr(p, "low") and hasattr(p, "high"):
            # BETWEEN is FALSE when: not null AND (< low OR > high)
            cols = self._get_columns(p.expression)
            if not cols:
                return None
            col_id = next(iter(cols))
            if col_id not in col_vars:
                return None
            val_var, null_var, sort = col_vars[col_id]

            low_val = self._get_literal_value(p.low)
            high_val = self._get_literal_value(p.high)

            if low_val is None or high_val is None:
                return z3.Not(null_var)

            # Check if this is a string type
            is_string = sort == z3.StringSort()
            is_int = sort == z3.IntSort()

            if is_string:
                low_str = z3.StringVal(str(low_val))
                high_str = z3.StringVal(str(high_val))
                # BETWEEN is FALSE when: < low OR > high (lexicographic)
                return z3.And(
                    z3.Not(null_var),
                    z3.Or(z3.StrLT(val_var, low_str), z3.StrLT(high_str, val_var)),
                )

            try:
                if is_int:
                    low_f, high_f = int(low_val), int(high_val)
                else:
                    low_f, high_f = float(low_val), float(high_val)
            except (ValueError, TypeError):
                return z3.Not(null_var)

            return z3.And(
                z3.Not(null_var),
                z3.Or(val_var < low_f, val_var > high_f),
            )

        # Duck type: LIKE predicate (has expression, pattern)
        if hasattr(p, "expression") and hasattr(p, "pattern"):
            # LIKE is FALSE when: not null AND not matching pattern
            # We encode this as NOT(passes(LIKE))
            like_passes = self._encode_like_passes(p, col_vars, z3, type_env)
            if like_passes is None:
                cols = self._get_columns(p.expression)
                if cols:
                    col_id = next(iter(cols))
                    if col_id in col_vars:
                        _, null_var, _ = col_vars[col_id]
                        return z3.Not(null_var)
                return None

            # Get null_var to ensure not null
            cols = self._get_columns(p.expression)
            if cols:
                col_id = next(iter(cols))
                if col_id in col_vars:
                    _, null_var, _ = col_vars[col_id]
                    # FALSE means passes returns FALSE (not null and not matching)
                    return z3.And(z3.Not(null_var), z3.Not(like_passes))
            return None

        return None

    def _encode_comparison_false(
        self,
        p: BoundComparison,
        col_vars: dict[str, tuple[Any, Any, Any]],
        z3: Any,
        type_env: dict[str, DataType] | None = None,
    ) -> Any:
        """Encode that comparison is definitively FALSE.

        For FALSE under SQL 3VL:
        - The value must not be NULL (otherwise result is UNKNOWN)
        - The comparison must evaluate to FALSE

        Uses proper Z3 sorts based on type_env for type-aware encoding.
        """
        left_cols = self._get_columns(p.left)
        right_val = self._get_literal_value(p.right)

        if not left_cols or right_val is None:
            return None

        col_id = next(iter(left_cols))
        if col_id not in col_vars:
            return None

        val_var, null_var, sort = col_vars[col_id]
        op = p.operator

        # Check if this is a string type based on sort
        is_string = sort == z3.StringSort()
        is_int = sort == z3.IntSort()

        if is_string:
            # String comparison - negate using Z3 string theory
            str_lit = z3.StringVal(str(right_val))

            if op == "=":
                cmp_false = val_var != str_lit
            elif op == "!=":
                cmp_false = val_var == str_lit
            elif op == "<":
                # NOT(a < b) = a >= b
                cmp_false = z3.StrLE(str_lit, val_var)
            elif op == "<=":
                # NOT(a <= b) = a > b = b < a
                cmp_false = z3.StrLT(str_lit, val_var)
            elif op == ">":
                # NOT(a > b) = a <= b
                cmp_false = z3.StrLE(val_var, str_lit)
            elif op == ">=":
                # NOT(a >= b) = a < b
                cmp_false = z3.StrLT(val_var, str_lit)
            else:
                return z3.Not(null_var)

            return z3.And(z3.Not(null_var), cmp_false)

        # Numeric comparison
        try:
            if is_int:
                lit_val = int(right_val)
            else:
                lit_val = float(right_val)

            # Negate the comparison for FALSE
            if op == "=":
                cmp_false = val_var != lit_val
            elif op == "!=":
                cmp_false = val_var == lit_val
            elif op == ">":
                cmp_false = val_var <= lit_val
            elif op == ">=":
                cmp_false = val_var < lit_val
            elif op == "<":
                cmp_false = val_var >= lit_val
            elif op == "<=":
                cmp_false = val_var > lit_val
            else:
                return None

            return z3.And(z3.Not(null_var), cmp_false)

        except (ValueError, TypeError):
            # Fallback to string comparison
            str_lit = str(right_val)
            str_var = z3.String(f"str_{col_id}")

            if op == "=":
                cmp_false = str_var != z3.StringVal(str_lit)
                return z3.And(z3.Not(null_var), cmp_false)
            elif op == "!=":
                cmp_false = str_var == z3.StringVal(str_lit)
                return z3.And(z3.Not(null_var), cmp_false)
            else:
                return z3.Not(null_var)
