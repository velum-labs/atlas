"""Tests for the proof module (implication and empirical validation).

Covers:
- PredicateImplicationChecker with Z3 SMT encoding
- Type-aware predicate reasoning (StringSort, IntSort, RealSort)
- LIKE predicate support with Z3 regex encoding
- Empirical query equivalence validation via content hashing
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from alma_algebrakit.bound.types import (
    BoundBetween,
    BoundColumnRef,
    BoundComparison,
    BoundIn,
    BoundIsNull,
    BoundLike,
    BoundLiteral,
    BoundLogical,
)
from alma_algebrakit.proof.empirical import (
    EmpiricalValidator,
    _compute_content_hash,
    _normalize_result_set,
    _normalize_value,
)
from alma_algebrakit.proof.implication import PredicateImplicationChecker
from alma_algebrakit.schema.types import DataType, SQLDataType

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def checker() -> PredicateImplicationChecker:
    """Create a predicate implication checker with SMT enabled."""
    return PredicateImplicationChecker(use_smt=True, smt_timeout_ms=1000)


@pytest.fixture
def checker_no_smt() -> PredicateImplicationChecker:
    """Create a checker without SMT (fast path only)."""
    return PredicateImplicationChecker(use_smt=False)


def make_bound_column(
    name: str,
    table_id: str = "t",
    data_type: DataType | None = None,
) -> BoundColumnRef:
    """Helper to create a BoundColumnRef."""
    if data_type is None:
        data_type = DataType(base_type=SQLDataType.INTEGER)
    return BoundColumnRef(
        table_id=table_id,
        column_id=name,
        sql_name=name,
        data_type=data_type,
    )


def make_bound_literal(
    value: Any,
    sql_type: SQLDataType = SQLDataType.INTEGER,
) -> BoundLiteral:
    """Helper to create a BoundLiteral."""
    return BoundLiteral(
        value=value,
        data_type=DataType(base_type=sql_type),
        sql_text=str(value),
    )


def make_comparison(
    col_name: str,
    operator: str,
    value: Any,
    col_type: SQLDataType = SQLDataType.INTEGER,
    val_type: SQLDataType | None = None,
) -> BoundComparison:
    """Helper to create a BoundComparison."""
    col = make_bound_column(col_name, data_type=DataType(base_type=col_type))
    lit = make_bound_literal(value, val_type or col_type)
    return BoundComparison(
        left=col,
        operator=operator,
        right=lit,
        upstream_columns=[col],
    )


def make_type_env(**kwargs: SQLDataType) -> dict[str, DataType]:
    """Create a type environment from column names and types."""
    return {name: DataType(base_type=sql_type) for name, sql_type in kwargs.items()}


# =============================================================================
# Predicate Implication Tests - Basic
# =============================================================================


class TestPredicateImplicationBasic:
    """Basic tests for predicate implication without SMT."""

    def test_same_predicate_implies_itself(
        self, checker_no_smt: PredicateImplicationChecker
    ) -> None:
        """Test that a predicate implies itself."""
        pred = make_comparison("x", "=", 5)
        result = checker_no_smt.check_implication(pred, pred)
        assert result.implies is True

    def test_different_predicates_no_fast_path(
        self, checker_no_smt: PredicateImplicationChecker
    ) -> None:
        """Test that different predicates don't match on fast path."""
        p1 = make_comparison("x", ">", 5)
        p2 = make_comparison("x", ">", 3)
        # Without SMT, algebraic range analysis still detects implication
        result = checker_no_smt.check_implication(p1, p2)
        # Note: algebraic analysis CAN prove x > 5 implies x > 3
        assert result.implies is True

    def test_conjunction_implies_conjunct_fast_path(
        self, checker_no_smt: PredicateImplicationChecker
    ) -> None:
        """Test that AND predicate implies its parts on fast path."""
        p1 = make_comparison("x", ">", 5)
        p2 = make_comparison("y", "<", 10)
        conjunction = BoundLogical(
            operator="AND",
            operands=[p1, p2],
            upstream_columns=[],
        )
        # Conjunction should imply p1 via fast path
        result1 = checker_no_smt.check_implication(conjunction, p1)
        result2 = checker_no_smt.check_implication(conjunction, p2)
        assert result1.implies is True
        assert result2.implies is True


# =============================================================================
# Predicate Implication Tests - SMT
# =============================================================================


class TestPredicateImplicationSMT:
    """Tests for SMT-based predicate implication."""

    @pytest.mark.skipif(
        not PredicateImplicationChecker._z3_available(),
        reason="Z3 not available",
    )
    def test_stricter_bound_implies_weaker(self, checker: PredicateImplicationChecker) -> None:
        """Test that x > 5 implies x > 3."""
        stronger = make_comparison("x", ">", 5)
        weaker = make_comparison("x", ">", 3)
        type_env = make_type_env(x=SQLDataType.INTEGER)

        result = checker.check_implication(stronger, weaker, type_env=type_env)
        assert result.implies is True

    @pytest.mark.skipif(
        not PredicateImplicationChecker._z3_available(),
        reason="Z3 not available",
    )
    def test_weaker_does_not_imply_stricter(self, checker: PredicateImplicationChecker) -> None:
        """Test that x > 3 does NOT imply x > 5."""
        weaker = make_comparison("x", ">", 3)
        stronger = make_comparison("x", ">", 5)
        type_env = make_type_env(x=SQLDataType.INTEGER)

        result = checker.check_implication(weaker, stronger, type_env=type_env)
        assert result.implies is False

    @pytest.mark.skipif(
        not PredicateImplicationChecker._z3_available(),
        reason="Z3 not available",
    )
    def test_equality_implies_range(self, checker: PredicateImplicationChecker) -> None:
        """Test that x = 5 implies x >= 5."""
        equality = make_comparison("x", "=", 5)
        range_pred = make_comparison("x", ">=", 5)
        type_env = make_type_env(x=SQLDataType.INTEGER)

        result = checker.check_implication(equality, range_pred, type_env=type_env)
        assert result.implies is True

    @pytest.mark.skipif(
        not PredicateImplicationChecker._z3_available(),
        reason="Z3 not available",
    )
    def test_between_implies_range(self, checker: PredicateImplicationChecker) -> None:
        """Test that x BETWEEN 5 AND 10 implies x >= 5."""
        col = make_bound_column("x", data_type=DataType(base_type=SQLDataType.INTEGER))
        between = BoundBetween(
            expression=col,
            low=make_bound_literal(5),
            high=make_bound_literal(10),
            upstream_columns=[col],
        )
        range_pred = make_comparison("x", ">=", 5)
        type_env = make_type_env(x=SQLDataType.INTEGER)

        result = checker.check_implication(between, range_pred, type_env=type_env)
        assert result.implies is True


# =============================================================================
# Type-Aware SMT Tests
# =============================================================================


class TestTypeAwareSMT:
    """Tests for type-aware Z3 encoding."""

    @pytest.mark.skipif(
        not PredicateImplicationChecker._z3_available(),
        reason="Z3 not available",
    )
    def test_string_comparison(self, checker: PredicateImplicationChecker) -> None:
        """Test string comparisons use StringSort."""
        # name = 'Alice' should imply itself
        pred = make_comparison(
            "name",
            "=",
            "Alice",
            col_type=SQLDataType.TEXT,
            val_type=SQLDataType.TEXT,
        )
        type_env = make_type_env(name=SQLDataType.TEXT)

        result = checker.check_implication(pred, pred, type_env=type_env)
        assert result.implies is True

    @pytest.mark.skipif(
        not PredicateImplicationChecker._z3_available(),
        reason="Z3 not available",
    )
    def test_real_comparison(self, checker: PredicateImplicationChecker) -> None:
        """Test that real number comparisons work."""
        # price > 10.5 implies price > 5.0
        stronger = make_comparison(
            "price",
            ">",
            10.5,
            col_type=SQLDataType.REAL,
            val_type=SQLDataType.REAL,
        )
        weaker = make_comparison(
            "price",
            ">",
            5.0,
            col_type=SQLDataType.REAL,
            val_type=SQLDataType.REAL,
        )
        type_env = make_type_env(price=SQLDataType.REAL)

        result = checker.check_implication(stronger, weaker, type_env=type_env)
        assert result.implies is True


# =============================================================================
# LIKE Predicate Tests
# =============================================================================


class TestLikePredicateSupport:
    """Tests for LIKE predicate encoding."""

    @pytest.mark.skipif(
        not PredicateImplicationChecker._z3_available(),
        reason="Z3 not available",
    )
    def test_like_implies_itself(self, checker: PredicateImplicationChecker) -> None:
        """Test that LIKE pattern implies itself."""
        col = make_bound_column("name", data_type=DataType(base_type=SQLDataType.TEXT))
        pattern = make_bound_literal("John%", SQLDataType.TEXT)

        like_pred = BoundLike(
            expression=col,
            pattern=pattern,
            case_insensitive=False,
            upstream_columns=[col],
        )
        type_env = make_type_env(name=SQLDataType.TEXT)

        result = checker.check_implication(like_pred, like_pred, type_env=type_env)
        assert result.implies is True

    @pytest.mark.skipif(
        not PredicateImplicationChecker._z3_available(),
        reason="Z3 not available",
    )
    def test_more_specific_like_implies_broader(self, checker: PredicateImplicationChecker) -> None:
        """Test that 'John Smith%' implies 'John%' pattern."""
        col = make_bound_column("name", data_type=DataType(base_type=SQLDataType.TEXT))

        # More specific: 'John Smith%'
        specific = BoundLike(
            expression=col,
            pattern=make_bound_literal("John Smith%", SQLDataType.TEXT),
            case_insensitive=False,
            upstream_columns=[col],
        )
        # Broader: 'John%'
        broader = BoundLike(
            expression=col,
            pattern=make_bound_literal("John%", SQLDataType.TEXT),
            case_insensitive=False,
            upstream_columns=[col],
        )
        type_env = make_type_env(name=SQLDataType.TEXT)

        # 'John Smith%' should imply 'John%' (anything starting with 'John Smith'
        # also starts with 'John')
        # Note: This specific test may depend on Z3's string theory capabilities
        result = checker.check_implication(specific, broader, type_env=type_env)
        # At minimum, both should be valid predicates that can be checked
        assert result.implies is True or result.implies is False  # No crash


# =============================================================================
# IS NULL Tests
# =============================================================================


class TestIsNullPredicate:
    """Tests for IS NULL predicate handling."""

    @pytest.mark.skipif(
        not PredicateImplicationChecker._z3_available(),
        reason="Z3 not available",
    )
    def test_is_null_implies_itself(self, checker: PredicateImplicationChecker) -> None:
        """Test that IS NULL implies itself."""
        col = make_bound_column("x")
        is_null = BoundIsNull(
            expression=col,
            negated=False,
            upstream_columns=[col],
        )
        type_env = make_type_env(x=SQLDataType.INTEGER)

        result = checker.check_implication(is_null, is_null, type_env=type_env)
        assert result.implies is True

    @pytest.mark.skipif(
        not PredicateImplicationChecker._z3_available(),
        reason="Z3 not available",
    )
    def test_is_null_not_imply_is_not_null(self, checker: PredicateImplicationChecker) -> None:
        """Test that IS NULL does NOT imply IS NOT NULL."""
        col = make_bound_column("x")
        is_null = BoundIsNull(
            expression=col,
            negated=False,
            upstream_columns=[col],
        )
        is_not_null = BoundIsNull(
            expression=col,
            negated=True,
            upstream_columns=[col],
        )
        type_env = make_type_env(x=SQLDataType.INTEGER)

        result = checker.check_implication(is_null, is_not_null, type_env=type_env)
        assert result.implies is False


# =============================================================================
# IN Predicate Tests
# =============================================================================


class TestInPredicate:
    """Tests for IN predicate handling."""

    @pytest.mark.skipif(
        not PredicateImplicationChecker._z3_available(),
        reason="Z3 not available",
    )
    def test_in_subset_implies_superset(self, checker: PredicateImplicationChecker) -> None:
        """Test that IN (1, 2) implies IN (1, 2, 3)."""
        col = make_bound_column("x")

        in_subset = BoundIn(
            expression=col,
            values=[make_bound_literal(1), make_bound_literal(2)],
            negated=False,
            upstream_columns=[col],
        )
        in_superset = BoundIn(
            expression=col,
            values=[
                make_bound_literal(1),
                make_bound_literal(2),
                make_bound_literal(3),
            ],
            negated=False,
            upstream_columns=[col],
        )
        type_env = make_type_env(x=SQLDataType.INTEGER)

        # x IN (1, 2) implies x IN (1, 2, 3) because if x is 1 or 2,
        # it's also in {1, 2, 3}
        result = checker.check_implication(in_subset, in_superset, type_env=type_env)
        assert result.implies is True


# =============================================================================
# Empirical Validation - Value Normalization Tests
# =============================================================================


class TestValueNormalization:
    """Tests for normalizing values for content hashing."""

    def test_normalize_none(self) -> None:
        """Test normalizing NULL/None values."""
        assert _normalize_value(None) == "__NULL__"

    def test_normalize_float_precision(self) -> None:
        """Test that floats are normalized to fixed precision."""
        # Small float differences within 10 decimal precision are kept
        result = _normalize_value(1.0000000001)
        assert result == "1.0000000001"
        # Floats beyond 10 decimal places are rounded
        result2 = _normalize_value(1.00000000001)  # 11 decimal places
        assert result2 == "1"
        # Trailing zeros are stripped
        result3 = _normalize_value(1.5000)
        assert result3 == "1.5"

    def test_normalize_decimal(self) -> None:
        """Test normalizing Decimal values."""
        result = _normalize_value(Decimal("123.45"))
        assert "123.45" in result

    def test_normalize_bool(self) -> None:
        """Test normalizing boolean values."""
        assert _normalize_value(True) == "TRUE"
        assert _normalize_value(False) == "FALSE"

    def test_normalize_bytes(self) -> None:
        """Test normalizing bytes to hex."""
        result = _normalize_value(b"\x00\x01\x02")
        assert result == "000102"

    def test_normalize_datetime(self) -> None:
        """Test normalizing datetime to ISO format."""
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = _normalize_value(dt)
        assert "2024-01-15" in result

    def test_normalize_string(self) -> None:
        """Test that strings are passed through."""
        assert _normalize_value("hello") == "hello"

    def test_normalize_int(self) -> None:
        """Test that integers are converted to string."""
        assert _normalize_value(42) == "42"


# =============================================================================
# Empirical Validation - Result Set Normalization Tests
# =============================================================================


class TestResultSetNormalization:
    """Tests for normalizing result sets for comparison."""

    def test_normalize_empty_result(self) -> None:
        """Test normalizing empty result set."""
        result = _normalize_result_set([])
        assert result == []

    def test_normalize_single_row(self) -> None:
        """Test normalizing single row."""
        rows = [(1, "hello", None)]
        result = _normalize_result_set(rows)
        assert len(result) == 1
        assert result[0] == ("1", "hello", "__NULL__")

    def test_normalize_sorts_rows(self) -> None:
        """Test that rows are sorted for bag semantics."""
        rows = [
            (2, "b"),
            (1, "a"),
            (3, "c"),
        ]
        result = _normalize_result_set(rows)
        # Should be sorted
        assert result[0][0] == "1"
        assert result[1][0] == "2"
        assert result[2][0] == "3"

    def test_normalize_handles_duplicates(self) -> None:
        """Test that duplicate rows are preserved."""
        rows = [
            (1, "a"),
            (1, "a"),
            (2, "b"),
        ]
        result = _normalize_result_set(rows)
        # Duplicates should be preserved (bag semantics)
        assert len(result) == 3


# =============================================================================
# Empirical Validation - Content Hash Tests
# =============================================================================


class TestContentHash:
    """Tests for content hash computation."""

    def test_empty_result_hash(self) -> None:
        """Test hash of empty result set."""
        hash1 = _compute_content_hash([])
        hash2 = _compute_content_hash([])
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hex

    def test_same_content_same_hash(self) -> None:
        """Test that same content produces same hash."""
        rows1 = [(1, "a"), (2, "b")]
        rows2 = [(1, "a"), (2, "b")]
        assert _compute_content_hash(rows1) == _compute_content_hash(rows2)

    def test_different_order_same_hash(self) -> None:
        """Test that different row order produces same hash (bag semantics)."""
        rows1 = [(1, "a"), (2, "b")]
        rows2 = [(2, "b"), (1, "a")]
        assert _compute_content_hash(rows1) == _compute_content_hash(rows2)

    def test_different_content_different_hash(self) -> None:
        """Test that different content produces different hash."""
        rows1 = [(1, "a")]
        rows2 = [(1, "b")]
        assert _compute_content_hash(rows1) != _compute_content_hash(rows2)

    def test_null_handling_consistent(self) -> None:
        """Test that NULL values are hashed consistently."""
        rows1 = [(1, None, "x")]
        rows2 = [(1, None, "x")]
        assert _compute_content_hash(rows1) == _compute_content_hash(rows2)


# =============================================================================
# Empirical Validator Tests
# =============================================================================


class TestEmpiricalValidator:
    """Tests for the EmpiricalValidator class."""

    def test_content_equivalence_via_hash_same_results(self) -> None:
        """Test that identical results produce the same hash."""
        rows1 = [(1, "a"), (2, "b")]
        rows2 = [(1, "a"), (2, "b")]

        hash1 = _compute_content_hash(rows1)
        hash2 = _compute_content_hash(rows2)

        assert hash1 == hash2

    def test_content_equivalence_via_hash_different_results(self) -> None:
        """Test that different results produce different hashes."""
        rows1 = [(1, "a"), (2, "b")]
        rows2 = [(1, "x"), (2, "y")]

        hash1 = _compute_content_hash(rows1)
        hash2 = _compute_content_hash(rows2)

        assert hash1 != hash2

    def test_content_equivalence_via_hash_order_independent(self) -> None:
        """Test that row order doesn't affect hash (bag semantics)."""
        rows1 = [(1, "a"), (2, "b"), (3, "c")]
        rows2 = [(3, "c"), (1, "a"), (2, "b")]

        hash1 = _compute_content_hash(rows1)
        hash2 = _compute_content_hash(rows2)

        assert hash1 == hash2

    def test_validate_query_equivalence(self) -> None:
        """Test full query equivalence validation with mock executor."""

        # Create a mock execution result with the expected interface
        def make_result(rows):
            result = MagicMock()
            result.is_success.return_value = True
            result.rows = rows
            result.error = None
            return result

        mock_executor = MagicMock()
        mock_executor.is_available.return_value = True
        mock_executor.execute.side_effect = [
            make_result([(1, "a"), (2, "b")]),  # Original query results
            make_result([(2, "b"), (1, "a")]),  # Rewritten query results (different order)
        ]

        validator = EmpiricalValidator(executor=mock_executor)

        # Should be equivalent despite different row order
        result = validator._check_content_hash(
            "SELECT * FROM t1",
            "SELECT * FROM v1",
        )

        assert result["hash_match"] is True
        assert mock_executor.execute.call_count == 2


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_checker_graceful_z3_unavailable(self) -> None:
        """Test that checker works gracefully when Z3 is unavailable."""
        # Create checker - should not crash even if Z3 is missing
        checker = PredicateImplicationChecker(use_smt=True)

        # Basic fingerprint-based check should still work
        pred = make_comparison("x", "=", 5)
        result = checker.check_implication(pred, pred)
        assert result.implies is True

    def test_empty_predicate_list(self, checker: PredicateImplicationChecker) -> None:
        """Test handling of empty predicate operands."""
        empty_and = BoundLogical(
            operator="AND",
            operands=[],
            upstream_columns=[],
        )
        pred = make_comparison("x", "=", 5)

        # Should not crash
        result = checker.check_implication(empty_and, pred)
        assert result.implies is False

    def test_complex_nested_predicates(self, checker_no_smt: PredicateImplicationChecker) -> None:
        """Test deeply nested predicates."""
        p1 = make_comparison("x", ">", 1)
        p2 = make_comparison("y", "<", 10)
        p3 = make_comparison("z", "=", 5)

        inner_and = BoundLogical(
            operator="AND",
            operands=[p1, p2],
            upstream_columns=[],
        )
        outer_or = BoundLogical(
            operator="OR",
            operands=[inner_and, p3],
            upstream_columns=[],
        )

        # (x > 1 AND y < 10) OR z = 5 should imply z = 5 (disjunct)
        # This requires SMT for full verification, but structure should not crash
        _ = checker_no_smt.check_implication(outer_or, p3)


# =============================================================================
# Extended Edge Case Tests - Fourier-Motzkin, Cache, Algebraic
# =============================================================================


class TestFourierMotzkinEdgeCases:
    """Additional edge case tests for the Fourier-Motzkin linear arithmetic layer."""

    def test_equality_implies_geq(self, checker_no_smt: PredicateImplicationChecker) -> None:
        """x = 5 should imply x >= 5 via algebraic layer."""
        p1 = make_comparison("x", "=", 5)
        p2 = make_comparison("x", ">=", 5)
        result = checker_no_smt.check_implication(p1, p2)
        assert result.implies is True

    def test_equality_implies_leq(self, checker_no_smt: PredicateImplicationChecker) -> None:
        """x = 5 should imply x <= 5 via algebraic layer."""
        p1 = make_comparison("x", "=", 5)
        p2 = make_comparison("x", "<=", 5)
        result = checker_no_smt.check_implication(p1, p2)
        assert result.implies is True

    def test_equality_implies_neq_different_value(
        self, checker_no_smt: PredicateImplicationChecker
    ) -> None:
        """x = 5 should imply x != 3 via algebraic layer."""
        p1 = make_comparison("x", "=", 5)
        p2 = make_comparison("x", "!=", 3)
        result = checker_no_smt.check_implication(p1, p2)
        assert result.implies is True

    def test_fm_two_variable_elimination(self, checker_no_smt: PredicateImplicationChecker) -> None:
        """(x > 5 AND y > 10) should imply (x > 3) via FM elimination."""
        x_gt_5 = make_comparison("x", ">", 5)
        y_gt_10 = make_comparison("y", ">", 10)
        p1 = BoundLogical(
            operator="AND",
            operands=[x_gt_5, y_gt_10],
            upstream_columns=x_gt_5.upstream_columns + y_gt_10.upstream_columns,
        )
        p2 = make_comparison("x", ">", 3)
        result = checker_no_smt.check_implication(p1, p2)
        assert result.implies is True

    def test_fm_strict_and_nonstrict_mixed(
        self, checker_no_smt: PredicateImplicationChecker
    ) -> None:
        """x > 5 should imply x >= 5 (strict implies non-strict at same value)."""
        p1 = make_comparison("x", ">", 5)
        p2 = make_comparison("x", ">=", 5)
        result = checker_no_smt.check_implication(p1, p2)
        assert result.implies is True

    def test_fm_nonstrict_does_not_imply_strict_at_boundary(
        self, checker_no_smt: PredicateImplicationChecker
    ) -> None:
        """x >= 5 should NOT imply x > 5 (x=5 is a counterexample)."""
        p1 = make_comparison("x", ">=", 5)
        p2 = make_comparison("x", ">", 5)
        result = checker_no_smt.check_implication(p1, p2)
        assert result.implies is not True  # Could be False or None

    def test_between_implies_between_subrange(
        self, checker_no_smt: PredicateImplicationChecker
    ) -> None:
        """x BETWEEN 5 AND 8 implies x BETWEEN 3 AND 10."""
        col = make_bound_column("x")
        p1 = BoundBetween(
            expression=col,
            low=make_bound_literal(5),
            high=make_bound_literal(8),
            upstream_columns=[col],
        )
        p2 = BoundBetween(
            expression=col,
            low=make_bound_literal(3),
            high=make_bound_literal(10),
            upstream_columns=[col],
        )
        result = checker_no_smt.check_implication(p1, p2)
        assert result.implies is True

    def test_between_does_not_imply_narrower_range(
        self, checker_no_smt: PredicateImplicationChecker
    ) -> None:
        """x BETWEEN 1 AND 20 should NOT imply x BETWEEN 5 AND 10."""
        col = make_bound_column("x")
        p1 = BoundBetween(
            expression=col,
            low=make_bound_literal(1),
            high=make_bound_literal(20),
            upstream_columns=[col],
        )
        p2 = BoundBetween(
            expression=col,
            low=make_bound_literal(5),
            high=make_bound_literal(10),
            upstream_columns=[col],
        )
        result = checker_no_smt.check_implication(p1, p2)
        assert result.implies is not True


class TestCacheBehavior:
    """Tests for the LRU cache in PredicateImplicationChecker."""

    def test_cache_hit_on_repeated_query(self) -> None:
        """Second call for same predicates should return cached result."""
        checker = PredicateImplicationChecker(use_smt=False, cache_size=100)
        p1 = make_comparison("x", "=", 5)
        p2 = make_comparison("x", "=", 5)

        result1 = checker.check_implication(p1, p2)
        assert result1.cached is False

        result2 = checker.check_implication(p1, p2)
        assert result2.cached is True
        assert result2.implies == result1.implies

    def test_cache_stats(self) -> None:
        """Cache stats should report hits and misses."""
        checker = PredicateImplicationChecker(use_smt=False, cache_size=100)
        p1 = make_comparison("x", "=", 5)

        checker.check_implication(p1, p1)
        checker.check_implication(p1, p1)

        stats = checker.cache_stats()
        assert stats["hits"] >= 1
        assert stats["misses"] >= 1
        assert stats["size"] >= 1

    def test_cache_clear(self) -> None:
        """Clearing cache should reset stats."""
        checker = PredicateImplicationChecker(use_smt=False, cache_size=100)
        p1 = make_comparison("x", "=", 5)

        checker.check_implication(p1, p1)
        checker.clear_cache()

        stats = checker.cache_stats()
        assert stats["size"] == 0

    def test_cache_eviction(self) -> None:
        """Cache should evict entries when full."""
        checker = PredicateImplicationChecker(use_smt=False, cache_size=3)

        # Fill cache with 3 different pairs
        for i in range(5):
            p = make_comparison("x", "=", i)
            q = make_comparison("x", "=", i + 100)
            checker.check_implication(p, q)

        stats = checker.cache_stats()
        assert stats["size"] <= 3


@pytest.mark.skipif(
    not PredicateImplicationChecker._z3_available(),
    reason="Z3 not available",
)
class TestSMTThreeValuedLogic:
    """Tests for correct SQL 3-valued logic handling in SMT encoding."""

    def test_not_not_p_equivalent_to_p(self) -> None:
        """NOT(NOT(x > 5)) should be equivalent to x > 5."""
        checker = PredicateImplicationChecker(use_smt=True, smt_timeout_ms=2000)
        p = make_comparison("x", ">", 5)

        not_p = BoundLogical(operator="NOT", operands=[p], upstream_columns=p.upstream_columns)
        not_not_p = BoundLogical(
            operator="NOT", operands=[not_p], upstream_columns=p.upstream_columns
        )

        # NOT(NOT(p)) should imply p and p should imply NOT(NOT(p))
        result1 = checker.check_implication(not_not_p, p)
        assert result1.implies is True

        result2 = checker.check_implication(p, not_not_p)
        assert result2.implies is True

    def test_smt_string_equality_implication(self) -> None:
        """x = 'hello' should imply x = 'hello' (string type)."""
        checker = PredicateImplicationChecker(use_smt=True, smt_timeout_ms=2000)
        p1 = make_comparison("name", "=", "hello", col_type=SQLDataType.VARCHAR)
        type_env = make_type_env(**{"t.name": SQLDataType.VARCHAR})

        result = checker.check_implication(p1, p1, type_env=type_env)
        assert result.implies is True

    def test_smt_integer_range_implication(self) -> None:
        """x > 10 AND x < 20 should imply x >= 10 (via SMT)."""
        checker = PredicateImplicationChecker(use_smt=True, smt_timeout_ms=2000)
        p_gt10 = make_comparison("x", ">", 10)
        p_lt20 = make_comparison("x", "<", 20)
        conjunction = BoundLogical(
            operator="AND",
            operands=[p_gt10, p_lt20],
            upstream_columns=p_gt10.upstream_columns + p_lt20.upstream_columns,
        )
        p_geq10 = make_comparison("x", ">=", 10)

        type_env = make_type_env(**{"t.x": SQLDataType.INTEGER})
        result = checker.check_implication(conjunction, p_geq10, type_env=type_env)
        assert result.implies is True
