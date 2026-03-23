"""Tests for algebrakit bound types and fingerprint module."""


from alma_algebrakit.bound.fingerprint import bound_expr_fingerprint, bound_predicate_fingerprint
from alma_algebrakit.bound.types import (
    AttributeRef,
    BoundBetween,
    BoundComparison,
    BoundFunctionCall,
    BoundIn,
    BoundIsNull,
    BoundLiteral,
    BoundLogical,
    BoundWindowFunction,
)


class TestAttributeRef:
    """Tests for AttributeRef."""

    def test_qualified_id_returns_table_id_column_id(self) -> None:
        """AttributeRef qualified_id() returns 'table_id.column_id'."""
        ref = AttributeRef(table_id="public.users", column_id="id", sql_name="id")
        assert ref.qualified_id() == "public.users.id"

    def test_equality_by_qualified_id(self) -> None:
        """AttributeRef equality by qualified_id."""
        ref1 = AttributeRef(table_id="t1", column_id="col", sql_name="col")
        ref2 = AttributeRef(table_id="t1", column_id="col", sql_name="col")
        ref3 = AttributeRef(table_id="t2", column_id="col", sql_name="col")
        assert ref1 == ref2
        assert ref1 != ref3

    def test_hashing_consistent_with_equality(self) -> None:
        """AttributeRef hashing consistent with equality."""
        ref1 = AttributeRef(table_id="t1", column_id="col", sql_name="col")
        ref2 = AttributeRef(table_id="t1", column_id="col", sql_name="col")
        assert hash(ref1) == hash(ref2)
        s = {ref1}
        assert ref2 in s

    def test_display_name_with_alias(self) -> None:
        """AttributeRef display_name with alias."""
        ref = AttributeRef(table_id="t1", column_id="col", sql_name="col", table_alias="u")
        assert ref.display_name() == "u.col"

    def test_display_name_without_alias(self) -> None:
        """AttributeRef display_name without alias."""
        ref = AttributeRef(table_id="t1", column_id="col", sql_name="col")
        assert ref.display_name() == "col"


class TestBoundLiteral:
    """Tests for BoundLiteral."""

    def test_is_null_returns_true_for_none_value(self) -> None:
        """BoundLiteral.is_null() returns True for None value."""
        lit = BoundLiteral(value=None)
        assert lit.is_null() is True

    def test_is_null_returns_false_for_non_none(self) -> None:
        """BoundLiteral.is_null() returns False for non-None."""
        lit = BoundLiteral(value=5)
        assert lit.is_null() is False
        lit_str = BoundLiteral(value="hello")
        assert lit_str.is_null() is False


class TestBoundComparison:
    """Tests for BoundComparison construction and upstream_columns."""

    def test_bound_comparison_construction_and_upstream_columns(self) -> None:
        """BoundComparison construction and structure."""
        col_ref = AttributeRef(table_id="t1", column_id="x", sql_name="x")
        lit = BoundLiteral(value=5)
        pred = BoundComparison(left=col_ref, operator="=", right=lit, upstream_columns=[col_ref])
        assert pred.left == col_ref
        assert pred.right == lit
        assert pred.operator == "="
        assert col_ref in pred.upstream_columns


class TestBoundIsNull:
    """Tests for BoundIsNull."""

    def test_bound_is_null_with_negated_false(self) -> None:
        """BoundIsNull with negated=False."""
        col_ref = AttributeRef(table_id="t1", column_id="x", sql_name="x")
        pred = BoundIsNull(expression=col_ref, negated=False)
        assert pred.negated is False
        fp = bound_predicate_fingerprint(pred)
        assert "IS NULL" in fp
        assert "NOT" not in fp

    def test_bound_is_null_with_negated_true(self) -> None:
        """BoundIsNull with negated=True."""
        col_ref = AttributeRef(table_id="t1", column_id="x", sql_name="x")
        pred = BoundIsNull(expression=col_ref, negated=True)
        assert pred.negated is True
        fp = bound_predicate_fingerprint(pred)
        assert "NOT" in fp
        assert "NULL" in fp


class TestBoundWindowFunction:
    """Tests for BoundWindowFunction."""

    def test_is_ranking_function_returns_true_for_row_number(self) -> None:
        """BoundWindowFunction.is_ranking_function() for ROW_NUMBER."""
        func = BoundFunctionCall(function_name="row_number", args=[])
        wf = BoundWindowFunction(function=func)
        assert wf.is_ranking_function() is True

    def test_is_ranking_function_returns_true_for_rank(self) -> None:
        """BoundWindowFunction.is_ranking_function() for RANK."""
        func = BoundFunctionCall(function_name="rank", args=[])
        wf = BoundWindowFunction(function=func)
        assert wf.is_ranking_function() is True

    def test_is_ranking_function_returns_false_for_sum(self) -> None:
        """BoundWindowFunction.is_ranking_function() returns False for SUM."""
        func = BoundFunctionCall(function_name="sum", args=[])
        wf = BoundWindowFunction(function=func)
        assert wf.is_ranking_function() is False

    def test_is_offset_function_returns_true_for_lag(self) -> None:
        """BoundWindowFunction.is_offset_function() for LAG."""
        func = BoundFunctionCall(function_name="lag", args=[])
        wf = BoundWindowFunction(function=func)
        assert wf.is_offset_function() is True

    def test_is_offset_function_returns_true_for_lead(self) -> None:
        """BoundWindowFunction.is_offset_function() for LEAD."""
        func = BoundFunctionCall(function_name="lead", args=[])
        wf = BoundWindowFunction(function=func)
        assert wf.is_offset_function() is True

    def test_is_offset_function_returns_false_for_row_number(self) -> None:
        """BoundWindowFunction.is_offset_function() returns False for ROW_NUMBER."""
        func = BoundFunctionCall(function_name="row_number", args=[])
        wf = BoundWindowFunction(function=func)
        assert wf.is_offset_function() is False


class TestBoundPredicateFingerprint:
    """Tests for bound_predicate_fingerprint."""

    def test_bound_predicate_fingerprint_for_bound_comparison(self) -> None:
        """bound_predicate_fingerprint for BoundComparison."""
        col = AttributeRef(table_id="t1", column_id="x", sql_name="x")
        lit = BoundLiteral(value=5)
        pred = BoundComparison(left=col, operator="=", right=lit)
        fp = bound_predicate_fingerprint(pred)
        assert "t1.x" in fp
        assert "=" in fp
        assert "5" in fp

    def test_bound_predicate_fingerprint_for_bound_logical_and(self) -> None:
        """bound_predicate_fingerprint for BoundLogical (AND)."""
        col1 = AttributeRef(table_id="t1", column_id="a", sql_name="a")
        col2 = AttributeRef(table_id="t1", column_id="b", sql_name="b")
        pred1 = BoundComparison(left=col1, operator="=", right=BoundLiteral(value=1))
        pred2 = BoundComparison(left=col2, operator="=", right=BoundLiteral(value=2))
        logical = BoundLogical(operator="AND", operands=[pred1, pred2])
        fp = bound_predicate_fingerprint(logical)
        assert "AND" in fp
        assert "t1.a" in fp
        assert "t1.b" in fp

    def test_bound_predicate_fingerprint_for_bound_in(self) -> None:
        """bound_predicate_fingerprint for BoundIn."""
        col = AttributeRef(table_id="t1", column_id="x", sql_name="x")
        vals = [BoundLiteral(value=1), BoundLiteral(value=2)]
        pred = BoundIn(expression=col, values=vals)
        fp = bound_predicate_fingerprint(pred)
        assert "IN" in fp
        assert "t1.x" in fp
        assert "1" in fp
        assert "2" in fp

    def test_bound_predicate_fingerprint_for_bound_between(self) -> None:
        """bound_predicate_fingerprint for BoundBetween."""
        col = AttributeRef(table_id="t1", column_id="x", sql_name="x")
        pred = BoundBetween(
            expression=col,
            low=BoundLiteral(value=1),
            high=BoundLiteral(value=10),
        )
        fp = bound_predicate_fingerprint(pred)
        assert "BETWEEN" in fp
        assert "t1.x" in fp
        assert "1" in fp
        assert "10" in fp


class TestBoundExprFingerprint:
    """Tests for bound_expr_fingerprint."""

    def test_bound_expr_fingerprint_for_attribute_ref(self) -> None:
        """bound_expr_fingerprint for AttributeRef."""
        ref = AttributeRef(table_id="t1", column_id="col", sql_name="col")
        fp = bound_expr_fingerprint(ref)
        assert fp == "t1.col"

    def test_bound_expr_fingerprint_for_bound_literal(self) -> None:
        """bound_expr_fingerprint for BoundLiteral."""
        lit = BoundLiteral(value=42)
        fp = bound_expr_fingerprint(lit)
        assert "42" in fp


class TestFingerprintConsistency:
    """Tests for fingerprint consistency."""

    def test_same_predicate_always_produces_same_string(self) -> None:
        """Fingerprint consistency: same predicate always produces same string."""
        col = AttributeRef(table_id="t1", column_id="x", sql_name="x")
        lit = BoundLiteral(value=5)
        pred = BoundComparison(left=col, operator="=", right=lit)
        fp1 = bound_predicate_fingerprint(pred)
        fp2 = bound_predicate_fingerprint(pred)
        assert fp1 == fp2
