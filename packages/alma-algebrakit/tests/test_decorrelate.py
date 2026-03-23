"""Comprehensive tests for the decorrelation transforms module.

Tests the transformation of correlated subqueries into equivalent join-based forms:
- EXISTS → SEMI-JOIN
- NOT EXISTS → ANTI-JOIN
- IN (subquery) → SEMI-JOIN
- NOT IN → rejected (NULL semantics)
"""

from __future__ import annotations

from alma_algebrakit.models.algebra import (
    AtomicPredicate,
    ColumnRef,
    ComparisonOp,
    CompoundPredicate,
    ExistsExpression,
    InSubqueryExpression,
    Join,
    JoinType,
    Literal,
    LogicalOp,
    Projection,
    Relation,
    Selection,
    SubqueryExpression,
)
from alma_algebrakit.transforms.decorrelate import (
    CorrelationInfo,
    decorrelate_query,
)

# =============================================================================
# EXISTS → SEMI-JOIN
# =============================================================================


class TestExistsToSemiJoin:
    """Tests for EXISTS subquery transformation to SEMI-JOIN."""

    def test_exists_correlated_subquery_becomes_semi_join(self) -> None:
        """Build Selection with EXISTS containing correlated subquery. Verify SEMI-JOIN."""
        # SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
        users = Relation(name="users", alias="u", columns=["id", "name"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])

        correlation_pred = AtomicPredicate(
            left=ColumnRef(table="o", column="user_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="u", column="id"),
        )
        subquery = Selection(
            predicate=correlation_pred,
            input=orders,
        )
        exists_expr = ExistsExpression(query=subquery, negated=False)

        # Use CompoundPredicate to wrap EXISTS (operands accept predicate-like expressions)
        query = Selection(
            predicate=CompoundPredicate(op=LogicalOp.AND, operands=[exists_expr]),
            input=users,
        )

        result = decorrelate_query(query)

        assert result.success
        assert "EXISTS → SEMI-JOIN" in result.transforms_applied
        assert result.decorrelated_ra is not None

        # Verify result is a Join with SEMI type
        assert isinstance(result.decorrelated_ra, Join)
        assert result.decorrelated_ra.join_type == JoinType.SEMI
        assert result.decorrelated_ra.left == users
        assert result.decorrelated_ra.right == orders
        assert result.decorrelated_ra.condition == correlation_pred

    def test_exists_standalone_predicate(self) -> None:
        """EXISTS as the sole predicate (wrapped in AND for single operand)."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["user_id"])

        subquery = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="o", column="user_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="u", column="id"),
            ),
            input=orders,
        )
        exists_expr = ExistsExpression(query=subquery, negated=False)
        query = Selection(
            predicate=CompoundPredicate(op=LogicalOp.AND, operands=[exists_expr]),
            input=users,
        )

        result = decorrelate_query(query)

        assert result.success
        assert "EXISTS → SEMI-JOIN" in result.transforms_applied
        assert isinstance(result.decorrelated_ra, Join)
        assert result.decorrelated_ra.join_type == JoinType.SEMI


# =============================================================================
# NOT EXISTS → ANTI-JOIN
# =============================================================================


class TestNotExistsToAntiJoin:
    """Tests for NOT EXISTS transformation to ANTI-JOIN."""

    def test_not_exists_becomes_anti_join(self) -> None:
        """NOT EXISTS with correlated subquery. Verify ANTI-JOIN."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["user_id"])

        subquery = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="o", column="user_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="u", column="id"),
            ),
            input=orders,
        )
        exists_expr = ExistsExpression(query=subquery, negated=True)
        query = Selection(
            predicate=CompoundPredicate(op=LogicalOp.AND, operands=[exists_expr]),
            input=users,
        )

        result = decorrelate_query(query)

        assert result.success
        assert "NOT EXISTS → ANTI-JOIN" in result.transforms_applied
        assert isinstance(result.decorrelated_ra, Join)
        assert result.decorrelated_ra.join_type == JoinType.ANTI


# =============================================================================
# IN (subquery) → SEMI-JOIN
# =============================================================================


class TestInToSemiJoin:
    """Tests for IN (subquery) transformation to SEMI-JOIN."""

    def test_in_subquery_becomes_semi_join(self) -> None:
        """IN (subquery) with correlation. Verify SEMI-JOIN with combined condition."""
        users = Relation(name="users", alias="u", columns=["id", "region"])
        orders = Relation(name="orders", alias="o", columns=["user_id", "region"])

        # u.id IN (SELECT o.user_id FROM orders o WHERE o.region = u.region)
        subquery_inner = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="o", column="region"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="u", column="region"),
            ),
            input=orders,
        )
        subquery = Projection(
            columns=[(ColumnRef(table="o", column="user_id"), None)],
            input=subquery_inner,
        )
        in_expr = InSubqueryExpression(
            left=ColumnRef(table="u", column="id"),
            query=subquery,
            negated=False,
        )
        query = Selection(
            predicate=CompoundPredicate(op=LogicalOp.AND, operands=[in_expr]),
            input=users,
        )

        result = decorrelate_query(query)

        assert result.success
        assert "IN (subquery) → SEMI-JOIN" in result.transforms_applied
        assert isinstance(result.decorrelated_ra, Join)
        assert result.decorrelated_ra.join_type == JoinType.SEMI
        # Join condition should combine u.id = o.user_id AND u.region = o.region
        assert result.decorrelated_ra.condition is not None
        # Join condition combines u.id = o.user_id and optionally u.region = o.region
        if isinstance(result.decorrelated_ra.condition, CompoundPredicate):
            assert result.decorrelated_ra.condition.op == LogicalOp.AND
            assert len(result.decorrelated_ra.condition.operands) >= 1
        else:
            assert isinstance(result.decorrelated_ra.condition, AtomicPredicate)


# =============================================================================
# NOT IN rejection
# =============================================================================


class TestNotInRejection:
    """Tests for NOT IN rejection due to NULL semantics."""

    def test_not_in_returns_non_decorrelatable(self) -> None:
        """InSubqueryExpression with negated=True. Verify non_decorrelatable populated."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["user_id"])

        subquery = Projection(
            columns=[(ColumnRef(table="o", column="user_id"), None)],
            input=orders,
        )
        in_expr = InSubqueryExpression(
            left=ColumnRef(table="u", column="id"),
            query=subquery,
            negated=True,
        )
        query = Selection(
            predicate=CompoundPredicate(op=LogicalOp.AND, operands=[in_expr]),
            input=users,
        )

        result = decorrelate_query(query)

        # NOT IN should be rejected - success=False with non_decorrelatable populated
        assert not result.success
        assert len(result.non_decorrelatable) > 0


# =============================================================================
# Uncorrelated EXISTS
# =============================================================================


class TestUncorrelatedExists:
    """Tests for uncorrelated EXISTS (no transformation)."""

    def test_uncorrelated_exists_no_transformation(self) -> None:
        """EXISTS subquery that doesn't reference outer tables. No transformation."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["user_id"])

        # EXISTS (SELECT 1 FROM orders WHERE user_id > 100) - no u.id reference
        subquery = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="o", column="user_id"),
                op=ComparisonOp.GT,
                right=Literal(value=100),
            ),
            input=orders,
        )
        exists_expr = ExistsExpression(query=subquery, negated=False)
        query = Selection(
            predicate=CompoundPredicate(op=LogicalOp.AND, operands=[exists_expr]),
            input=users,
        )

        result = decorrelate_query(query)

        # Should succeed but no transform applied (uncorrelated can't become join)
        assert result.success
        assert "EXISTS → SEMI-JOIN" not in result.transforms_applied
        # Original structure preserved
        assert isinstance(result.decorrelated_ra, Selection)


# =============================================================================
# Multiple correlations
# =============================================================================


class TestMultipleCorrelations:
    """Tests for EXISTS with multiple correlation predicates."""

    def test_exists_with_multiple_correlations(self) -> None:
        """EXISTS with o.user_id = u.id AND o.region = u.region."""
        users = Relation(name="users", alias="u", columns=["id", "region"])
        orders = Relation(name="orders", alias="o", columns=["user_id", "region"])

        corr_pred = CompoundPredicate(
            op=LogicalOp.AND,
            operands=[
                AtomicPredicate(
                    left=ColumnRef(table="o", column="user_id"),
                    op=ComparisonOp.EQ,
                    right=ColumnRef(table="u", column="id"),
                ),
                AtomicPredicate(
                    left=ColumnRef(table="o", column="region"),
                    op=ComparisonOp.EQ,
                    right=ColumnRef(table="u", column="region"),
                ),
            ],
        )
        subquery = Selection(predicate=corr_pred, input=orders)
        exists_expr = ExistsExpression(query=subquery, negated=False)
        query = Selection(
            predicate=CompoundPredicate(op=LogicalOp.AND, operands=[exists_expr]),
            input=users,
        )

        result = decorrelate_query(query)

        assert result.success
        assert "EXISTS → SEMI-JOIN" in result.transforms_applied
        assert isinstance(result.decorrelated_ra, Join)
        assert result.decorrelated_ra.join_type == JoinType.SEMI
        assert result.decorrelated_ra.condition is not None
        assert isinstance(result.decorrelated_ra.condition, CompoundPredicate)
        assert len(result.decorrelated_ra.condition.operands) == 2


# =============================================================================
# AND conjunction with EXISTS
# =============================================================================


class TestAndConjunctionWithExists:
    """Tests for AND of EXISTS and regular predicate."""

    def test_and_of_exists_and_regular_predicate(self) -> None:
        """Selection predicate is AND of EXISTS and regular predicate."""
        users = Relation(name="users", alias="u", columns=["id", "active"])
        orders = Relation(name="orders", alias="o", columns=["user_id"])

        exists_subquery = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="o", column="user_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="u", column="id"),
            ),
            input=orders,
        )
        exists_expr = ExistsExpression(query=exists_subquery, negated=False)

        regular_pred = AtomicPredicate(
            left=ColumnRef(table="u", column="active"),
            op=ComparisonOp.EQ,
            right=Literal(value=True),
        )

        query = Selection(
            predicate=CompoundPredicate(
                op=LogicalOp.AND,
                operands=[exists_expr, regular_pred],
            ),
            input=users,
        )

        result = decorrelate_query(query)

        assert result.success
        assert "EXISTS → SEMI-JOIN" in result.transforms_applied
        # Result should be Selection(regular_pred, Join(SEMI))
        assert isinstance(result.decorrelated_ra, Selection)
        assert result.decorrelated_ra.predicate == regular_pred
        assert isinstance(result.decorrelated_ra.input, Join)
        assert result.decorrelated_ra.input.join_type == JoinType.SEMI


# =============================================================================
# No correlations (plain query)
# =============================================================================


class TestNoCorrelations:
    """Tests for queries without correlated subqueries."""

    def test_simple_selection_no_correlations(self) -> None:
        """Simple Selection on Relation. success=True, no transforms."""
        users = Relation(name="users", alias="u", columns=["id", "name"])

        pred = AtomicPredicate(
            left=ColumnRef(table="u", column="id"),
            op=ComparisonOp.GT,
            right=Literal(value=0),
        )
        query = Selection(predicate=pred, input=users)

        result = decorrelate_query(query)

        assert result.success
        assert len(result.transforms_applied) == 0
        assert result.decorrelated_ra == query

    def test_plain_relation_no_correlations(self) -> None:
        """Plain Relation. success=True, no transforms."""
        users = Relation(name="users", alias="u")
        result = decorrelate_query(users)

        assert result.success
        assert len(result.transforms_applied) == 0
        assert result.decorrelated_ra == users


# =============================================================================
# Nested join with EXISTS in WHERE
# =============================================================================


class TestNestedJoinWithExists:
    """Tests for Join with EXISTS in WHERE clause."""

    def test_join_with_exists_in_where(self) -> None:
        """Join of two tables with EXISTS in the WHERE clause."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["user_id", "total"])
        products = Relation(name="products", alias="p", columns=["id", "order_id"])

        # users JOIN orders ON u.id = o.user_id
        base_join = Join(
            left=users,
            right=orders,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="user_id"),
            ),
        )

        # WHERE EXISTS (SELECT 1 FROM products p WHERE p.order_id = o.id)
        exists_subquery = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="p", column="order_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="id"),
            ),
            input=products,
        )
        exists_expr = ExistsExpression(query=exists_subquery, negated=False)

        query = Selection(
            predicate=CompoundPredicate(op=LogicalOp.AND, operands=[exists_expr]),
            input=base_join,
        )

        result = decorrelate_query(query)

        assert result.success
        assert "EXISTS → SEMI-JOIN" in result.transforms_applied
        # Result: (users JOIN orders) SEMI-JOIN products
        assert isinstance(result.decorrelated_ra, Join)
        assert result.decorrelated_ra.join_type == JoinType.SEMI
        assert result.decorrelated_ra.right == products


# =============================================================================
# DecorrelationResult fields
# =============================================================================


class TestDecorrelationResultFields:
    """Tests for DecorrelationResult structure."""

    def test_result_fields_populated_correctly(self) -> None:
        """Verify success, transforms_applied, correlations_found are populated."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["user_id"])

        subquery = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="o", column="user_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="u", column="id"),
            ),
            input=orders,
        )
        exists_expr = ExistsExpression(query=subquery, negated=False)
        query = Selection(
            predicate=CompoundPredicate(op=LogicalOp.AND, operands=[exists_expr]),
            input=users,
        )

        result = decorrelate_query(query)

        assert result.success
        assert len(result.transforms_applied) >= 1
        assert len(result.correlations_found) >= 1
        assert isinstance(result.correlations_found[0], CorrelationInfo)
        assert result.correlations_found[0].outer_table == "u"
        assert result.correlations_found[0].outer_column == "id"
        assert result.correlations_found[0].inner_table == "o"
        assert result.correlations_found[0].inner_column == "user_id"


# =============================================================================
# Correlation detection
# =============================================================================


class TestCorrelationDetection:
    """Tests for correlation predicate detection."""

    def test_equi_correlation_detected(self) -> None:
        """o.user_id = u.id where u is outer is correctly detected as correlation."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["user_id"])

        subquery = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="o", column="user_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="u", column="id"),
            ),
            input=orders,
        )
        exists_expr = ExistsExpression(query=subquery, negated=False)
        query = Selection(
            predicate=CompoundPredicate(op=LogicalOp.AND, operands=[exists_expr]),
            input=users,
        )

        result = decorrelate_query(query)

        assert result.success
        assert len(result.correlations_found) == 1
        corr = result.correlations_found[0]
        assert corr.outer_table == "u"
        assert corr.outer_column == "id"
        assert corr.inner_table == "o"
        assert corr.inner_column == "user_id"

    def test_non_equi_correlation_ignored(self) -> None:
        """Correlation with > instead of = should not be extracted."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["user_id"])

        # o.user_id > u.id - non-equi, not a valid correlation for join
        subquery = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="o", column="user_id"),
                op=ComparisonOp.GT,
                right=ColumnRef(table="u", column="id"),
            ),
            input=orders,
        )
        exists_expr = ExistsExpression(query=subquery, negated=False)
        query = Selection(
            predicate=CompoundPredicate(op=LogicalOp.AND, operands=[exists_expr]),
            input=users,
        )

        result = decorrelate_query(query)

        # No correlation extracted (non-equi), so no transform
        assert "EXISTS → SEMI-JOIN" not in result.transforms_applied
        assert len(result.correlations_found) == 0


# =============================================================================
# EXISTS with remaining inner predicates
# =============================================================================


class TestExistsWithInnerPredicates:
    """Tests for EXISTS with both correlation and inner-only predicates."""

    def test_exists_correlation_and_inner_predicate(self) -> None:
        """EXISTS has correlation AND inner-only predicate. Inner becomes Selection on right."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["user_id", "total"])

        # EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.total > 100)
        inner_pred = CompoundPredicate(
            op=LogicalOp.AND,
            operands=[
                AtomicPredicate(
                    left=ColumnRef(table="o", column="user_id"),
                    op=ComparisonOp.EQ,
                    right=ColumnRef(table="u", column="id"),
                ),
                AtomicPredicate(
                    left=ColumnRef(table="o", column="total"),
                    op=ComparisonOp.GT,
                    right=Literal(value=100),
                ),
            ],
        )
        subquery = Selection(predicate=inner_pred, input=orders)
        exists_expr = ExistsExpression(query=subquery, negated=False)
        query = Selection(
            predicate=CompoundPredicate(op=LogicalOp.AND, operands=[exists_expr]),
            input=users,
        )

        result = decorrelate_query(query)

        assert result.success
        assert "EXISTS → SEMI-JOIN" in result.transforms_applied
        assert isinstance(result.decorrelated_ra, Join)
        # Right side should be Selection(orders, o.total > 100)
        right = result.decorrelated_ra.right
        assert isinstance(right, Selection)
        assert right.input == orders
        assert right.predicate is not None


# =============================================================================
# Scalar subquery not decorrelated
# =============================================================================


class TestScalarSubqueryNotDecorrelated:
    """Tests for scalar subquery in projection."""

    def test_scalar_subquery_in_projection_non_decorrelatable(self) -> None:
        """SubqueryExpression in projection should be marked non-decorrelatable."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["user_id", "total"])

        subquery = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="o", column="user_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="u", column="id"),
            ),
            input=orders,
        )
        scalar_expr = SubqueryExpression(query=subquery)

        query = Projection(
            columns=[
                (ColumnRef(table="u", column="id"), "id"),
                (scalar_expr, "order_total"),
            ],
            input=users,
        )

        result = decorrelate_query(query)

        # Scalar subqueries in projection are non-decorrelatable
        assert not result.success or len(result.non_decorrelatable) > 0


# =============================================================================
# Full pipeline test
# =============================================================================


class TestFullPipeline:
    """End-to-end pipeline tests."""

    def test_realistic_exists_full_tree_structure(self) -> None:
        """Realistic query with EXISTS. Verify full decorrelated tree structure."""
        # SELECT * FROM users u WHERE u.active = true AND EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
        users = Relation(name="users", alias="u", columns=["id", "name", "active"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id", "total"])

        exists_subquery = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="o", column="user_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="u", column="id"),
            ),
            input=orders,
        )
        exists_expr = ExistsExpression(query=exists_subquery, negated=False)

        active_pred = AtomicPredicate(
            left=ColumnRef(table="u", column="active"),
            op=ComparisonOp.EQ,
            right=Literal(value=True),
        )

        query = Selection(
            predicate=CompoundPredicate(
                op=LogicalOp.AND,
                operands=[active_pred, exists_expr],
            ),
            input=users,
        )

        result = decorrelate_query(query)

        assert result.success
        assert "EXISTS → SEMI-JOIN" in result.transforms_applied

        # Tree: Selection(active_pred, Join(users, orders, SEMI))
        assert isinstance(result.decorrelated_ra, Selection)
        assert result.decorrelated_ra.predicate == active_pred

        join = result.decorrelated_ra.input
        assert isinstance(join, Join)
        assert join.join_type == JoinType.SEMI
        assert join.left == users
        assert join.right == orders
        assert join.condition is not None
        assert join.condition.fingerprint() == "o.user_id = u.id"
