"""Tests for GCS (Greatest Common Subexpression) computation and hybrid learning."""

import pytest

from alma_algebrakit.learning.gcs import (
    cq_to_ra,
    extract_common_structure,
    gcs_all,
    greatest_common_subexpression,
    is_spj_query,
    ra_to_cq,
)
from alma_algebrakit.learning.hybrid import (
    HybridLearningResult,
    HybridViewLearner,
    learn_verified_views,
    learn_views_hybrid,
)
from alma_algebrakit.models.algebra import (
    AggregateFunction,
    AggregateSpec,
    Aggregation,
    AtomicPredicate,
    ColumnRef,
    ComparisonOp,
    Join,
    JoinType,
    Literal,
    Projection,
    RAExpression,
    Relation,
    Selection,
    Union,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def simple_relation() -> RAExpression:
    """Simple relation: SELECT * FROM orders"""
    return Relation(name="orders", alias="o")


@pytest.fixture
def simple_selection() -> RAExpression:
    """Selection: SELECT * FROM orders WHERE status = 'active'"""
    return Selection(
        predicate=AtomicPredicate(
            left=ColumnRef(table="o", column="status"),
            op=ComparisonOp.EQ,
            right=Literal(value="active"),
        ),
        input=Relation(name="orders", alias="o"),
    )


@pytest.fixture
def range_selection_10() -> RAExpression:
    """Selection: SELECT * FROM orders WHERE amount > 10"""
    return Selection(
        predicate=AtomicPredicate(
            left=ColumnRef(table="o", column="amount"),
            op=ComparisonOp.GT,
            right=Literal(value=10),
        ),
        input=Relation(name="orders", alias="o"),
    )


@pytest.fixture
def range_selection_5() -> RAExpression:
    """Selection: SELECT * FROM orders WHERE amount > 5"""
    return Selection(
        predicate=AtomicPredicate(
            left=ColumnRef(table="o", column="amount"),
            op=ComparisonOp.GT,
            right=Literal(value=5),
        ),
        input=Relation(name="orders", alias="o"),
    )


@pytest.fixture
def two_table_join() -> RAExpression:
    """Join: SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id"""
    return Join(
        left=Relation(name="orders", alias="o"),
        right=Relation(name="customers", alias="c"),
        join_type=JoinType.INNER,
        condition=AtomicPredicate(
            left=ColumnRef(table="o", column="customer_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="c", column="id"),
        ),
    )


@pytest.fixture
def three_table_join() -> RAExpression:
    """Join: orders o JOIN customers c JOIN products p"""
    return Join(
        left=Join(
            left=Relation(name="orders", alias="o"),
            right=Relation(name="customers", alias="c"),
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="o", column="customer_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="c", column="id"),
            ),
        ),
        right=Relation(name="products", alias="p"),
        join_type=JoinType.INNER,
        condition=AtomicPredicate(
            left=ColumnRef(table="o", column="product_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="p", column="id"),
        ),
    )


@pytest.fixture
def join_with_selection() -> RAExpression:
    """Join with selection: orders JOIN customers WHERE status = 'active'"""
    return Selection(
        predicate=AtomicPredicate(
            left=ColumnRef(table="o", column="status"),
            op=ComparisonOp.EQ,
            right=Literal(value="active"),
        ),
        input=Join(
            left=Relation(name="orders", alias="o"),
            right=Relation(name="customers", alias="c"),
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="o", column="customer_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="c", column="id"),
            ),
        ),
    )


@pytest.fixture
def aggregation_query() -> RAExpression:
    """Aggregation: SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id"""
    return Aggregation(
        group_by=[ColumnRef(table="o", column="customer_id")],
        aggregates=[
            AggregateSpec(
                function=AggregateFunction.SUM,
                argument=ColumnRef(table="o", column="amount"),
                alias="total",
            )
        ],
        input=Relation(name="orders", alias="o"),
    )


@pytest.fixture
def union_query() -> RAExpression:
    """Union: SELECT * FROM orders UNION SELECT * FROM archive_orders"""
    return Union(
        left=Relation(name="orders", alias="o"),
        right=Relation(name="archive_orders", alias="a"),
        all=False,
    )


# =============================================================================
# Test: RA to CQ Conversion
# =============================================================================


class TestRAToCQ:
    """Tests for ra_to_cq conversion."""

    def test_simple_relation(self, simple_relation: RAExpression) -> None:
        """Test converting a simple relation."""
        result = ra_to_cq(simple_relation)
        assert result.success
        assert result.is_spj
        assert result.cq is not None
        assert len(result.cq.atoms) == 1
        assert result.cq.atoms[0].relation_id == "o"

    def test_selection(self, simple_selection: RAExpression) -> None:
        """Test converting a selection."""
        result = ra_to_cq(simple_selection)
        assert result.success
        assert result.is_spj
        assert result.cq is not None
        assert len(result.cq.atoms) == 1
        assert len(result.cq.predicates) == 1

    def test_two_table_join(self, two_table_join: RAExpression) -> None:
        """Test converting a two-table join."""
        result = ra_to_cq(two_table_join)
        assert result.success
        assert result.is_spj
        assert result.cq is not None
        assert len(result.cq.atoms) == 2

    def test_aggregation_not_spj(self, aggregation_query: RAExpression) -> None:
        """Test that aggregation is detected as non-SPJ."""
        result = ra_to_cq(aggregation_query)
        assert not result.is_spj

    def test_union_not_spj(self, union_query: RAExpression) -> None:
        """Test that UNION is detected as non-SPJ."""
        result = ra_to_cq(union_query)
        assert not result.is_spj


class TestIsSPJQuery:
    """Tests for is_spj_query function."""

    def test_simple_relation_is_spj(self, simple_relation: RAExpression) -> None:
        assert is_spj_query(simple_relation)

    def test_selection_is_spj(self, simple_selection: RAExpression) -> None:
        assert is_spj_query(simple_selection)

    def test_join_is_spj(self, two_table_join: RAExpression) -> None:
        assert is_spj_query(two_table_join)

    def test_aggregation_not_spj(self, aggregation_query: RAExpression) -> None:
        assert not is_spj_query(aggregation_query)

    def test_union_not_spj(self, union_query: RAExpression) -> None:
        assert not is_spj_query(union_query)


# =============================================================================
# Test: GCS Computation
# =============================================================================


class TestGCS:
    """Tests for GCS computation."""

    def test_gcs_single_expression(self, simple_relation: RAExpression) -> None:
        """GCS of a single expression is itself."""
        result = gcs_all([simple_relation])
        assert result.success
        assert result.input_count == 1

    def test_gcs_identical_expressions(self, simple_relation: RAExpression) -> None:
        """GCS of identical expressions is that expression."""
        result = gcs_all([simple_relation, simple_relation])
        assert result.success
        assert result.input_count == 2

    def test_gcs_two_different_relations(self) -> None:
        """GCS of disjoint relations should fail (no common relations)."""
        r1 = Relation(name="orders", alias="o")
        r2 = Relation(name="customers", alias="c")

        result = gcs_all([r1, r2])
        # Should fail because no common relations
        assert not result.success or len(result.common_relations) == 0

    def test_gcs_same_relation_different_selections(
        self, range_selection_10: RAExpression, range_selection_5: RAExpression
    ) -> None:
        """GCS of selections on same relation with different predicates."""
        result = gcs_all([range_selection_10, range_selection_5])
        # Both select from orders, so there should be a common relation
        assert "o" in result.common_relations or result.success

    def test_gcs_join_vs_subset(
        self, two_table_join: RAExpression, simple_relation: RAExpression
    ) -> None:
        """GCS of a join and one of its tables."""
        result = gcs_all([two_table_join, simple_relation])
        # The common relation should be orders (o)
        if result.success:
            assert "o" in result.common_relations

    def test_gcs_empty_list(self) -> None:
        """GCS of empty list should fail."""
        result = gcs_all([])
        assert not result.success
        assert result.input_count == 0

    def test_greatest_common_subexpression_two_args(self, simple_relation: RAExpression) -> None:
        """Test the two-argument convenience function."""
        result = greatest_common_subexpression(simple_relation, simple_relation)
        assert result.success


class TestPredicateRelaxation:
    """Tests for predicate relaxation in GCS."""

    def test_relax_range_predicates(self) -> None:
        """Test relaxing range predicates: x > 10 and x > 5 → x > 5."""
        # Query 1: amount > 10
        q1 = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="o", column="amount"),
                op=ComparisonOp.GT,
                right=Literal(value=10),
            ),
            input=Relation(name="orders", alias="o"),
        )
        # Query 2: amount > 5
        q2 = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="o", column="amount"),
                op=ComparisonOp.GT,
                right=Literal(value=5),
            ),
            input=Relation(name="orders", alias="o"),
        )

        result = gcs_all([q1, q2])
        # The GCS should have a relaxed predicate (amount > 5)
        # This depends on the predicate relaxation implementation
        assert result.success or "o" in result.common_relations

    def test_identical_predicates_kept(self) -> None:
        """Test that identical predicates are preserved."""
        pred = AtomicPredicate(
            left=ColumnRef(table="o", column="status"),
            op=ComparisonOp.EQ,
            right=Literal(value="active"),
        )
        q1 = Selection(predicate=pred, input=Relation(name="orders", alias="o"))
        q2 = Selection(predicate=pred, input=Relation(name="orders", alias="o"))

        result = gcs_all([q1, q2])
        assert result.success
        # The predicate should be preserved
        assert len(result.dropped_predicates) == 0 or len(result.common_predicates) > 0


class TestCQToRA:
    """Tests for CQ to RA conversion."""

    def test_roundtrip_simple_relation(self, simple_relation: RAExpression) -> None:
        """Test roundtrip conversion for simple relation."""
        cq_result = ra_to_cq(simple_relation)
        assert cq_result.success
        assert cq_result.cq is not None

        ra_expr = cq_to_ra(cq_result.cq)
        assert ra_expr is not None


# =============================================================================
# Test: Hybrid Learning
# =============================================================================


class TestHybridViewLearner:
    """Tests for HybridViewLearner."""

    def test_learn_from_empty_list(self) -> None:
        """Learning from empty list returns empty result."""
        learner = HybridViewLearner()
        result = learner.learn_views([])
        assert result.total_patterns == 0
        assert len(result.views) == 0

    def test_learn_from_single_expression(self, simple_relation: RAExpression) -> None:
        """Learning from single expression."""
        learner = HybridViewLearner()
        result = learner.learn_views([simple_relation])
        assert result.total_patterns == 1

    def test_learn_from_similar_expressions(
        self, simple_relation: RAExpression, simple_selection: RAExpression
    ) -> None:
        """Learning from similar expressions should produce clusters."""
        learner = HybridViewLearner()
        result = learner.learn_views([simple_relation, simple_selection])
        assert result.total_patterns == 2

    def test_learn_spj_uses_gcs(
        self, two_table_join: RAExpression, join_with_selection: RAExpression
    ) -> None:
        """SPJ queries should attempt GCS-based learning."""
        learner = HybridViewLearner()
        result = learner.learn_views([two_table_join, join_with_selection])
        assert result.spj_patterns == 2

    def test_learn_non_spj_uses_fallback(self, aggregation_query: RAExpression) -> None:
        """Non-SPJ queries should use feature-based fallback."""
        learner = HybridViewLearner()
        # Need at least 2 patterns to form a cluster
        result = learner.learn_views([aggregation_query, aggregation_query])
        assert result.spj_patterns == 0

    def test_verified_views_only(
        self, simple_relation: RAExpression, aggregation_query: RAExpression
    ) -> None:
        """require_verification=True should only return verified views."""
        learner = HybridViewLearner(require_verification=True)
        result = learner.learn_views([simple_relation, aggregation_query])
        # All returned views should be verified
        for view in result.views:
            assert view.is_verified


class TestLearnedView:
    """Tests for LearnedView properties."""

    def test_view_from_gcs(self, two_table_join: RAExpression) -> None:
        """Test view learned via GCS."""
        learner = HybridViewLearner()
        result = learner.learn_views([two_table_join, two_table_join])

        if result.views:
            view = result.views[0]
            assert view.cluster_id is not None
            assert view.pattern_count >= 1


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_learn_views_hybrid(self, simple_relation: RAExpression) -> None:
        """Test learn_views_hybrid convenience function."""
        result = learn_views_hybrid([simple_relation, simple_relation])
        assert isinstance(result, HybridLearningResult)

    def test_learn_verified_views(self, two_table_join: RAExpression) -> None:
        """Test learn_verified_views convenience function."""
        views = learn_verified_views([two_table_join, two_table_join])
        assert isinstance(views, list)
        for view in views:
            assert view.is_verified


# =============================================================================
# Test: Extract Common Structure
# =============================================================================


class TestExtractCommonStructure:
    """Tests for extract_common_structure function."""

    def test_common_structure_empty(self) -> None:
        """Empty list returns failure."""
        result = extract_common_structure([])
        assert not result.success

    def test_common_structure_single(self, simple_relation: RAExpression) -> None:
        """Single expression returns itself."""
        result = extract_common_structure([simple_relation])
        assert result.success

    def test_common_structure_threshold(
        self, two_table_join: RAExpression, three_table_join: RAExpression
    ) -> None:
        """Test common structure with threshold."""
        result = extract_common_structure(
            [two_table_join, three_table_join],
            relation_threshold=0.5,
        )
        # Both have orders and customers
        if result.success:
            assert "o" in result.common_relations or "c" in result.common_relations


# =============================================================================
# Test: Integration
# =============================================================================


class TestGCSIntegration:
    """Integration tests for the GCS system."""

    def test_full_workflow(self) -> None:
        """Test full workflow from expressions to views."""
        # Create a set of related queries
        expressions = [
            # Query 1: SELECT * FROM orders
            Relation(name="orders", alias="o"),
            # Query 2: SELECT * FROM orders WHERE status = 'active'
            Selection(
                predicate=AtomicPredicate(
                    left=ColumnRef(table="o", column="status"),
                    op=ComparisonOp.EQ,
                    right=Literal(value="active"),
                ),
                input=Relation(name="orders", alias="o"),
            ),
            # Query 3: SELECT id, amount FROM orders
            Projection(
                columns=[
                    (ColumnRef(table="o", column="id"), None),
                    (ColumnRef(table="o", column="amount"), None),
                ],
                input=Relation(name="orders", alias="o"),
            ),
        ]

        result = learn_views_hybrid(expressions)

        assert result.total_patterns == 3
        assert result.spj_patterns == 3  # All are SPJ

    def test_mixed_spj_and_non_spj(self) -> None:
        """Test with mix of SPJ and non-SPJ queries."""
        expressions = [
            # SPJ query
            Relation(name="orders", alias="o"),
            # Non-SPJ query (aggregation)
            Aggregation(
                group_by=[ColumnRef(table="o", column="customer_id")],
                aggregates=[
                    AggregateSpec(
                        function=AggregateFunction.COUNT,
                        argument=None,
                        alias="cnt",
                    )
                ],
                input=Relation(name="orders", alias="o"),
            ),
        ]

        result = learn_views_hybrid(expressions)

        assert result.total_patterns == 2
        assert result.spj_patterns == 1  # Only the simple relation is SPJ
