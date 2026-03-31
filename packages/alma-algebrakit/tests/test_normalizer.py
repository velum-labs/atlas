"""Tests for the relational algebra normalizer.

Covers:
- Selection merging (cascading selections -> single selection with conjunction)
- Join flattening (for inner joins)
- Join order canonicalization (alphabetical ordering)
- Join graph extraction
- Predicate extraction
- Top-level operator peeling and re-wrapping
"""

from __future__ import annotations

import pytest

from alma_algebrakit.models.algebra import (
    Aggregation,
    AtomicPredicate,
    ColumnRef,
    ComparisonOp,
    CompoundPredicate,
    CTEDefinition,
    Difference,
    Intersect,
    Join,
    JoinType,
    Limit,
    Literal,
    LogicalOp,
    Projection,
    Relation,
    Selection,
    Sort,
    SortSpec,
    Union,
    WithExpression,
)
from alma_algebrakit.normalizer import (
    JoinEdge,
    RANormalizer,
    extract_detailed_join_graph,
    extract_top_level_operators,
    wrap_with_operators,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def normalizer() -> RANormalizer:
    """Create a normalizer with all transformations enabled."""
    return RANormalizer(
        merge_selections=True,
        flatten_joins=True,
        canonicalize_join_order=True,
    )


@pytest.fixture
def orders_table() -> Relation:
    """Create an 'orders' relation."""
    return Relation(name="orders", alias="o")


@pytest.fixture
def customers_table() -> Relation:
    """Create a 'customers' relation."""
    return Relation(name="customers", alias="c")


@pytest.fixture
def products_table() -> Relation:
    """Create a 'products' relation."""
    return Relation(name="products", alias="p")


def make_comparison(
    table: str,
    column: str,
    op: ComparisonOp,
    value: str | int | float,
) -> AtomicPredicate:
    """Helper to create a comparison predicate."""
    return AtomicPredicate(
        left=ColumnRef(table=table, column=column),
        op=op,
        right=Literal(value=value),
    )


def make_eq_condition(
    left_table: str,
    left_col: str,
    right_table: str,
    right_col: str,
) -> AtomicPredicate:
    """Helper to create an equality join condition."""
    return AtomicPredicate(
        left=ColumnRef(table=left_table, column=left_col),
        op=ComparisonOp.EQ,
        right=ColumnRef(table=right_table, column=right_col),
    )


# =============================================================================
# Selection Merging Tests
# =============================================================================


class TestSelectionMerging:
    """Tests for merging cascading selections."""

    def test_merge_two_selections(self, normalizer: RANormalizer) -> None:
        """Test merging two cascading selections into one."""
        # σ_status='active'(σ_amount>100(orders))
        inner_sel = Selection(
            predicate=make_comparison("o", "amount", ComparisonOp.GT, 100),
            input=Relation(name="orders", alias="o"),
        )
        outer_sel = Selection(
            predicate=make_comparison("o", "status", ComparisonOp.EQ, "active"),
            input=inner_sel,
        )

        result = normalizer.normalize(outer_sel)

        # Should be single selection with AND predicate
        assert isinstance(result, Selection)
        assert isinstance(result.predicate, CompoundPredicate)
        assert result.predicate.op == LogicalOp.AND
        assert len(result.predicate.operands) == 2
        assert isinstance(result.input, Relation)

    def test_merge_three_selections(self, normalizer: RANormalizer) -> None:
        """Test merging three cascading selections."""
        # σ_p1(σ_p2(σ_p3(orders)))
        base = Relation(name="orders")
        sel1 = Selection(
            predicate=make_comparison("orders", "a", ComparisonOp.GT, 1),
            input=base,
        )
        sel2 = Selection(
            predicate=make_comparison("orders", "b", ComparisonOp.GT, 2),
            input=sel1,
        )
        sel3 = Selection(
            predicate=make_comparison("orders", "c", ComparisonOp.GT, 3),
            input=sel2,
        )

        result = normalizer.normalize(sel3)

        # Should be single selection
        assert isinstance(result, Selection)
        # Should have merged predicates
        assert isinstance(result.input, Relation)

    def test_no_merge_with_intervening_projection(self, normalizer: RANormalizer) -> None:
        """Test that selections separated by projection are not merged."""
        # σ_p1(π(σ_p2(orders)))
        base = Relation(name="orders")
        inner_sel = Selection(
            predicate=make_comparison("orders", "a", ComparisonOp.GT, 1),
            input=base,
        )
        proj = Projection(
            columns=[(ColumnRef(column="a"), None)],
            input=inner_sel,
        )
        outer_sel = Selection(
            predicate=make_comparison("orders", "a", ComparisonOp.LT, 10),
            input=proj,
        )

        result = normalizer.normalize(outer_sel)

        # Outer selection should remain
        assert isinstance(result, Selection)
        # With projection inside
        assert isinstance(result.input, Projection)
        # Inner selection should exist
        assert isinstance(result.input.input, Selection)

    def test_selection_merge_disabled(self) -> None:
        """Test that selection merging can be disabled."""
        normalizer = RANormalizer(merge_selections=False)

        inner_sel = Selection(
            predicate=make_comparison("orders", "a", ComparisonOp.GT, 1),
            input=Relation(name="orders"),
        )
        outer_sel = Selection(
            predicate=make_comparison("orders", "b", ComparisonOp.GT, 2),
            input=inner_sel,
        )

        result = normalizer.normalize(outer_sel)

        # Should remain as two separate selections
        assert isinstance(result, Selection)
        assert isinstance(result.input, Selection)


# =============================================================================
# Join Flattening Tests
# =============================================================================


class TestJoinFlattening:
    """Tests for flattening nested joins."""

    def test_flatten_nested_inner_joins(self, normalizer: RANormalizer) -> None:
        """Test flattening nested inner joins."""
        # (orders ⋈ customers) ⋈ products
        orders = Relation(name="orders", alias="o")
        customers = Relation(name="customers", alias="c")
        products = Relation(name="products", alias="p")

        inner_join = Join(
            left=orders,
            right=customers,
            join_type=JoinType.INNER,
            condition=make_eq_condition("o", "customer_id", "c", "id"),
        )
        outer_join = Join(
            left=inner_join,
            right=products,
            join_type=JoinType.INNER,
            condition=make_eq_condition("o", "product_id", "p", "id"),
        )

        result = normalizer.normalize(outer_join)

        # Inner-join flattening preserves semantics by lifting ON predicates
        # into a Selection over a left-deep join tree.
        assert isinstance(result, Selection)
        assert isinstance(result.input, Join)
        assert isinstance(result.predicate, CompoundPredicate)
        assert result.predicate.op == LogicalOp.AND
        assert len(result.predicate.operands) == 2

    def test_do_not_flatten_outer_joins(self, normalizer: RANormalizer) -> None:
        """Test that outer joins are not reordered."""
        # orders LEFT JOIN customers
        orders = Relation(name="orders", alias="o")
        customers = Relation(name="customers", alias="c")

        left_join = Join(
            left=orders,
            right=customers,
            join_type=JoinType.LEFT,
            condition=make_eq_condition("o", "customer_id", "c", "id"),
        )

        result = normalizer.normalize(left_join)

        # Should maintain LEFT join type
        assert isinstance(result, Join)
        assert result.join_type == JoinType.LEFT


# =============================================================================
# Join Order Canonicalization Tests
# =============================================================================


class TestJoinOrderCanonicalization:
    """Tests for canonicalizing join order."""

    def test_reorder_inner_join_alphabetically(self, normalizer: RANormalizer) -> None:
        """Test that inner joins are reordered alphabetically."""
        # customers ⋈ orders should become orders ⋈ customers (c < o -> no change)
        customers = Relation(name="customers", alias="c")
        orders = Relation(name="orders", alias="o")

        join = Join(
            left=orders,  # 'o' > 'c', so should swap
            right=customers,
            join_type=JoinType.INNER,
            condition=make_eq_condition("o", "customer_id", "c", "id"),
        )

        result = normalizer.normalize(join)

        if isinstance(result, Selection):
            assert isinstance(result.input, Join)
            result = result.input

        assert isinstance(result, Join)
        # 'c' < 'o', so customers should be on left
        left_tables = result.left.referenced_tables()
        result.right.referenced_tables()
        assert "c" in left_tables or "customers" in left_tables

    def test_do_not_reorder_left_join(self, normalizer: RANormalizer) -> None:
        """Test that LEFT joins are not reordered."""
        orders = Relation(name="orders", alias="o")
        customers = Relation(name="customers", alias="c")

        join = Join(
            left=orders,
            right=customers,
            join_type=JoinType.LEFT,
            condition=make_eq_condition("o", "customer_id", "c", "id"),
        )

        result = normalizer.normalize(join)

        # Should keep original order for LEFT join
        assert isinstance(result, Join)
        assert result.join_type == JoinType.LEFT
        # orders should still be on the left
        left_tables = result.left.referenced_tables()
        assert "o" in left_tables or "orders" in left_tables


# =============================================================================
# Join Graph Extraction Tests
# =============================================================================


class TestJoinGraphExtraction:
    """Tests for extracting join graphs."""

    def test_extract_simple_join_graph(self, normalizer: RANormalizer) -> None:
        """Test extracting a simple join graph."""
        orders = Relation(name="orders")
        customers = Relation(name="customers")

        join = Join(
            left=orders,
            right=customers,
            join_type=JoinType.INNER,
            condition=make_eq_condition("orders", "customer_id", "customers", "id"),
        )

        edges = normalizer.extract_join_graph(join)

        assert len(edges) == 1
        left, right, cond = edges[0]
        assert "orders" in left or "customers" in left
        assert cond is not None

    def test_extract_multi_table_join_graph(self, normalizer: RANormalizer) -> None:
        """Test extracting a multi-table join graph."""
        orders = Relation(name="orders")
        customers = Relation(name="customers")
        products = Relation(name="products")

        inner_join = Join(
            left=orders,
            right=customers,
            join_type=JoinType.INNER,
            condition=make_eq_condition("orders", "customer_id", "customers", "id"),
        )
        outer_join = Join(
            left=inner_join,
            right=products,
            join_type=JoinType.INNER,
            condition=make_eq_condition("orders", "product_id", "products", "id"),
        )

        edges = normalizer.extract_join_graph(outer_join)

        assert len(edges) == 2

    def test_extract_detailed_join_graph(self) -> None:
        """Test extracting detailed join graph with JoinEdge objects."""
        orders = Relation(name="orders", alias="o")
        customers = Relation(name="customers", alias="c")

        join = Join(
            left=orders,
            right=customers,
            join_type=JoinType.LEFT,
            condition=make_eq_condition("o", "customer_id", "c", "id"),
        )

        edges = extract_detailed_join_graph(join)

        assert len(edges) == 1
        edge = edges[0]
        assert isinstance(edge, JoinEdge)
        assert edge.join_type == JoinType.LEFT
        assert len(edge.left_rels) > 0
        assert len(edge.right_rels) > 0


# =============================================================================
# Predicate Extraction Tests
# =============================================================================


class TestPredicateExtraction:
    """Tests for extracting predicates from expressions."""

    def test_extract_single_selection_predicate(self, normalizer: RANormalizer) -> None:
        """Test extracting a predicate from a single selection."""
        pred = make_comparison("orders", "amount", ComparisonOp.GT, 100)
        sel = Selection(
            predicate=pred,
            input=Relation(name="orders"),
        )

        predicates = normalizer.extract_predicates(sel)

        assert len(predicates) == 1
        assert predicates[0].fingerprint() == pred.fingerprint()

    def test_extract_multiple_predicates(self, normalizer: RANormalizer) -> None:
        """Test extracting predicates from nested selections."""
        orders = Relation(name="orders")
        sel1 = Selection(
            predicate=make_comparison("orders", "a", ComparisonOp.GT, 1),
            input=orders,
        )
        sel2 = Selection(
            predicate=make_comparison("orders", "b", ComparisonOp.LT, 10),
            input=sel1,
        )

        predicates = normalizer.extract_predicates(sel2)

        assert len(predicates) == 2

    def test_extract_join_condition(self, normalizer: RANormalizer) -> None:
        """Test extracting join conditions as predicates."""
        cond = make_eq_condition("o", "customer_id", "c", "id")
        join = Join(
            left=Relation(name="orders", alias="o"),
            right=Relation(name="customers", alias="c"),
            join_type=JoinType.INNER,
            condition=cond,
        )

        predicates = normalizer.extract_predicates(join)

        assert len(predicates) == 1
        assert predicates[0].fingerprint() == cond.fingerprint()

    def test_extract_having_predicate(self, normalizer: RANormalizer) -> None:
        """Test extracting HAVING clause predicate."""
        having = make_comparison("orders", "total", ComparisonOp.GT, 1000)
        agg = Aggregation(
            group_by=[ColumnRef(column="customer_id")],
            aggregates=[],
            input=Relation(name="orders"),
            having=having,
        )

        predicates = normalizer.extract_predicates(agg)

        assert len(predicates) == 1
        assert predicates[0].fingerprint() == having.fingerprint()


# =============================================================================
# Top-Level Operator Extraction Tests
# =============================================================================


class TestTopLevelOperatorExtraction:
    """Tests for peeling and re-wrapping top-level operators."""

    def test_extract_sort_and_limit(self) -> None:
        """Test extracting Sort and Limit operators."""
        base = Projection(
            columns=[(ColumnRef(column="id"), None)],
            input=Relation(name="orders"),
        )
        sorted_expr = Sort(
            order_by=[SortSpec(expression=ColumnRef(column="id"))],
            input=base,
        )
        limited = Limit(limit=10, offset=5, input=sorted_expr)

        ops = extract_top_level_operators(limited)

        assert ops.limit is not None
        assert ops.limit.limit == 10
        assert ops.limit.offset == 5
        assert ops.sort is not None
        assert ops.aggregation is None
        assert ops.core == base

    def test_extract_aggregation(self) -> None:
        """Test extracting Aggregation operator."""
        base = Relation(name="orders")
        agg = Aggregation(
            group_by=[ColumnRef(column="customer_id")],
            aggregates=[],
            input=base,
        )

        ops = extract_top_level_operators(agg)

        assert ops.aggregation is not None
        assert ops.sort is None
        assert ops.limit is None
        assert ops.core == base

    def test_wrap_with_operators(self) -> None:
        """Test re-wrapping a core expression with operators."""
        # Extract operators
        base = Relation(name="orders")
        sorted_expr = Sort(
            order_by=[SortSpec(expression=ColumnRef(column="id"))],
            input=base,
        )
        limited = Limit(limit=10, input=sorted_expr)

        ops = extract_top_level_operators(limited)

        # Re-wrap with new core
        new_core = Relation(name="v_orders")
        result = wrap_with_operators(new_core, ops)

        # Should have Limit(Sort(new_core))
        assert isinstance(result, Limit)
        assert result.limit == 10
        assert isinstance(result.input, Sort)
        assert isinstance(result.input.input, Relation)
        assert result.input.input.name == "v_orders"

    def test_wrap_with_column_rewrites(self) -> None:
        """Test re-wrapping with column rewrite map."""
        base = Relation(name="orders", alias="o")
        sorted_expr = Sort(
            order_by=[SortSpec(expression=ColumnRef(table="o", column="id"))],
            input=base,
        )

        ops = extract_top_level_operators(sorted_expr)

        # Re-wrap with column rewrite
        new_core = Relation(name="v_orders", alias="v")
        result = wrap_with_operators(
            new_core,
            ops,
            column_rewrite_map={"o.id": "v.id"},
        )

        # Sort expression should use new column reference
        assert isinstance(result, Sort)
        sort_col = result.order_by[0].expression
        assert isinstance(sort_col, ColumnRef)
        assert sort_col.table == "v"
        assert sort_col.column == "id"


# =============================================================================
# Predicate Implication Tests
# =============================================================================


class TestPredicateImplication:
    """Tests for predicate implication checking."""

    def test_same_predicate_implies_itself(self, normalizer: RANormalizer) -> None:
        """Test that a predicate implies itself."""
        pred = make_comparison("orders", "amount", ComparisonOp.GT, 100)

        assert normalizer.predicate_implies(pred, pred) is True

    def test_conjunction_implies_conjunct(self, normalizer: RANormalizer) -> None:
        """Test that a conjunction implies each of its conjuncts."""
        p1 = make_comparison("orders", "amount", ComparisonOp.GT, 100)
        p2 = make_comparison("orders", "status", ComparisonOp.EQ, "active")

        conjunction = CompoundPredicate(op=LogicalOp.AND, operands=[p1, p2])

        # Conjunction implies p1
        assert normalizer.predicate_implies(conjunction, p1) is True
        # Conjunction implies p2
        assert normalizer.predicate_implies(conjunction, p2) is True

    def test_implies_disjunction_member(self, normalizer: RANormalizer) -> None:
        """Test that a predicate implies a disjunction containing it."""
        p1 = make_comparison("orders", "amount", ComparisonOp.GT, 100)
        p2 = make_comparison("orders", "status", ComparisonOp.EQ, "active")

        disjunction = CompoundPredicate(op=LogicalOp.OR, operands=[p1, p2])

        # p1 implies (p1 OR p2)
        assert normalizer.predicate_implies(p1, disjunction) is True

    def test_different_columns_no_implication(self, normalizer: RANormalizer) -> None:
        """Test that predicates on different columns don't imply each other."""
        p1 = make_comparison("orders", "amount", ComparisonOp.GT, 100)
        p2 = make_comparison("orders", "quantity", ComparisonOp.GT, 100)

        assert normalizer.predicate_implies(p1, p2) is False


# =============================================================================
# Set Operations Tests
# =============================================================================


class TestSetOperations:
    """Tests for normalizing set operations."""

    def test_normalize_union(self, normalizer: RANormalizer) -> None:
        """Test normalizing UNION with nested selections."""
        left_sel = Selection(
            predicate=make_comparison("orders", "a", ComparisonOp.GT, 1),
            input=Selection(
                predicate=make_comparison("orders", "b", ComparisonOp.LT, 10),
                input=Relation(name="orders"),
            ),
        )
        right_sel = Selection(
            predicate=make_comparison("customers", "c", ComparisonOp.EQ, "active"),
            input=Selection(
                predicate=make_comparison("customers", "d", ComparisonOp.NE, "deleted"),
                input=Relation(name="customers"),
            ),
        )
        union = Union(left=left_sel, right=right_sel, all=True)

        result = normalizer.normalize(union)

        # Both branches should have merged selections
        assert isinstance(result, Union)
        assert isinstance(result.left, Selection)
        assert isinstance(result.left.predicate, CompoundPredicate)
        assert isinstance(result.right, Selection)
        assert isinstance(result.right.predicate, CompoundPredicate)

    def test_normalize_intersect(self, normalizer: RANormalizer) -> None:
        """Test normalizing INTERSECT."""
        intersect = Intersect(
            left=Relation(name="a"),
            right=Relation(name="b"),
            all=False,
        )

        result = normalizer.normalize(intersect)

        assert isinstance(result, Intersect)

    def test_normalize_difference(self, normalizer: RANormalizer) -> None:
        """Test normalizing EXCEPT (difference)."""
        diff = Difference(
            left=Relation(name="a"),
            right=Relation(name="b"),
        )

        result = normalizer.normalize(diff)

        assert isinstance(result, Difference)


# =============================================================================
# CTE Tests
# =============================================================================


class TestCTENormalization:
    """Tests for normalizing CTEs (WITH expressions)."""

    def test_normalize_cte_queries(self, normalizer: RANormalizer) -> None:
        """Test that CTE queries are normalized."""
        cte_query = Selection(
            predicate=make_comparison("orders", "a", ComparisonOp.GT, 1),
            input=Selection(
                predicate=make_comparison("orders", "b", ComparisonOp.LT, 10),
                input=Relation(name="orders"),
            ),
        )
        cte = CTEDefinition(
            name="recent_orders",
            query=cte_query,
        )
        with_expr = WithExpression(
            ctes=[cte],
            main_query=Relation(name="recent_orders"),
        )

        result = normalizer.normalize(with_expr)

        # CTE query should have merged selections
        assert isinstance(result, WithExpression)
        assert len(result.ctes) == 1
        cte_result = result.ctes[0].query
        assert isinstance(cte_result, Selection)
        # Should be merged into single selection
        assert isinstance(cte_result.predicate, CompoundPredicate)
