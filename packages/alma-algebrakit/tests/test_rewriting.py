"""Tests for the rewriting module (predicates, columns, joins, equivalence)."""

from alma_algebrakit.models.algebra import (
    AggregateFunction,
    AggregateSpec,
    Aggregation,
    AtomicPredicate,
    ColumnRef,
    ComparisonOp,
    CompoundPredicate,
    Join,
    JoinType,
    Literal,
    LogicalOp,
    Projection,
    Relation,
    RelationRef,
    Selection,
)
from alma_algebrakit.rewriting.columns import (
    build_column_rewrite_map,
    check_column_availability,
    collect_column_references,
)
from alma_algebrakit.rewriting.equivalence import (
    EquivalenceChecker,
    EquivalenceLevel,
)
from alma_algebrakit.rewriting.joins import check_outer_join_safety
from alma_algebrakit.rewriting.predicates import (
    classify_predicates,
    extract_selection_predicates,
)

# =============================================================================
# Tests for extract_selection_predicates
# =============================================================================


class TestExtractSelectionPredicates:
    """Tests for extract_selection_predicates function."""

    def test_extract_from_simple_selection(self) -> None:
        """Test extraction from a simple selection."""
        pred = AtomicPredicate(
            left=ColumnRef(column="x"),
            op=ComparisonOp.EQ,
            right=Literal(value=5),
        )
        relation = Relation(name="t", alias="t", columns=["x", "y"])
        selection = Selection(predicate=pred, input=relation)

        preds = extract_selection_predicates(selection)
        assert len(preds) == 1
        assert preds[0].fingerprint() == pred.fingerprint()

    def test_extract_from_nested_selections(self) -> None:
        """Test extraction from nested selections."""
        pred1 = AtomicPredicate(
            left=ColumnRef(column="x"),
            op=ComparisonOp.EQ,
            right=Literal(value=5),
        )
        pred2 = AtomicPredicate(
            left=ColumnRef(column="y"),
            op=ComparisonOp.GT,
            right=Literal(value=10),
        )
        relation = Relation(name="t", alias="t", columns=["x", "y"])
        inner = Selection(predicate=pred2, input=relation)
        outer = Selection(predicate=pred1, input=inner)

        preds = extract_selection_predicates(outer)
        assert len(preds) == 2
        fingerprints = {p.fingerprint() for p in preds}
        assert pred1.fingerprint() in fingerprints
        assert pred2.fingerprint() in fingerprints

    def test_extract_ignores_join_conditions(self) -> None:
        """Test that join ON conditions are not extracted as selection predicates."""
        relation1 = Relation(name="t1", alias="t1", columns=["id", "x"])
        relation2 = Relation(name="t2", alias="t2", columns=["id", "y"])
        join_cond = AtomicPredicate(
            left=ColumnRef(table="t1", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="t2", column="id"),
        )
        join = Join(
            left=relation1,
            right=relation2,
            join_type=JoinType.INNER,
            condition=join_cond,
        )

        # Join condition should NOT be extracted
        preds = extract_selection_predicates(join)
        assert len(preds) == 0

    def test_extract_through_projection(self) -> None:
        """Test extraction works through projections."""
        pred = AtomicPredicate(
            left=ColumnRef(column="x"),
            op=ComparisonOp.EQ,
            right=Literal(value=5),
        )
        relation = Relation(name="t", alias="t", columns=["x", "y"])
        selection = Selection(predicate=pred, input=relation)
        projection = Projection(
            columns=[(ColumnRef(column="x"), None)],
            input=selection,
        )

        preds = extract_selection_predicates(projection)
        assert len(preds) == 1


# =============================================================================
# Tests for classify_predicates
# =============================================================================


class TestClassifyPredicates:
    """Tests for classify_predicates function."""

    def test_exact_match_drops_predicate(self) -> None:
        """Test that predicates matching view predicates are dropped."""
        query_pred = AtomicPredicate(
            left=ColumnRef(table="t", column="x"),
            op=ComparisonOp.EQ,
            right=Literal(value=5),
        )
        view_pred = AtomicPredicate(
            left=ColumnRef(table="t", column="x"),
            op=ComparisonOp.EQ,
            right=Literal(value=5),
        )

        classification = classify_predicates(
            query_predicates=[query_pred],
            view_predicates=[view_pred],
            covered_aliases={"t"},
            remaining_aliases=set(),
        )

        assert len(classification.drop) == 1
        assert len(classification.keep) == 0

    def test_unmatched_covered_predicate_kept(self) -> None:
        """Test that predicates on covered tables that don't match are kept."""
        query_pred = AtomicPredicate(
            left=ColumnRef(table="t", column="x"),
            op=ComparisonOp.EQ,
            right=Literal(value=5),
        )
        view_pred = AtomicPredicate(
            left=ColumnRef(table="t", column="x"),
            op=ComparisonOp.GT,  # Different operator
            right=Literal(value=3),
        )

        classification = classify_predicates(
            query_predicates=[query_pred],
            view_predicates=[view_pred],
            covered_aliases={"t"},
            remaining_aliases=set(),
        )

        # Predicate should be kept since it doesn't exactly match
        assert len(classification.keep) == 1
        assert len(classification.drop) == 0

    def test_remaining_table_predicate_kept(self) -> None:
        """Test that predicates referencing remaining tables are kept."""
        query_pred = AtomicPredicate(
            left=ColumnRef(table="r", column="y"),  # 'r' is remaining
            op=ComparisonOp.EQ,
            right=Literal(value=10),
        )

        classification = classify_predicates(
            query_predicates=[query_pred],
            view_predicates=[],
            covered_aliases={"t"},
            remaining_aliases={"r"},
        )

        assert len(classification.keep) == 1
        assert len(classification.drop) == 0

    def test_mixed_predicate_kept(self) -> None:
        """Test that predicates referencing both covered and remaining are kept."""
        # A join-like predicate that spans covered and remaining
        query_pred = AtomicPredicate(
            left=ColumnRef(table="t", column="id"),  # covered
            op=ComparisonOp.EQ,
            right=ColumnRef(table="r", column="t_id"),  # remaining
        )

        classification = classify_predicates(
            query_predicates=[query_pred],
            view_predicates=[],
            covered_aliases={"t"},
            remaining_aliases={"r"},
        )

        assert len(classification.keep) == 1
        assert "t.id" in classification.required_covered_cols


# =============================================================================
# Tests for OR/NOT predicates (regression tests for fixed bugs)
# =============================================================================


class TestORNOTPredicates:
    """Tests for OR and NOT predicate handling (regression tests)."""

    def test_or_predicate_not_equijoin(self) -> None:
        """Test that joins with OR predicates are not considered equi-joins."""
        relation1 = Relation(name="t1", alias="t1", columns=["id", "x"])
        relation2 = Relation(name="t2", alias="t2", columns=["id", "y"])

        # OR condition: t1.id = t2.id OR t1.x = t2.y
        or_condition = CompoundPredicate(
            op=LogicalOp.OR,
            operands=[
                AtomicPredicate(
                    left=ColumnRef(table="t1", column="id"),
                    op=ComparisonOp.EQ,
                    right=ColumnRef(table="t2", column="id"),
                ),
                AtomicPredicate(
                    left=ColumnRef(table="t1", column="x"),
                    op=ComparisonOp.EQ,
                    right=ColumnRef(table="t2", column="y"),
                ),
            ],
        )

        join = Join(
            left=relation1,
            right=relation2,
            join_type=JoinType.INNER,
            condition=or_condition,
        )

        # OR joins should NOT be equi-joins (fixed bug)
        assert not join.is_inner_equijoin()

    def test_not_predicate_not_equijoin(self) -> None:
        """Test that joins with NOT predicates are not considered equi-joins."""
        relation1 = Relation(name="t1", alias="t1", columns=["id"])
        relation2 = Relation(name="t2", alias="t2", columns=["id"])

        # NOT condition
        not_condition = CompoundPredicate(
            op=LogicalOp.NOT,
            operands=[
                AtomicPredicate(
                    left=ColumnRef(table="t1", column="id"),
                    op=ComparisonOp.EQ,
                    right=ColumnRef(table="t2", column="id"),
                ),
            ],
        )

        join = Join(
            left=relation1,
            right=relation2,
            join_type=JoinType.INNER,
            condition=not_condition,
        )

        # NOT joins should NOT be equi-joins (fixed bug)
        assert not join.is_inner_equijoin()

    def test_and_equijoin_still_works(self) -> None:
        """Test that AND equi-joins are still correctly identified."""
        relation1 = Relation(name="t1", alias="t1", columns=["id", "x"])
        relation2 = Relation(name="t2", alias="t2", columns=["id", "x"])

        # AND condition: t1.id = t2.id AND t1.x = t2.x
        and_condition = CompoundPredicate(
            op=LogicalOp.AND,
            operands=[
                AtomicPredicate(
                    left=ColumnRef(table="t1", column="id"),
                    op=ComparisonOp.EQ,
                    right=ColumnRef(table="t2", column="id"),
                ),
                AtomicPredicate(
                    left=ColumnRef(table="t1", column="x"),
                    op=ComparisonOp.EQ,
                    right=ColumnRef(table="t2", column="x"),
                ),
            ],
        )

        join = Join(
            left=relation1,
            right=relation2,
            join_type=JoinType.INNER,
            condition=and_condition,
        )

        # AND equi-joins should still be recognized
        assert join.is_inner_equijoin()


# =============================================================================
# Tests for fingerprint normalization
# =============================================================================


class TestFingerprintNormalization:
    """Tests for predicate fingerprint normalization."""

    def test_and_commutative_fingerprint(self) -> None:
        """Test that (A AND B) and (B AND A) have same fingerprint."""
        pred_a = AtomicPredicate(
            left=ColumnRef(column="a"),
            op=ComparisonOp.EQ,
            right=Literal(value=1),
        )
        pred_b = AtomicPredicate(
            left=ColumnRef(column="b"),
            op=ComparisonOp.EQ,
            right=Literal(value=2),
        )

        and_ab = CompoundPredicate(op=LogicalOp.AND, operands=[pred_a, pred_b])
        and_ba = CompoundPredicate(op=LogicalOp.AND, operands=[pred_b, pred_a])

        assert and_ab.fingerprint() == and_ba.fingerprint()

    def test_or_commutative_fingerprint(self) -> None:
        """Test that (A OR B) and (B OR A) have same fingerprint."""
        pred_a = AtomicPredicate(
            left=ColumnRef(column="a"),
            op=ComparisonOp.EQ,
            right=Literal(value=1),
        )
        pred_b = AtomicPredicate(
            left=ColumnRef(column="b"),
            op=ComparisonOp.EQ,
            right=Literal(value=2),
        )

        or_ab = CompoundPredicate(op=LogicalOp.OR, operands=[pred_a, pred_b])
        or_ba = CompoundPredicate(op=LogicalOp.OR, operands=[pred_b, pred_a])

        assert or_ab.fingerprint() == or_ba.fingerprint()

    def test_not_fingerprint_preserved(self) -> None:
        """Test that NOT predicates have consistent fingerprints."""
        pred_a = AtomicPredicate(
            left=ColumnRef(column="a"),
            op=ComparisonOp.EQ,
            right=Literal(value=1),
        )

        not_a = CompoundPredicate(op=LogicalOp.NOT, operands=[pred_a])

        # NOT should not be affected by sorting (single operand)
        assert "NOT" in not_a.fingerprint()
        assert pred_a.fingerprint() in not_a.fingerprint()


# =============================================================================
# Tests for EquivalenceChecker
# =============================================================================


class TestEquivalenceChecker:
    """Tests for the EquivalenceChecker class."""

    # -------------------------------------------------------------------------
    # Simple Scenarios - Structural Equivalence
    # -------------------------------------------------------------------------

    def test_identical_relation_is_equivalent(self) -> None:
        """Same relation instance should be structurally equivalent."""
        checker = EquivalenceChecker()
        relation = Relation(name="users", alias="u", columns=["id", "name"])

        result = checker.check_structural(relation, relation)

        assert result.equivalent is True
        assert result.level == EquivalenceLevel.STRUCTURAL
        assert result.confidence == 1.0

    def test_same_selection_is_equivalent(self) -> None:
        """sigma_x>5(R) == sigma_x>5(R) should be equivalent."""
        checker = EquivalenceChecker()
        relation = Relation(name="t", alias="t", columns=["x"])
        pred = AtomicPredicate(
            left=ColumnRef(table="t", column="x"),
            op=ComparisonOp.GT,
            right=Literal(value=5),
        )
        sel1 = Selection(predicate=pred, input=relation)
        sel2 = Selection(predicate=pred, input=relation)

        result = checker.check_structural(sel1, sel2)

        assert result.equivalent is True
        assert result.confidence == 1.0

    def test_different_predicates_not_equivalent(self) -> None:
        """sigma_x>5(R) != sigma_x>10(R) should not be equivalent."""
        checker = EquivalenceChecker()
        relation = Relation(name="t", alias="t", columns=["x"])
        pred1 = AtomicPredicate(
            left=ColumnRef(table="t", column="x"),
            op=ComparisonOp.GT,
            right=Literal(value=5),
        )
        pred2 = AtomicPredicate(
            left=ColumnRef(table="t", column="x"),
            op=ComparisonOp.GT,
            right=Literal(value=10),
        )
        sel1 = Selection(predicate=pred1, input=relation)
        sel2 = Selection(predicate=pred2, input=relation)

        result = checker.check_structural(sel1, sel2)

        assert result.equivalent is False
        assert result.confidence == 0.0

    # -------------------------------------------------------------------------
    # Complex Scenarios - Structural Equivalence
    # -------------------------------------------------------------------------

    def test_cascading_selections_equivalent_to_merged(self) -> None:
        """sigma_a(sigma_b(R)) == sigma_a AND b(R) after normalization."""
        checker = EquivalenceChecker()
        relation = Relation(name="t", alias="t", columns=["a", "b"])

        pred_a = AtomicPredicate(
            left=ColumnRef(table="t", column="a"),
            op=ComparisonOp.GT,
            right=Literal(value=5),
        )
        pred_b = AtomicPredicate(
            left=ColumnRef(table="t", column="b"),
            op=ComparisonOp.LT,
            right=Literal(value=10),
        )

        # Cascading: sigma_a(sigma_b(R))
        inner_sel = Selection(predicate=pred_b, input=relation)
        cascading = Selection(predicate=pred_a, input=inner_sel)

        # Merged: sigma_a AND b(R)
        merged_pred = CompoundPredicate(op=LogicalOp.AND, operands=[pred_a, pred_b])
        merged = Selection(predicate=merged_pred, input=relation)

        result = checker.check_structural(cascading, merged)

        # After normalization, they should be equivalent
        assert result.equivalent is True

    def test_commutative_join_order_equivalent(self) -> None:
        """A join B == B join A for inner equi-joins after normalization."""
        checker = EquivalenceChecker()
        users = Relation(name="users", alias="u", columns=["id", "name"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])

        join_cond = AtomicPredicate(
            left=ColumnRef(table="u", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="o", column="user_id"),
        )

        join1 = Join(
            left=users,
            right=orders,
            join_type=JoinType.INNER,
            condition=join_cond,
        )
        join2 = Join(
            left=orders,
            right=users,
            join_type=JoinType.INNER,
            condition=join_cond,
        )

        result = checker.check_join_equivalence(join1, join2)

        # Inner equi-joins should have equivalent join graphs
        assert result.equivalent is True

    def test_associative_join_reordering_not_detected_by_graph(self) -> None:
        """(A join B) join C vs A join (B join C) - join graph extraction is tree-based.

        Note: The join graph extraction is based on tree structure, so differently
        associated joins will have different extracted graphs. This tests that
        the implementation correctly reports them as non-equivalent at the
        structural level (with confidence < 1.0 indicating uncertainty).
        """
        checker = EquivalenceChecker()
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id", "c_id"])
        c = Relation(name="c", alias="c", columns=["id"])

        ab_cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        bc_cond = AtomicPredicate(
            left=ColumnRef(table="b", column="c_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="c", column="id"),
        )

        # (A join B) join C
        ab_join = Join(left=a, right=b, join_type=JoinType.INNER, condition=ab_cond)
        left_assoc = Join(left=ab_join, right=c, join_type=JoinType.INNER, condition=bc_cond)

        # A join (B join C)
        bc_join = Join(left=b, right=c, join_type=JoinType.INNER, condition=bc_cond)
        right_assoc = Join(left=a, right=bc_join, join_type=JoinType.INNER, condition=ab_cond)

        result = checker.check_join_equivalence(left_assoc, right_assoc)

        # Tree-based extraction shows different graphs - implementation limitation
        # Confidence is 0.9 indicating this is a structural check, not semantic
        assert result.confidence == 0.9
        assert result.level == EquivalenceLevel.STRUCTURAL

    # -------------------------------------------------------------------------
    # Complex Scenarios - Containment
    # -------------------------------------------------------------------------

    def test_containment_view_covers_subset_of_query_tables(self) -> None:
        """View with {users} contained in query with {users, orders}."""
        checker = EquivalenceChecker()
        users = Relation(name="users", alias="u", columns=["id", "name"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])

        # Container: users join orders
        join_cond = AtomicPredicate(
            left=ColumnRef(table="u", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="o", column="user_id"),
        )
        container = Join(left=users, right=orders, join_type=JoinType.INNER, condition=join_cond)

        # Contained: just users
        contained = users

        result = checker.check_containment(container, contained)

        assert result.equivalent is True
        assert result.level == EquivalenceLevel.CONTAINMENT

    def test_containment_fails_when_view_has_extra_tables(self) -> None:
        """View with {users, products} not contained in query with {users, orders}."""
        checker = EquivalenceChecker()
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["id"])
        products = Relation(name="products", alias="p", columns=["id"])

        # Container: users join orders
        container = Join(
            left=users,
            right=orders,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="id"),
            ),
        )

        # Contained: users join products (products not in container)
        contained = Join(
            left=users,
            right=products,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="p", column="id"),
            ),
        )

        result = checker.check_containment(container, contained)

        assert result.equivalent is False
        assert "products" in result.explanation or "p" in result.explanation

    def test_containment_with_projection_subset(self) -> None:
        """View projecting {id, name} contained in query projecting {id}."""
        checker = EquivalenceChecker()
        relation = Relation(name="users", alias="u", columns=["id", "name", "email"])

        # Container projects more columns
        container = Projection(
            columns=[
                (ColumnRef(table="u", column="id"), "id"),
                (ColumnRef(table="u", column="name"), "name"),
            ],
            input=relation,
        )

        # Contained projects fewer columns
        contained = Projection(
            columns=[(ColumnRef(table="u", column="id"), "id")],
            input=relation,
        )

        result = checker.check_containment(container, contained)

        assert result.equivalent is True

    # -------------------------------------------------------------------------
    # Complex Scenarios - Join Equivalence
    # -------------------------------------------------------------------------

    def test_same_structure_different_tables_not_equivalent(self) -> None:
        """Different tables in same positions should not be equivalent."""
        checker = EquivalenceChecker()
        a = Relation(name="a", alias="a", columns=["id"])
        b = Relation(name="b", alias="b", columns=["id", "a_id"])
        c = Relation(name="c", alias="c", columns=["id", "a_id"])

        ab_cond = AtomicPredicate(
            left=ColumnRef(table="a", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="a_id"),
        )
        ac_cond = AtomicPredicate(
            left=ColumnRef(table="a", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="c", column="a_id"),
        )

        # A join B
        expr1 = Join(left=a, right=b, join_type=JoinType.INNER, condition=ab_cond)

        # A join C (different table on right)
        expr2 = Join(left=a, right=c, join_type=JoinType.INNER, condition=ac_cond)

        result = checker.check_join_equivalence(expr1, expr2)

        assert result.equivalent is False
        assert "differ" in result.explanation.lower()

    def test_left_join_blocks_equivalence_check(self) -> None:
        """A LEFT JOIN B should return low confidence / not equivalent."""
        checker = EquivalenceChecker()
        a = Relation(name="a", alias="a", columns=["id"])
        b = Relation(name="b", alias="b", columns=["id", "a_id"])

        cond = AtomicPredicate(
            left=ColumnRef(table="a", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="a_id"),
        )

        left_join = Join(left=a, right=b, join_type=JoinType.LEFT, condition=cond)

        # Try to check join equivalence with itself (outer join present)
        result = checker.check_join_equivalence(left_join, left_join)

        # Outer joins block join reordering equivalence
        assert result.equivalent is False
        assert "outer" in result.explanation.lower() or result.confidence < 1.0

    def test_star_schema_same_structure_equivalent(self) -> None:
        """Same star schema join order should be equivalent."""
        checker = EquivalenceChecker()
        fact = Relation(name="fact", alias="f", columns=["id", "d1_id", "d2_id"])
        dim1 = Relation(name="dim1", alias="d1", columns=["id"])
        dim2 = Relation(name="dim2", alias="d2", columns=["id"])

        fd1_cond = AtomicPredicate(
            left=ColumnRef(table="f", column="d1_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="d1", column="id"),
        )
        fd2_cond = AtomicPredicate(
            left=ColumnRef(table="f", column="d2_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="d2", column="id"),
        )

        # Same structure: (fact join dim1) join dim2
        j1 = Join(left=fact, right=dim1, join_type=JoinType.INNER, condition=fd1_cond)
        order1 = Join(left=j1, right=dim2, join_type=JoinType.INNER, condition=fd2_cond)

        # Same structure again
        j2 = Join(left=fact, right=dim1, join_type=JoinType.INNER, condition=fd1_cond)
        order2 = Join(left=j2, right=dim2, join_type=JoinType.INNER, condition=fd2_cond)

        result = checker.check_join_equivalence(order1, order2)

        assert result.equivalent is True
        assert result.confidence == 0.9

    # -------------------------------------------------------------------------
    # Empirical Test Generation
    # -------------------------------------------------------------------------

    def test_generate_empirical_test_includes_fingerprints(self) -> None:
        """Generated SQL template should include both expression fingerprints."""
        checker = EquivalenceChecker()
        r1 = Relation(name="t1", alias="t1", columns=["x"])
        r2 = Relation(name="t2", alias="t2", columns=["y"])

        result = checker.generate_empirical_test(r1, r2)

        assert r1.fingerprint() in result
        assert r2.fingerprint() in result

    def test_generate_empirical_test_has_test_instructions(self) -> None:
        """Should include row count and EXCEPT-based comparison instructions."""
        checker = EquivalenceChecker()
        r1 = Relation(name="t1", alias="t1", columns=["x"])
        r2 = Relation(name="t2", alias="t2", columns=["y"])

        result = checker.generate_empirical_test(r1, r2)

        assert "row count" in result.lower() or "COUNT" in result
        assert "EXCEPT" in result


# =============================================================================
# Tests for collect_column_references
# =============================================================================


class TestCollectColumnReferences:
    """Tests for the collect_column_references function."""

    # -------------------------------------------------------------------------
    # Simple Scenarios
    # -------------------------------------------------------------------------

    def test_collect_from_base_relation_empty(self) -> None:
        """Base relation R has no column references in predicates."""
        relation = Relation(name="t", alias="t", columns=["x", "y"])

        refs = collect_column_references(relation)

        # Base relation has no predicates/expressions with column refs
        assert refs == set()

    def test_collect_from_simple_selection(self) -> None:
        """sigma_t.x>5(R) should collect {t.x}."""
        relation = Relation(name="t", alias="t", columns=["x", "y"])
        pred = AtomicPredicate(
            left=ColumnRef(table="t", column="x"),
            op=ComparisonOp.GT,
            right=Literal(value=5),
        )
        selection = Selection(predicate=pred, input=relation)

        refs = collect_column_references(selection)

        assert "t.x" in refs

    def test_collect_from_simple_projection(self) -> None:
        """pi_t.a,t.b(R) should collect {t.a, t.b}."""
        relation = Relation(name="t", alias="t", columns=["a", "b", "c"])
        projection = Projection(
            columns=[
                (ColumnRef(table="t", column="a"), "a"),
                (ColumnRef(table="t", column="b"), "b"),
            ],
            input=relation,
        )

        refs = collect_column_references(projection)

        assert "t.a" in refs
        assert "t.b" in refs
        assert "t.c" not in refs

    # -------------------------------------------------------------------------
    # Complex Scenarios
    # -------------------------------------------------------------------------

    def test_collect_from_compound_predicate(self) -> None:
        """sigma_(t.x>5 AND t.y<10)(R) should collect {t.x, t.y}."""
        relation = Relation(name="t", alias="t", columns=["x", "y"])
        compound = CompoundPredicate(
            op=LogicalOp.AND,
            operands=[
                AtomicPredicate(
                    left=ColumnRef(table="t", column="x"),
                    op=ComparisonOp.GT,
                    right=Literal(value=5),
                ),
                AtomicPredicate(
                    left=ColumnRef(table="t", column="y"),
                    op=ComparisonOp.LT,
                    right=Literal(value=10),
                ),
            ],
        )
        selection = Selection(predicate=compound, input=relation)

        refs = collect_column_references(selection)

        assert "t.x" in refs
        assert "t.y" in refs

    def test_collect_from_join_condition(self) -> None:
        """A join_{a.id=b.a_id} B should collect {a.id, b.a_id}."""
        a = Relation(name="a", alias="a", columns=["id", "name"])
        b = Relation(name="b", alias="b", columns=["id", "a_id"])
        join_cond = AtomicPredicate(
            left=ColumnRef(table="a", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="a_id"),
        )
        join = Join(left=a, right=b, join_type=JoinType.INNER, condition=join_cond)

        refs = collect_column_references(join)

        assert "a.id" in refs
        assert "b.a_id" in refs

    def test_collect_from_aggregation_with_having(self) -> None:
        """gamma_{group_by: t.x, having: SUM(t.y)>100} collects {t.x, t.y}."""
        relation = Relation(name="t", alias="t", columns=["x", "y", "z"])
        having_pred = AtomicPredicate(
            left=ColumnRef(table="t", column="y"),
            op=ComparisonOp.GT,
            right=Literal(value=100),
        )
        aggregation = Aggregation(
            input=relation,
            group_by=[ColumnRef(table="t", column="x")],
            aggregates=[
                AggregateSpec(
                    function=AggregateFunction.SUM,
                    argument=ColumnRef(table="t", column="y"),
                    alias="sum_y",
                )
            ],
            having=having_pred,
        )

        refs = collect_column_references(aggregation)

        assert "t.x" in refs
        assert "t.y" in refs

    def test_collect_from_deeply_nested_expression(self) -> None:
        """pi(sigma(A join sigma(B join C))) - should collect all column refs."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id", "c_id", "val"])
        c = Relation(name="c", alias="c", columns=["id", "name"])

        # Inner: B join C
        bc_cond = AtomicPredicate(
            left=ColumnRef(table="b", column="c_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="c", column="id"),
        )
        bc_join = Join(left=b, right=c, join_type=JoinType.INNER, condition=bc_cond)

        # Selection on B join C
        bc_sel_pred = AtomicPredicate(
            left=ColumnRef(table="b", column="val"),
            op=ComparisonOp.GT,
            right=Literal(value=0),
        )
        bc_sel = Selection(predicate=bc_sel_pred, input=bc_join)

        # A join (selection on B join C)
        abc_cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        abc_join = Join(left=a, right=bc_sel, join_type=JoinType.INNER, condition=abc_cond)

        # Outer selection
        outer_pred = AtomicPredicate(
            left=ColumnRef(table="a", column="id"),
            op=ComparisonOp.GT,
            right=Literal(value=10),
        )
        outer_sel = Selection(predicate=outer_pred, input=abc_join)

        # Projection
        proj = Projection(
            columns=[
                (ColumnRef(table="a", column="id"), "a_id"),
                (ColumnRef(table="c", column="name"), "c_name"),
            ],
            input=outer_sel,
        )

        refs = collect_column_references(proj)

        # Should collect all columns from all levels
        assert "a.id" in refs
        assert "a.b_id" in refs
        assert "b.c_id" in refs
        assert "b.id" in refs
        assert "b.val" in refs
        assert "c.id" in refs
        assert "c.name" in refs

    def test_collect_from_self_join(self) -> None:
        """users u1 join users u2 ON u1.manager_id = u2.id."""
        u1 = Relation(name="users", alias="u1", columns=["id", "manager_id", "name"])
        u2 = Relation(name="users", alias="u2", columns=["id", "manager_id", "name"])
        self_join_cond = AtomicPredicate(
            left=ColumnRef(table="u1", column="manager_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="u2", column="id"),
        )
        self_join = Join(left=u1, right=u2, join_type=JoinType.INNER, condition=self_join_cond)

        refs = collect_column_references(self_join)

        assert "u1.manager_id" in refs
        assert "u2.id" in refs


# =============================================================================
# Tests for build_column_rewrite_map
# =============================================================================


class TestBuildColumnRewriteMap:
    """Tests for the build_column_rewrite_map function."""

    # -------------------------------------------------------------------------
    # Simple Scenarios
    # -------------------------------------------------------------------------

    def test_select_star_maps_covered_columns(self) -> None:
        """SELECT * view: t.col -> cv.col passthrough."""
        relation = Relation(name="t", alias="t", columns=["id", "name"])
        pred = AtomicPredicate(
            left=ColumnRef(table="t", column="id"),
            op=ComparisonOp.GT,
            right=Literal(value=0),
        )
        query = Selection(predicate=pred, input=relation)

        rewrite_map = build_column_rewrite_map(
            query_ra=query,
            covered_aliases={"t"},
            lineage={},  # Not used for SELECT *
            is_select_star=True,
            view_alias="cv",
        )

        assert "t.id" in rewrite_map
        assert rewrite_map["t.id"] == "cv.id"

    def test_explicit_lineage_single_column(self) -> None:
        """Lineage {(t, id): user_id} maps t.id -> cv.user_id."""
        relation = Relation(name="t", alias="t", columns=["id", "name"])
        pred = AtomicPredicate(
            left=ColumnRef(table="t", column="id"),
            op=ComparisonOp.GT,
            right=Literal(value=0),
        )
        query = Selection(predicate=pred, input=relation)

        lineage = {("t", "id"): "user_id"}

        rewrite_map = build_column_rewrite_map(
            query_ra=query,
            covered_aliases={"t"},
            lineage=lineage,
            is_select_star=False,
            view_alias="cv",
        )

        assert "t.id" in rewrite_map
        assert rewrite_map["t.id"] == "cv.user_id"

    def test_uncovered_alias_not_mapped(self) -> None:
        """Columns from non-covered tables should not appear in map."""
        u = Relation(name="users", alias="u", columns=["id"])
        o = Relation(name="orders", alias="o", columns=["id", "user_id"])
        join_cond = AtomicPredicate(
            left=ColumnRef(table="u", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="o", column="user_id"),
        )
        query = Join(left=u, right=o, join_type=JoinType.INNER, condition=join_cond)

        # Only 'u' is covered, 'o' is not
        rewrite_map = build_column_rewrite_map(
            query_ra=query,
            covered_aliases={"u"},
            lineage={("u", "id"): "user_id"},
            is_select_star=False,
            view_alias="cv",
        )

        # u.id should be mapped
        assert "u.id" in rewrite_map
        # o.user_id should NOT be mapped (uncovered)
        assert "o.user_id" not in rewrite_map

    # -------------------------------------------------------------------------
    # Complex Scenarios
    # -------------------------------------------------------------------------

    def test_multiple_covered_tables_lineage(self) -> None:
        """View covering {users, orders} with lineage for both."""
        u = Relation(name="users", alias="u", columns=["id", "name"])
        o = Relation(name="orders", alias="o", columns=["id", "user_id", "total"])
        join_cond = AtomicPredicate(
            left=ColumnRef(table="u", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="o", column="user_id"),
        )
        query = Join(left=u, right=o, join_type=JoinType.INNER, condition=join_cond)

        lineage = {
            ("u", "id"): "user_id",
            ("u", "name"): "user_name",
            ("o", "id"): "order_id",
            ("o", "user_id"): "fk_user_id",
        }

        rewrite_map = build_column_rewrite_map(
            query_ra=query,
            covered_aliases={"u", "o"},
            lineage=lineage,
            is_select_star=False,
            view_alias="cv",
        )

        assert rewrite_map.get("u.id") == "cv.user_id"
        assert rewrite_map.get("o.user_id") == "cv.fk_user_id"

    def test_partial_coverage_only_maps_covered(self) -> None:
        """Query has {A, B, C}, view covers {A, B} - only A, B columns mapped."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id", "c_id"])
        c = Relation(name="c", alias="c", columns=["id", "name"])

        ab_cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        bc_cond = AtomicPredicate(
            left=ColumnRef(table="b", column="c_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="c", column="id"),
        )

        ab_join = Join(left=a, right=b, join_type=JoinType.INNER, condition=ab_cond)
        query = Join(left=ab_join, right=c, join_type=JoinType.INNER, condition=bc_cond)

        lineage = {
            ("a", "id"): "a_id",
            ("a", "b_id"): "a_b_id",
            ("b", "id"): "b_id",
            ("b", "c_id"): "b_c_id",
        }

        rewrite_map = build_column_rewrite_map(
            query_ra=query,
            covered_aliases={"a", "b"},  # c is NOT covered
            lineage=lineage,
            is_select_star=False,
            view_alias="cv",
        )

        # a and b columns should be mapped
        assert "a.b_id" in rewrite_map
        assert "b.id" in rewrite_map
        assert "b.c_id" in rewrite_map
        # c columns should NOT be mapped
        assert "c.id" not in rewrite_map

    def test_column_rename_in_view(self) -> None:
        """View renames source.original_name -> view.new_name."""
        relation = Relation(name="t", alias="t", columns=["original_name"])
        pred = AtomicPredicate(
            left=ColumnRef(table="t", column="original_name"),
            op=ComparisonOp.EQ,
            right=Literal(value="test"),
        )
        query = Selection(predicate=pred, input=relation)

        lineage = {("t", "original_name"): "new_name"}

        rewrite_map = build_column_rewrite_map(
            query_ra=query,
            covered_aliases={"t"},
            lineage=lineage,
            is_select_star=False,
            view_alias="cv",
        )

        assert rewrite_map.get("t.original_name") == "cv.new_name"


# =============================================================================
# Tests for check_column_availability
# =============================================================================


class TestCheckColumnAvailability:
    """Tests for the check_column_availability function."""

    # -------------------------------------------------------------------------
    # Simple Scenarios
    # -------------------------------------------------------------------------

    def test_select_star_always_available(self) -> None:
        """SELECT * view - all columns from covered tables available."""
        required = {"t.id", "t.name", "t.email"}
        covered_aliases = {"t"}
        covered_rels: set[RelationRef] = set()  # Not used for SELECT *
        lineage: dict[tuple[str, str], str] = {}

        available, missing = check_column_availability(
            required_columns=required,
            covered_aliases=covered_aliases,
            covered_rels=covered_rels,
            lineage=lineage,
            is_select_star=True,
        )

        assert available is True
        assert missing == set()

    def test_single_column_in_lineage(self) -> None:
        """Required {t.id}, lineage has (t, id) -> available."""
        required = {"t.id"}
        covered_aliases = {"t"}
        covered_rels = {RelationRef(physical_table="t", alias="t")}
        lineage = {("t", "id"): "mapped_id"}

        available, missing = check_column_availability(
            required_columns=required,
            covered_aliases=covered_aliases,
            covered_rels=covered_rels,
            lineage=lineage,
            is_select_star=False,
        )

        assert available is True
        assert missing == set()

    def test_single_column_missing(self) -> None:
        """Required {t.email}, lineage only has (t, id) -> missing {t.email}."""
        required = {"t.email"}
        covered_aliases = {"t"}
        covered_rels = {RelationRef(physical_table="t", alias="t")}
        lineage = {("t", "id"): "mapped_id"}  # email not in lineage

        available, missing = check_column_availability(
            required_columns=required,
            covered_aliases=covered_aliases,
            covered_rels=covered_rels,
            lineage=lineage,
            is_select_star=False,
        )

        assert available is False
        assert "t.email" in missing

    # -------------------------------------------------------------------------
    # Complex Scenarios
    # -------------------------------------------------------------------------

    def test_multiple_columns_partial_availability(self) -> None:
        """Required {t.id, t.name, t.email}, lineage has (t,id), (t,name) -> missing {t.email}."""
        required = {"t.id", "t.name", "t.email"}
        covered_aliases = {"t"}
        covered_rels = {RelationRef(physical_table="t", alias="t")}
        lineage = {
            ("t", "id"): "mapped_id",
            ("t", "name"): "mapped_name",
            # email is missing
        }

        available, missing = check_column_availability(
            required_columns=required,
            covered_aliases=covered_aliases,
            covered_rels=covered_rels,
            lineage=lineage,
            is_select_star=False,
        )

        assert available is False
        assert "t.email" in missing
        assert "t.id" not in missing
        assert "t.name" not in missing

    def test_columns_from_multiple_covered_tables(self) -> None:
        """Required from both users and orders, lineage covers both."""
        required = {"u.id", "u.name", "o.id", "o.total"}
        covered_aliases = {"u", "o"}
        covered_rels = {
            RelationRef(physical_table="users", alias="u"),
            RelationRef(physical_table="orders", alias="o"),
        }
        lineage = {
            ("u", "id"): "user_id",
            ("u", "name"): "user_name",
            ("o", "id"): "order_id",
            ("o", "total"): "order_total",
        }

        available, missing = check_column_availability(
            required_columns=required,
            covered_aliases=covered_aliases,
            covered_rels=covered_rels,
            lineage=lineage,
            is_select_star=False,
        )

        assert available is True
        assert missing == set()

    def test_physical_table_fallback_lookup(self) -> None:
        """Lineage has (users, id) but query uses alias (u, id) - should match via RelationRef."""
        required = {"u.id"}
        covered_aliases = {"u"}
        covered_rels = {RelationRef(physical_table="users", alias="u")}
        # Lineage uses physical table name, not alias
        lineage = {("users", "id"): "user_id"}

        available, missing = check_column_availability(
            required_columns=required,
            covered_aliases=covered_aliases,
            covered_rels=covered_rels,
            lineage=lineage,
            is_select_star=False,
        )

        assert available is True
        assert missing == set()

    def test_uncovered_table_columns_not_checked(self) -> None:
        """Required {covered.id, remaining.id} - only covered.id checked."""
        required = {"c.id", "r.id"}
        covered_aliases = {"c"}  # 'r' is NOT covered
        covered_rels = {RelationRef(physical_table="covered", alias="c")}
        lineage = {("c", "id"): "covered_id"}  # Only lineage for covered table

        available, missing = check_column_availability(
            required_columns=required,
            covered_aliases=covered_aliases,
            covered_rels=covered_rels,
            lineage=lineage,
            is_select_star=False,
        )

        # r.id is not checked because 'r' is not in covered_aliases
        assert available is True
        assert missing == set()

    def test_all_columns_from_join_condition_available(self) -> None:
        """Join u.id = o.user_id - both columns must be available for boundary join."""
        # This simulates checking that a boundary join has all needed columns
        required = {"u.id", "o.user_id"}
        covered_aliases = {"u", "o"}
        covered_rels = {
            RelationRef(physical_table="users", alias="u"),
            RelationRef(physical_table="orders", alias="o"),
        }
        lineage = {
            ("u", "id"): "user_id",
            ("o", "user_id"): "order_user_id",
        }

        available, missing = check_column_availability(
            required_columns=required,
            covered_aliases=covered_aliases,
            covered_rels=covered_rels,
            lineage=lineage,
            is_select_star=False,
        )

        assert available is True
        assert missing == set()


# =============================================================================
# Tests for check_outer_join_safety
# =============================================================================


class TestCheckOuterJoinSafety:
    """Tests for the check_outer_join_safety function."""

    # -------------------------------------------------------------------------
    # Simple Scenarios - Inner Joins (Always Safe)
    # -------------------------------------------------------------------------

    def test_inner_join_no_boundary(self) -> None:
        """A join B where both covered - safe (no boundary)."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id"])
        cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        query = Join(left=a, right=b, join_type=JoinType.INNER, condition=cond)

        # Both relations covered - no boundary to cross
        covered = {
            RelationRef(physical_table="a", alias="a"),
            RelationRef(physical_table="b", alias="b"),
        }
        remaining: set[RelationRef] = set()

        is_safe, reason = check_outer_join_safety(query, covered, remaining)

        assert is_safe is True
        assert reason == ""

    def test_inner_join_at_boundary(self) -> None:
        """A join B where A covered, B remaining - safe (inner join)."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id"])
        cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        query = Join(left=a, right=b, join_type=JoinType.INNER, condition=cond)

        covered = {RelationRef(physical_table="a", alias="a")}
        remaining = {RelationRef(physical_table="b", alias="b")}

        is_safe, reason = check_outer_join_safety(query, covered, remaining)

        # Inner joins are always safe to split
        assert is_safe is True

    # -------------------------------------------------------------------------
    # Complex Scenarios - LEFT JOIN
    # -------------------------------------------------------------------------

    def test_left_join_covered_preserved_remaining_null_extended(self) -> None:
        """A LEFT JOIN B, A covered, B remaining - SAFE (covered on preserved side)."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id"])
        cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        query = Join(left=a, right=b, join_type=JoinType.LEFT, condition=cond)

        # A (left) is covered, B (right/null-extended) is remaining
        covered = {RelationRef(physical_table="a", alias="a")}
        remaining = {RelationRef(physical_table="b", alias="b")}

        is_safe, reason = check_outer_join_safety(query, covered, remaining)

        # Covered on preserved (left) side is SAFE
        assert is_safe is True

    def test_left_join_covered_null_extended_remaining_preserved(self) -> None:
        """A LEFT JOIN B, B covered, A remaining - UNSAFE (covered on null-extended side)."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id"])
        cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        query = Join(left=a, right=b, join_type=JoinType.LEFT, condition=cond)

        # B (right/null-extended) is covered, A (left/preserved) is remaining
        covered = {RelationRef(physical_table="b", alias="b")}
        remaining = {RelationRef(physical_table="a", alias="a")}

        is_safe, reason = check_outer_join_safety(query, covered, remaining)

        # Covered on null-extended (right) side is UNSAFE
        assert is_safe is False
        assert "null-extended" in reason.lower() or "LEFT" in reason

    def test_left_join_covered_spans_both_sides(self) -> None:
        """(A LEFT JOIN B) LEFT JOIN C, A and B both covered, C remaining - check safety."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id", "c_id"])
        c = Relation(name="c", alias="c", columns=["id"])

        ab_cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        bc_cond = AtomicPredicate(
            left=ColumnRef(table="b", column="c_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="c", column="id"),
        )

        ab_join = Join(left=a, right=b, join_type=JoinType.LEFT, condition=ab_cond)
        query = Join(left=ab_join, right=c, join_type=JoinType.LEFT, condition=bc_cond)

        # A and B are covered, C is remaining
        covered = {
            RelationRef(physical_table="a", alias="a"),
            RelationRef(physical_table="b", alias="b"),
        }
        remaining = {RelationRef(physical_table="c", alias="c")}

        is_safe, reason = check_outer_join_safety(query, covered, remaining)

        # The outer LEFT JOIN (ab_join LEFT JOIN c) has covered on left - should be safe
        # The inner LEFT JOIN (a LEFT JOIN b) doesn't cross the boundary
        assert is_safe is True

    def test_chained_left_joins_coverage(self) -> None:
        """A LEFT JOIN B LEFT JOIN C, A covered, B and C remaining - check each boundary."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id", "c_id"])
        c = Relation(name="c", alias="c", columns=["id"])

        ab_cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        bc_cond = AtomicPredicate(
            left=ColumnRef(table="b", column="c_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="c", column="id"),
        )

        ab_join = Join(left=a, right=b, join_type=JoinType.LEFT, condition=ab_cond)
        query = Join(left=ab_join, right=c, join_type=JoinType.LEFT, condition=bc_cond)

        # Only A is covered, B and C are remaining
        covered = {RelationRef(physical_table="a", alias="a")}
        remaining = {
            RelationRef(physical_table="b", alias="b"),
            RelationRef(physical_table="c", alias="c"),
        }

        is_safe, reason = check_outer_join_safety(query, covered, remaining)

        # A LEFT JOIN B: A (covered) on preserved side - safe
        # (A LEFT JOIN B) LEFT JOIN C: covered (A) is still on left - safe
        assert is_safe is True

    # -------------------------------------------------------------------------
    # Complex Scenarios - RIGHT JOIN
    # -------------------------------------------------------------------------

    def test_right_join_covered_preserved_safe(self) -> None:
        """A RIGHT JOIN B, B covered - SAFE (covered on preserved side)."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id"])
        cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        query = Join(left=a, right=b, join_type=JoinType.RIGHT, condition=cond)

        # B (right/preserved) is covered, A (left/null-extended) is remaining
        covered = {RelationRef(physical_table="b", alias="b")}
        remaining = {RelationRef(physical_table="a", alias="a")}

        is_safe, reason = check_outer_join_safety(query, covered, remaining)

        # Covered on preserved (right) side is SAFE
        assert is_safe is True

    def test_right_join_covered_null_extended_unsafe(self) -> None:
        """A RIGHT JOIN B, A covered - UNSAFE (covered on null-extended side)."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id"])
        cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        query = Join(left=a, right=b, join_type=JoinType.RIGHT, condition=cond)

        # A (left/null-extended) is covered, B (right/preserved) is remaining
        covered = {RelationRef(physical_table="a", alias="a")}
        remaining = {RelationRef(physical_table="b", alias="b")}

        is_safe, reason = check_outer_join_safety(query, covered, remaining)

        # Covered on null-extended (left) side is UNSAFE
        assert is_safe is False
        assert "null-extended" in reason.lower() or "RIGHT" in reason

    # -------------------------------------------------------------------------
    # Complex Scenarios - FULL OUTER JOIN
    # -------------------------------------------------------------------------

    def test_full_outer_join_at_boundary_always_unsafe(self) -> None:
        """A FULL OUTER JOIN B, any coverage split - UNSAFE."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id"])
        cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        query = Join(left=a, right=b, join_type=JoinType.FULL, condition=cond)

        # A covered, B remaining
        covered = {RelationRef(physical_table="a", alias="a")}
        remaining = {RelationRef(physical_table="b", alias="b")}

        is_safe, reason = check_outer_join_safety(query, covered, remaining)

        # FULL OUTER JOIN at boundary is always UNSAFE
        assert is_safe is False
        assert "FULL" in reason

    def test_full_outer_join_both_covered_safe(self) -> None:
        """A FULL OUTER JOIN B, both covered (no boundary) - SAFE."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id"])
        cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        query = Join(left=a, right=b, join_type=JoinType.FULL, condition=cond)

        # Both covered - no boundary to cross
        covered = {
            RelationRef(physical_table="a", alias="a"),
            RelationRef(physical_table="b", alias="b"),
        }
        remaining: set[RelationRef] = set()

        is_safe, reason = check_outer_join_safety(query, covered, remaining)

        # No boundary = safe
        assert is_safe is True

    # -------------------------------------------------------------------------
    # Complex Scenarios - Mixed Join Types
    # -------------------------------------------------------------------------

    def test_inner_then_left_join_chain(self) -> None:
        """(A join B) LEFT JOIN C - A,B covered, C remaining."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id", "c_id"])
        c = Relation(name="c", alias="c", columns=["id"])

        ab_cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        bc_cond = AtomicPredicate(
            left=ColumnRef(table="b", column="c_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="c", column="id"),
        )

        # Inner join A and B
        ab_join = Join(left=a, right=b, join_type=JoinType.INNER, condition=ab_cond)
        # Left join with C
        query = Join(left=ab_join, right=c, join_type=JoinType.LEFT, condition=bc_cond)

        covered = {
            RelationRef(physical_table="a", alias="a"),
            RelationRef(physical_table="b", alias="b"),
        }
        remaining = {RelationRef(physical_table="c", alias="c")}

        is_safe, reason = check_outer_join_safety(query, covered, remaining)

        # Inner join doesn't restrict. LEFT JOIN has covered on left (preserved) - safe
        assert is_safe is True

    def test_left_then_inner_join_chain(self) -> None:
        """(A LEFT JOIN B) join C - check coverage positions."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id", "c_id"])
        c = Relation(name="c", alias="c", columns=["id"])

        ab_cond = AtomicPredicate(
            left=ColumnRef(table="a", column="b_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="id"),
        )
        bc_cond = AtomicPredicate(
            left=ColumnRef(table="b", column="c_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="c", column="id"),
        )

        # Left join A and B
        ab_join = Join(left=a, right=b, join_type=JoinType.LEFT, condition=ab_cond)
        # Inner join with C
        query = Join(left=ab_join, right=c, join_type=JoinType.INNER, condition=bc_cond)

        # A covered, B and C remaining
        covered = {RelationRef(physical_table="a", alias="a")}
        remaining = {
            RelationRef(physical_table="b", alias="b"),
            RelationRef(physical_table="c", alias="c"),
        }

        is_safe, reason = check_outer_join_safety(query, covered, remaining)

        # A LEFT JOIN B: A (covered) on left (preserved) - safe
        # Inner join with C doesn't add restrictions
        assert is_safe is True

    def test_star_schema_with_optional_dimension(self) -> None:
        """Fact join Dim1 join Dim2 LEFT JOIN OptionalDim."""
        fact = Relation(name="fact", alias="f", columns=["id", "d1_id", "d2_id", "opt_id"])
        dim1 = Relation(name="dim1", alias="d1", columns=["id"])
        dim2 = Relation(name="dim2", alias="d2", columns=["id"])
        opt_dim = Relation(name="opt_dim", alias="od", columns=["id"])

        fd1_cond = AtomicPredicate(
            left=ColumnRef(table="f", column="d1_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="d1", column="id"),
        )
        fd2_cond = AtomicPredicate(
            left=ColumnRef(table="f", column="d2_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="d2", column="id"),
        )
        fod_cond = AtomicPredicate(
            left=ColumnRef(table="f", column="opt_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="od", column="id"),
        )

        # Build: ((fact join dim1) join dim2) LEFT JOIN opt_dim
        j1 = Join(left=fact, right=dim1, join_type=JoinType.INNER, condition=fd1_cond)
        j2 = Join(left=j1, right=dim2, join_type=JoinType.INNER, condition=fd2_cond)
        query = Join(left=j2, right=opt_dim, join_type=JoinType.LEFT, condition=fod_cond)

        # fact, dim1, dim2 covered; opt_dim remaining
        covered = {
            RelationRef(physical_table="fact", alias="f"),
            RelationRef(physical_table="dim1", alias="d1"),
            RelationRef(physical_table="dim2", alias="d2"),
        }
        remaining = {RelationRef(physical_table="opt_dim", alias="od")}

        is_safe, reason = check_outer_join_safety(query, covered, remaining)

        # Inner joins don't restrict. LEFT JOIN has covered on left - safe
        assert is_safe is True
