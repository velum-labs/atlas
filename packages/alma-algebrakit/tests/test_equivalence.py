"""Tests for algebrakit rewriting equivalence module."""

from alma_algebrakit.models.algebra import (
    AtomicPredicate,
    ColumnRef,
    ComparisonOp,
    Join,
    JoinType,
    Literal,
    Relation,
    Selection,
)
from alma_algebrakit.rewriting.equivalence import EquivalenceChecker, EquivalenceLevel


class TestStructuralEquivalence:
    """Tests for structural equivalence."""

    def test_same_expression_equivalent_true(self) -> None:
        """Structural equivalence: same expression returns equivalent=True."""
        checker = EquivalenceChecker()
        relation = Relation(name="users", alias="u", columns=["id", "name"])
        result = checker.check_structural(relation, relation)
        assert result.equivalent is True
        assert result.level == EquivalenceLevel.STRUCTURAL
        assert result.confidence == 1.0

    def test_different_predicates_equivalent_false(self) -> None:
        """Structural equivalence: different predicates returns equivalent=False."""
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

    def test_structural_equivalence_after_join_reorder_inner_equijoin(
        self,
    ) -> None:
        """Structural equivalence after join reorder (inner equi-join)."""
        checker = EquivalenceChecker()
        users = Relation(name="users", alias="u", columns=["id", "name"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])
        join_cond = AtomicPredicate(
            left=ColumnRef(table="u", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="o", column="user_id"),
        )
        join1 = Join(left=users, right=orders, join_type=JoinType.INNER, condition=join_cond)
        join2 = Join(left=orders, right=users, join_type=JoinType.INNER, condition=join_cond)
        result = checker.check_join_equivalence(join1, join2)
        assert result.equivalent is True
        assert result.confidence == 0.9


class TestCheckContainment:
    """Tests for check_containment."""

    def test_subset_tables_returns_true(self) -> None:
        """check_containment: subset tables returns True."""
        checker = EquivalenceChecker()
        users = Relation(name="users", alias="u", columns=["id", "name"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])
        join_cond = AtomicPredicate(
            left=ColumnRef(table="u", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="o", column="user_id"),
        )
        container = Join(left=users, right=orders, join_type=JoinType.INNER, condition=join_cond)
        contained = users
        result = checker.check_containment(container, contained)
        assert result.equivalent is True
        assert result.level == EquivalenceLevel.CONTAINMENT

    def test_missing_tables_returns_false(self) -> None:
        """check_containment: missing tables returns False."""
        checker = EquivalenceChecker()
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["id"])
        products = Relation(name="products", alias="p", columns=["id"])
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


class TestCheckJoinEquivalence:
    """Tests for check_join_equivalence."""

    def test_same_edges_equivalent(self) -> None:
        """check_join_equivalence: same edges returns equivalent."""
        checker = EquivalenceChecker()
        a = Relation(name="a", alias="a", columns=["id"])
        b = Relation(name="b", alias="b", columns=["id", "a_id"])
        cond = AtomicPredicate(
            left=ColumnRef(table="a", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="a_id"),
        )
        join1 = Join(left=a, right=b, join_type=JoinType.INNER, condition=cond)
        join2 = Join(left=a, right=b, join_type=JoinType.INNER, condition=cond)
        result = checker.check_join_equivalence(join1, join2)
        assert result.equivalent is True
        assert result.confidence == 0.9

    def test_outer_join_present_equivalent_false(self) -> None:
        """check_join_equivalence: outer join present returns equivalent=False."""
        checker = EquivalenceChecker()
        a = Relation(name="a", alias="a", columns=["id"])
        b = Relation(name="b", alias="b", columns=["id", "a_id"])
        cond = AtomicPredicate(
            left=ColumnRef(table="a", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="b", column="a_id"),
        )
        left_join = Join(left=a, right=b, join_type=JoinType.LEFT, condition=cond)
        result = checker.check_join_equivalence(left_join, left_join)
        assert result.equivalent is False
        assert "outer" in result.explanation.lower()


class TestGenerateEmpiricalTest:
    """Tests for generate_empirical_test."""

    def test_generate_empirical_test_returns_non_empty_string(self) -> None:
        """generate_empirical_test returns non-empty string."""
        checker = EquivalenceChecker()
        r1 = Relation(name="t1", alias="t1", columns=["x"])
        r2 = Relation(name="t2", alias="t2", columns=["y"])
        result = checker.generate_empirical_test(r1, r2)
        assert isinstance(result, str)
        assert len(result) > 0
        assert "Empirical" in result or "empirical" in result
        assert r1.fingerprint() in result
        assert r2.fingerprint() in result
