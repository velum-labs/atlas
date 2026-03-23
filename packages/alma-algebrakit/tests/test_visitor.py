"""Tests for the visitor pattern implementation."""

import pytest

from alma_algebrakit.models.algebra import (
    Aggregation,
    AtomicPredicate,
    ColumnRef,
    ComparisonOp,
    Join,
    JoinType,
    Limit,
    Literal,
    Projection,
    RAExpression,
    Relation,
    Selection,
    Sort,
    SortDirection,
    SortSpec,
)
from alma_algebrakit.visitor import (
    CollectionVisitor,
    RAExpressionVisitor,
    TransformationVisitor,
    dispatch_accept,
)

# =============================================================================
# Test Visitors
# =============================================================================


class TableCollector(CollectionVisitor):
    """Visitor that collects all table names."""

    def __init__(self) -> None:
        self.tables: list[str] = []

    def visit_relation(self, expr: Relation) -> None:
        self.tables.append(expr.name)

    def visit_join(self, expr: Join) -> None:
        expr.left.accept(self)
        expr.right.accept(self)


class SelectionCounter(CollectionVisitor):
    """Visitor that counts selections."""

    def __init__(self) -> None:
        self.count: int = 0

    def visit_selection(self, expr: Selection) -> None:
        self.count += 1
        expr.input.accept(self)


class TableRenamer(TransformationVisitor):
    """Visitor that renames tables."""

    def __init__(self, mapping: dict[str, str]) -> None:
        self.mapping = mapping

    def visit_relation(self, expr: Relation) -> RAExpression:
        new_name = self.mapping.get(expr.name, expr.name)
        if new_name != expr.name:
            return Relation(
                name=new_name,
                alias=expr.alias,
                columns=expr.columns,
            )
        return expr


class FingerprintVisitor(RAExpressionVisitor[str]):
    """Visitor that generates a simplified fingerprint."""

    def visit_relation(self, expr: Relation) -> str:
        return f"R({expr.name})"

    def visit_selection(self, expr: Selection) -> str:
        return f"σ({expr.input.accept(self)})"

    def visit_projection(self, expr: Projection) -> str:
        return f"π({expr.input.accept(self)})"

    def visit_join(self, expr: Join) -> str:
        left = expr.left.accept(self)
        right = expr.right.accept(self)
        return f"({left}⋈{right})"

    def visit_aggregation(self, expr: Aggregation) -> str:
        return f"γ({expr.input.accept(self)})"

    def visit_union(self, expr) -> str:
        return f"({expr.left.accept(self)}∪{expr.right.accept(self)})"

    def visit_difference(self, expr) -> str:
        return f"({expr.left.accept(self)}-{expr.right.accept(self)})"

    def visit_intersect(self, expr) -> str:
        return f"({expr.left.accept(self)}∩{expr.right.accept(self)})"

    def visit_sort(self, expr: Sort) -> str:
        return f"τ({expr.input.accept(self)})"

    def visit_limit(self, expr: Limit) -> str:
        return f"λ({expr.input.accept(self)})"

    def visit_with_expression(self, expr) -> str:
        ctes = ", ".join(f"{cte.name}={cte.query.accept(self)}" for cte in expr.ctes)
        main = expr.main_query.accept(self)
        return f"WITH({ctes}; {main})"


# =============================================================================
# Tests
# =============================================================================


class TestCollectionVisitor:
    """Tests for CollectionVisitor."""

    def test_collect_tables_from_single_relation(self) -> None:
        """Test collecting tables from a single relation."""
        relation = Relation(name="users", alias="u", columns=["id", "name"])

        collector = TableCollector()
        relation.accept(collector)

        assert collector.tables == ["users"]

    def test_collect_tables_from_join(self) -> None:
        """Test collecting tables from a join."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["user_id"])
        join = Join(left=users, right=orders, join_type=JoinType.INNER)

        collector = TableCollector()
        join.accept(collector)

        assert set(collector.tables) == {"users", "orders"}

    def test_count_selections(self) -> None:
        """Test counting selections in a query."""
        relation = Relation(name="t", alias="t", columns=["x", "y"])
        pred1 = AtomicPredicate(
            left=ColumnRef(column="x"),
            op=ComparisonOp.GT,
            right=Literal(value=0),
        )
        pred2 = AtomicPredicate(
            left=ColumnRef(column="y"),
            op=ComparisonOp.LT,
            right=Literal(value=100),
        )
        inner = Selection(predicate=pred1, input=relation)
        outer = Selection(predicate=pred2, input=inner)

        counter = SelectionCounter()
        outer.accept(counter)

        assert counter.count == 2


class TestTransformationVisitor:
    """Tests for TransformationVisitor."""

    def test_rename_single_table(self) -> None:
        """Test renaming a single table."""
        relation = Relation(name="old_name", alias="t", columns=["id"])

        renamer = TableRenamer({"old_name": "new_name"})
        result = relation.accept(renamer)

        assert isinstance(result, Relation)
        assert result.name == "new_name"
        assert result.alias == "t"

    def test_rename_preserves_structure(self) -> None:
        """Test that transformation preserves query structure."""
        relation = Relation(name="t", alias="t", columns=["x"])
        pred = AtomicPredicate(
            left=ColumnRef(column="x"),
            op=ComparisonOp.GT,
            right=Literal(value=0),
        )
        selection = Selection(predicate=pred, input=relation)
        projection = Projection(
            columns=[(ColumnRef(column="x"), None)],
            input=selection,
        )

        renamer = TableRenamer({})  # No renames
        result = projection.accept(renamer)

        # Structure should be preserved
        assert isinstance(result, Projection)

    def test_rename_in_join(self) -> None:
        """Test renaming tables in a join."""
        left = Relation(name="users", alias="u", columns=["id"])
        right = Relation(name="orders", alias="o", columns=["user_id"])
        join = Join(left=left, right=right, join_type=JoinType.INNER)

        renamer = TableRenamer({"users": "customers"})
        result = join.accept(renamer)

        assert isinstance(result, Join)
        assert isinstance(result.left, Relation)
        assert result.left.name == "customers"


class TestFingerprintVisitor:
    """Tests for custom fingerprint visitor."""

    def test_simple_relation(self) -> None:
        """Test fingerprint of a simple relation."""
        relation = Relation(name="t", alias="t", columns=["x"])

        visitor = FingerprintVisitor()
        fp = relation.accept(visitor)

        assert fp == "R(t)"

    def test_selection_fingerprint(self) -> None:
        """Test fingerprint of a selection."""
        relation = Relation(name="t", alias="t", columns=["x"])
        pred = AtomicPredicate(
            left=ColumnRef(column="x"),
            op=ComparisonOp.GT,
            right=Literal(value=0),
        )
        selection = Selection(predicate=pred, input=relation)

        visitor = FingerprintVisitor()
        fp = selection.accept(visitor)

        assert fp == "σ(R(t))"

    def test_join_fingerprint(self) -> None:
        """Test fingerprint of a join."""
        left = Relation(name="a", alias="a", columns=["id"])
        right = Relation(name="b", alias="b", columns=["id"])
        join = Join(left=left, right=right, join_type=JoinType.INNER)

        visitor = FingerprintVisitor()
        fp = join.accept(visitor)

        assert fp == "(R(a)⋈R(b))"

    def test_complex_query_fingerprint(self) -> None:
        """Test fingerprint of a complex query."""
        relation = Relation(name="t", alias="t", columns=["x"])
        pred = AtomicPredicate(
            left=ColumnRef(column="x"),
            op=ComparisonOp.GT,
            right=Literal(value=0),
        )
        selection = Selection(predicate=pred, input=relation)
        projection = Projection(
            columns=[(ColumnRef(column="x"), None)],
            input=selection,
        )
        sort = Sort(
            order_by=[SortSpec(expression=ColumnRef(column="x"), direction=SortDirection.ASC)],
            input=projection,
        )
        limit = Limit(limit=10, input=sort)

        visitor = FingerprintVisitor()
        fp = limit.accept(visitor)

        assert fp == "λ(τ(π(σ(R(t)))))"


class TestDispatchAccept:
    """Tests for dispatch_accept function."""

    def test_dispatch_to_correct_visitor_method(self) -> None:
        """Test that dispatch_accept calls the correct method."""
        relation = Relation(name="t", alias="t", columns=["x"])

        visitor = FingerprintVisitor()
        result = dispatch_accept(relation, visitor)

        assert result == "R(t)"

    def test_unknown_type_raises_error(self) -> None:
        """Test that unknown types raise TypeError."""

        class FakeExpression:
            pass

        visitor = FingerprintVisitor()

        with pytest.raises(TypeError, match="Unknown RAExpression type"):
            dispatch_accept(FakeExpression(), visitor)  # type: ignore


# =============================================================================
# WithExpression Visitor Tests
# =============================================================================


class TestWithExpressionVisitors:
    """Tests for visiting WithExpression (CTE) nodes with all visitor types."""

    def test_collection_visitor_on_with_expression(self) -> None:
        """CollectionVisitor should visit both CTE queries and main query."""
        from alma_algebrakit.models.algebra import CTEDefinition, WithExpression

        cte_relation = Relation(name="base_data", alias="base_data", columns=["x"])
        main_relation = Relation(name="main_table", alias="main_table", columns=["y"])

        with_expr = WithExpression(
            ctes=[
                CTEDefinition(
                    name="cte1",
                    query=cte_relation,
                )
            ],
            main_query=main_relation,
        )

        visitor = TableCollector()
        with_expr.accept(visitor)

        assert "base_data" in visitor.tables
        assert "main_table" in visitor.tables

    def test_transformation_visitor_renames_table_in_cte(self) -> None:
        """TransformationVisitor should transform tables inside CTEs."""
        from alma_algebrakit.models.algebra import CTEDefinition, WithExpression

        cte_relation = Relation(name="old_table", alias="old_table", columns=["x"])
        main_relation = Relation(name="main", alias="main", columns=["y"])

        with_expr = WithExpression(
            ctes=[
                CTEDefinition(
                    name="cte1",
                    query=cte_relation,
                )
            ],
            main_query=main_relation,
        )

        visitor = TableRenamer({"old_table": "new_table"})
        result = with_expr.accept(visitor)

        assert isinstance(result, WithExpression)
        cte_query = result.ctes[0].query
        assert isinstance(cte_query, Relation)
        assert cte_query.name == "new_table"
        # Main query unchanged
        assert isinstance(result.main_query, Relation)
        assert result.main_query.name == "main"

    def test_transformation_visitor_renames_table_in_main_query(self) -> None:
        """TransformationVisitor should transform tables in the main query."""
        from alma_algebrakit.models.algebra import CTEDefinition, WithExpression

        cte_relation = Relation(name="cte_source", alias="cte_source", columns=["x"])
        main_relation = Relation(name="old_table", alias="old_table", columns=["y"])

        with_expr = WithExpression(
            ctes=[
                CTEDefinition(
                    name="cte1",
                    query=cte_relation,
                )
            ],
            main_query=main_relation,
        )

        visitor = TableRenamer({"old_table": "renamed_table"})
        result = with_expr.accept(visitor)

        assert isinstance(result, WithExpression)
        assert isinstance(result.main_query, Relation)
        assert result.main_query.name == "renamed_table"

    def test_fingerprint_visitor_on_with_expression(self) -> None:
        """FingerprintVisitor should produce consistent fingerprints for CTEs."""
        from alma_algebrakit.models.algebra import CTEDefinition, WithExpression

        cte_relation = Relation(name="src", alias="src", columns=["a"])
        main_relation = Relation(name="dst", alias="dst", columns=["b"])

        with_expr = WithExpression(
            ctes=[
                CTEDefinition(
                    name="cte1",
                    query=cte_relation,
                )
            ],
            main_query=main_relation,
        )

        visitor = FingerprintVisitor()
        fp = with_expr.accept(visitor)

        assert "R(src)" in fp
        assert "R(dst)" in fp

    def test_transformation_visitor_no_change_returns_same_object(self) -> None:
        """When no transformation occurs, should return the same object."""
        from alma_algebrakit.models.algebra import CTEDefinition, WithExpression

        cte_relation = Relation(name="src", alias="src", columns=["x"])
        main_relation = Relation(name="main", alias="main", columns=["y"])

        with_expr = WithExpression(
            ctes=[
                CTEDefinition(
                    name="cte1",
                    query=cte_relation,
                )
            ],
            main_query=main_relation,
        )

        # Rename a table that doesn't exist - no change should occur
        visitor = TableRenamer({"nonexistent": "new_name"})
        result = with_expr.accept(visitor)

        assert result is with_expr  # Same object returned
