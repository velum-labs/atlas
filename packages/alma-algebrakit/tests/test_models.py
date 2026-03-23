"""Tests for algebrakit model types."""

from alma_algebrakit import (
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
    Selection,
)


class TestRelation:
    """Tests for Relation type."""

    def test_create_simple_relation(self):
        r = Relation(name="users")
        assert r.name == "users"
        assert r.alias is None
        assert r.schema_name is None  # Field is schema_name not schema

    def test_create_relation_with_alias(self):
        r = Relation(name="users", alias="u")
        assert r.name == "users"
        assert r.alias == "u"

    def test_create_relation_with_schema(self):
        r = Relation(name="users", schema_name="public")  # Field is schema_name
        assert r.schema_name == "public"
        assert r.name == "users"


class TestPredicate:
    """Tests for Predicate types."""

    def test_atomic_predicate(self):
        p = AtomicPredicate(
            left=ColumnRef(column="id"),  # Field is 'column' not 'name'
            op=ComparisonOp.EQ,
            right=Literal(value=1),
        )
        assert p.op == ComparisonOp.EQ
        assert isinstance(p.left, ColumnRef)

    def test_compound_predicate(self):
        p1 = AtomicPredicate(
            left=ColumnRef(column="id"),
            op=ComparisonOp.GT,
            right=Literal(value=0),
        )
        p2 = AtomicPredicate(
            left=ColumnRef(column="active"),
            op=ComparisonOp.EQ,
            right=Literal(value=True),
        )
        compound = CompoundPredicate(
            op=LogicalOp.AND,
            operands=[p1, p2],
        )
        assert compound.op == LogicalOp.AND
        assert len(compound.operands) == 2


class TestSelection:
    """Tests for Selection operator."""

    def test_create_selection(self):
        r = Relation(name="users")
        p = AtomicPredicate(
            left=ColumnRef(column="active"),
            op=ComparisonOp.EQ,
            right=Literal(value=True),
        )
        s = Selection(input=r, predicate=p)
        assert isinstance(s.input, Relation)
        assert isinstance(s.predicate, AtomicPredicate)


class TestProjection:
    """Tests for Projection operator."""

    def test_create_projection(self):
        r = Relation(name="users")
        # Projection columns are tuples of (Expression, alias | None)
        p = Projection(
            input=r,
            columns=[
                (ColumnRef(column="id"), None),
                (ColumnRef(column="name"), "user_name"),
            ],
        )
        assert len(p.columns) == 2


class TestJoin:
    """Tests for Join operator."""

    def test_inner_join(self):
        left = Relation(name="orders")
        right = Relation(name="users")
        j = Join(
            left=left,
            right=right,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(column="user_id", table="orders"),
                op=ComparisonOp.EQ,
                right=ColumnRef(column="id", table="users"),
            ),
        )
        assert j.join_type == JoinType.INNER

    def test_left_join(self):
        left = Relation(name="orders")
        right = Relation(name="users")
        j = Join(
            left=left,
            right=right,
            join_type=JoinType.LEFT,
            condition=AtomicPredicate(
                left=ColumnRef(column="user_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(column="id"),
            ),
        )
        assert j.join_type == JoinType.LEFT


class TestAggregation:
    """Tests for Aggregation operator."""

    def test_simple_aggregation(self):
        r = Relation(name="orders")
        # AggregateSpec uses 'function' and 'argument' fields
        agg = Aggregation(
            input=r,
            group_by=[ColumnRef(column="user_id")],
            aggregates=[
                AggregateSpec(
                    function=AggregateFunction.COUNT,
                    argument=ColumnRef(column="id"),
                    alias="order_count",
                ),
            ],
        )
        assert len(agg.group_by) == 1
        assert len(agg.aggregates) == 1
        assert agg.aggregates[0].function == AggregateFunction.COUNT


class TestRAExpressionNesting:
    """Tests for nesting RA expressions."""

    def test_nested_selection_projection(self):
        """Test Selection -> Projection -> Relation."""
        r = Relation(name="users")
        s = Selection(
            input=r,
            predicate=AtomicPredicate(
                left=ColumnRef(column="active"),
                op=ComparisonOp.EQ,
                right=Literal(value=True),
            ),
        )
        p = Projection(
            input=s,
            columns=[(ColumnRef(column="id"), None), (ColumnRef(column="name"), None)],
        )
        assert isinstance(p.input, Selection)
        assert isinstance(p.input.input, Relation)

    def test_join_with_selections(self):
        """Test Join with Selections on both inputs."""
        users = Selection(
            input=Relation(name="users"),
            predicate=AtomicPredicate(
                left=ColumnRef(column="active"),
                op=ComparisonOp.EQ,
                right=Literal(value=True),
            ),
        )
        orders = Selection(
            input=Relation(name="orders"),
            predicate=AtomicPredicate(
                left=ColumnRef(column="status"),
                op=ComparisonOp.EQ,
                right=Literal(value="completed"),
            ),
        )
        j = Join(
            left=users,
            right=orders,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(column="id", table="users"),
                op=ComparisonOp.EQ,
                right=ColumnRef(column="user_id", table="orders"),
            ),
        )
        assert isinstance(j.left, Selection)
        assert isinstance(j.right, Selection)
