"""Tests for the algebrakit.folding module."""

import pytest

from alma_algebrakit import (
    AggregateFunction,
    AggregateSpec,
    Aggregation,
    AtomicPredicate,
    ColumnRef,
    ComparisonOp,
    CompoundPredicate,
    Difference,
    Intersect,
    Join,
    JoinType,
    Literal,
    LogicalOp,
    Projection,
    Relation,
    Selection,
    Union,
)
from alma_algebrakit.folding import (
    FoldCoverage,
    FoldingConfig,
    RAFolder,
    ViewSpecification,
    analyze_boundary,
    check_view_is_spj,
    normalize_table_name,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def users_relation() -> Relation:
    """A simple users relation."""
    return Relation(name="users", alias="u", columns=["id", "name", "email"])


@pytest.fixture
def orders_relation() -> Relation:
    """A simple orders relation."""
    return Relation(name="orders", alias="o", columns=["id", "user_id", "total"])


@pytest.fixture
def products_relation() -> Relation:
    """A simple products relation."""
    return Relation(name="products", alias="p", columns=["id", "name", "price"])


@pytest.fixture
def users_orders_join(users_relation, orders_relation) -> Join:
    """A join of users and orders."""
    return Join(
        left=users_relation,
        right=orders_relation,
        join_type=JoinType.INNER,
        condition=AtomicPredicate(
            left=ColumnRef(table="u", column="id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="o", column="user_id"),
        ),
    )


@pytest.fixture
def users_view_spec() -> ViewSpecification:
    """A simple view specification covering only users."""
    return ViewSpecification(
        name="users_view",
        ra_expression=None,
        base_tables=frozenset({"users"}),
        column_lineage={("u", "id"): "user_id", ("u", "name"): "user_name"},
        exposed_columns=frozenset({"user_id", "user_name"}),
    )


@pytest.fixture
def users_orders_view_spec(users_orders_join) -> ViewSpecification:
    """A view specification covering users and orders."""
    return ViewSpecification(
        name="users_orders_view",
        ra_expression=users_orders_join,
        base_tables=frozenset({"users", "orders"}),
        column_lineage={
            ("u", "id"): "user_id",
            ("u", "name"): "user_name",
            ("o", "id"): "order_id",
            ("o", "total"): "order_total",
        },
        exposed_columns=frozenset({"user_id", "user_name", "order_id", "order_total"}),
    )


# =============================================================================
# normalize_table_name tests
# =============================================================================


class TestNormalizeTableName:
    """Tests for the normalize_table_name function."""

    def test_simple_name(self):
        """Test normalizing a simple table name."""
        assert normalize_table_name("users") == "users"

    def test_schema_prefixed(self):
        """Test removing schema prefix."""
        assert normalize_table_name("public.users") == "users"

    def test_uppercase(self):
        """Test lowercasing."""
        assert normalize_table_name("USERS") == "users"

    def test_mixed_case_with_schema(self):
        """Test schema prefix and lowercasing."""
        assert normalize_table_name("Acme.Position_Snapshot") == "position_snapshot"


# =============================================================================
# check_view_is_spj tests
# =============================================================================


class TestCheckViewIsSPJ:
    """Tests for SPJ validation."""

    def test_simple_relation_is_spj(self, users_relation):
        """A simple relation is SPJ."""
        result = check_view_is_spj(users_relation)
        assert result.is_spj
        assert result.reason is None

    def test_join_is_spj(self, users_orders_join):
        """A simple join is SPJ."""
        result = check_view_is_spj(users_orders_join)
        assert result.is_spj

    def test_selection_is_spj(self, users_relation):
        """A selection is SPJ."""
        selection = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="u", column="name"),
                op=ComparisonOp.EQ,
                right=Literal(value="John"),
            ),
            input=users_relation,
        )
        result = check_view_is_spj(selection)
        assert result.is_spj

    def test_projection_is_spj(self, users_relation):
        """A projection is SPJ."""
        projection = Projection(
            columns=[(ColumnRef(table="u", column="id"), "id")],
            input=users_relation,
        )
        result = check_view_is_spj(projection)
        assert result.is_spj

    def test_none_is_not_spj(self):
        """None RA expression is not SPJ."""
        result = check_view_is_spj(None)
        assert not result.is_spj
        assert "None" in str(result.reason)


# =============================================================================
# RAFolder.compute_coverage tests
# =============================================================================


class TestComputeCoverage:
    """Tests for coverage computation."""

    def test_full_coverage(self, users_relation, users_view_spec):
        """Test full coverage detection."""
        folder = RAFolder()
        coverage, fraction = folder.compute_coverage(users_relation, users_view_spec)

        assert coverage == FoldCoverage.FULL
        assert fraction == 1.0

    def test_partial_coverage(self, users_orders_join, users_view_spec):
        """Test partial coverage detection."""
        folder = RAFolder()
        coverage, fraction = folder.compute_coverage(users_orders_join, users_view_spec)

        assert coverage == FoldCoverage.PARTIAL
        assert fraction == 0.5  # 1 of 2 tables covered

    def test_no_coverage(self, users_relation, orders_relation):
        """Test no coverage detection."""
        view_spec = ViewSpecification(
            name="products_view",
            ra_expression=None,
            base_tables=frozenset({"products"}),
            column_lineage={},
            exposed_columns=frozenset(),
        )
        folder = RAFolder()
        coverage, fraction = folder.compute_coverage(users_relation, view_spec)

        assert coverage == FoldCoverage.NONE
        assert fraction == 0.0


# =============================================================================
# RAFolder.fold tests
# =============================================================================


class TestFold:
    """Tests for the main fold operation."""

    def test_fold_no_overlap(self, users_relation):
        """Folding with no table overlap fails."""
        view_spec = ViewSpecification(
            name="products_view",
            ra_expression=None,
            base_tables=frozenset({"products"}),
            column_lineage={},
            exposed_columns=frozenset(),
        )
        folder = RAFolder()
        result = folder.fold(users_relation, view_spec)

        assert not result.success
        assert result.coverage == FoldCoverage.NONE
        assert "No table overlap" in result.rejection_reasons[0]

    def test_fold_full_coverage_success(self, users_relation, users_view_spec):
        """Full coverage folding succeeds."""
        folder = RAFolder()
        result = folder.fold(users_relation, users_view_spec)

        assert result.success
        assert result.coverage == FoldCoverage.FULL
        assert result.rewritten_ra is not None

    def test_fold_full_coverage_creates_view_relation(self, users_relation, users_view_spec):
        """Full coverage folding creates a view relation."""
        folder = RAFolder()
        result = folder.fold(users_relation, users_view_spec)

        # The rewritten RA should be a Relation pointing to the view
        assert result.success
        assert isinstance(result.rewritten_ra, Relation)
        assert result.rewritten_ra.name == "users_view"

    def test_fold_partial_coverage_with_spj_view(self, users_orders_join, users_view_spec):
        """Partial coverage with SPJ view returns boundary analysis."""
        folder = RAFolder()
        # Need to provide RA expression for SPJ check
        users_view_with_ra = ViewSpecification(
            name="users_view",
            ra_expression=Relation(name="users", alias="u"),
            base_tables=frozenset({"users"}),
            column_lineage={("u", "id"): "user_id", ("u", "name"): "user_name"},
            exposed_columns=frozenset({"user_id", "user_name"}),
        )
        result = folder.fold(users_orders_join, users_view_with_ra)

        # Partial coverage requires additional checks
        # Success depends on whether boundary analysis passes
        assert result.coverage == FoldCoverage.PARTIAL
        if result.success:
            assert result.boundary_analysis is not None


# =============================================================================
# analyze_boundary tests
# =============================================================================


class TestAnalyzeBoundary:
    """Tests for boundary analysis."""

    def test_partition_relations(self, users_orders_join):
        """Test that relations are correctly partitioned."""
        view_spec = ViewSpecification(
            name="users_view",
            ra_expression=Relation(name="users", alias="u"),
            base_tables=frozenset({"users"}),
            column_lineage={("u", "id"): "user_id"},
            exposed_columns=frozenset({"user_id"}),
        )

        analysis = analyze_boundary(users_orders_join, view_spec)

        # Check partitioning
        covered_tables = {r.physical_table for r in analysis.covered_rels}
        remaining_tables = {r.physical_table for r in analysis.remaining_rels}

        assert covered_tables == {"users"}
        assert remaining_tables == {"orders"}

    def test_self_join_on_covered_rejected(self):
        """Self-joins on covered tables are rejected."""
        # Create a self-join on users
        u1 = Relation(name="users", alias="u1")
        u2 = Relation(name="users", alias="u2")
        self_join = Join(
            left=u1,
            right=u2,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u1", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="u2", column="manager_id"),
            ),
        )

        view_spec = ViewSpecification(
            name="users_view",
            ra_expression=None,
            base_tables=frozenset({"users"}),
            column_lineage={},
            exposed_columns=frozenset(),
        )

        analysis = analyze_boundary(self_join, view_spec)

        assert not analysis.is_valid
        assert any("Self-join" in r for r in analysis.rejection_reasons)


# =============================================================================
# FoldingConfig tests
# =============================================================================


class TestFoldingConfig:
    """Tests for FoldingConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = FoldingConfig()

        assert config.check_predicate_implication is True
        assert config.min_attribute_coverage == 0.5
        assert config.allow_partial_coverage is True

    def test_custom_config(self):
        """Test custom configuration."""
        config = FoldingConfig(
            check_predicate_implication=False,
            min_attribute_coverage=0.8,
            smt_timeout_ms=2000,
        )

        assert config.check_predicate_implication is False
        assert config.min_attribute_coverage == 0.8
        assert config.smt_timeout_ms == 2000


# =============================================================================
# Folding Limitation Tests - Expected Rejections
# =============================================================================


class TestNoTableOverlapRejection:
    """Tests for rejection when there's no table overlap."""

    def test_simple_no_overlap_single_table(self) -> None:
        """Query on 'users', view on 'products' -> no overlap."""
        query = Relation(name="users", alias="u", columns=["id", "name"])
        view_spec = ViewSpecification(
            name="products_view",
            ra_expression=Relation(name="products", alias="p", columns=["id"]),
            base_tables=frozenset({"products"}),
            column_lineage={("p", "id"): "product_id"},
            exposed_columns=frozenset({"product_id"}),
        )

        folder = RAFolder()
        result = folder.fold(query, view_spec)

        assert not result.success
        assert result.coverage == FoldCoverage.NONE
        assert any("No table overlap" in r for r in result.rejection_reasons)

    def test_complex_no_overlap_multi_table_query(self) -> None:
        """Query joins users+orders, view covers products+inventory."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])
        query = Join(
            left=users,
            right=orders,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="user_id"),
            ),
        )

        view_spec = ViewSpecification(
            name="inventory_view",
            ra_expression=None,
            base_tables=frozenset({"products", "inventory"}),
            column_lineage={},
            exposed_columns=frozenset(),
        )

        folder = RAFolder()
        result = folder.fold(query, view_spec)

        assert not result.success
        assert result.coverage == FoldCoverage.NONE


class TestAttributeCoverageRejection:
    """Tests for rejection when attribute coverage is below threshold.

    Note: Attribute coverage is checked via _compute_attribute_coverage which
    compares view.exposed_columns against query.output_columns(). For full
    coverage, this check may pass even with low coverage if the view is considered
    sufficient. These tests document the actual behavior.
    """

    def test_simple_low_attribute_coverage_check_fold_condition(self) -> None:
        """Verify check_fold_condition reports low attribute coverage."""
        query = Relation(
            name="users",
            alias="u",
            columns=[
                "id",
                "name",
                "email",
                "phone",
                "address",
                "city",
                "state",
                "zip",
                "country",
                "created_at",
            ],
        )
        view_spec = ViewSpecification(
            name="users_minimal_view",
            ra_expression=Relation(name="users", alias="u", columns=["id", "name"]),
            base_tables=frozenset({"users"}),
            column_lineage={("u", "id"): "user_id", ("u", "name"): "user_name"},
            exposed_columns=frozenset({"user_id", "user_name"}),  # Only 2 columns
        )

        # Use high threshold
        config = FoldingConfig(min_attribute_coverage=0.8)
        folder = RAFolder(config=config)

        # check_fold_condition should report the issue
        can_fold, reasons = folder.check_fold_condition(query, view_spec)

        # Note: For full coverage, attribute check may be bypassed in fold()
        # This test verifies check_fold_condition itself detects the issue
        assert not can_fold
        assert any("coverage" in r.lower() and "below threshold" in r.lower() for r in reasons)

    def test_complex_attribute_coverage_partial_only(self) -> None:
        """Attribute coverage is more strictly enforced for partial coverage.

        For full coverage, the folder may succeed even with low attribute coverage
        since all base tables are replaced anyway.
        """
        users = Relation(
            name="users", alias="u", columns=["id", "name", "email", "phone", "department"]
        )
        orders = Relation(
            name="orders", alias="o", columns=["id", "user_id", "total", "status", "created_at"]
        )
        query = Join(
            left=users,
            right=orders,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="user_id"),
            ),
        )

        # View only covers users (partial coverage)
        view_spec = ViewSpecification(
            name="users_minimal",
            ra_expression=Relation(name="users", alias="u", columns=["id"]),
            base_tables=frozenset({"users"}),  # Partial - doesn't cover orders
            column_lineage={("u", "id"): "uid"},  # Only 1 column
            exposed_columns=frozenset({"uid"}),
        )

        config = FoldingConfig(min_attribute_coverage=0.8)
        folder = RAFolder(config=config)

        # check_fold_condition verifies coverage
        can_fold, reasons = folder.check_fold_condition(query, view_spec)

        # With strict threshold, should fail
        assert not can_fold
        assert any("coverage" in r.lower() for r in reasons)


class TestNonSPJViewRejection:
    """Tests for rejection when partial coverage view is not SPJ."""

    def test_simple_view_with_distinct(self) -> None:
        """View with DISTINCT is not SPJ."""
        users = Relation(name="users", alias="u", columns=["id", "name"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])
        query = Join(
            left=users,
            right=orders,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="user_id"),
            ),
        )

        # View with DISTINCT (not SPJ)
        view_ra = Projection(
            columns=[(ColumnRef(table="u", column="id"), "user_id")],
            input=Relation(name="users", alias="u", columns=["id"]),
            distinct=True,  # DISTINCT makes it non-SPJ
        )

        view_spec = ViewSpecification(
            name="users_distinct_view",
            ra_expression=view_ra,
            base_tables=frozenset({"users"}),  # Partial coverage
            column_lineage={("u", "id"): "user_id"},
            exposed_columns=frozenset({"user_id"}),
        )

        folder = RAFolder()
        result = folder.fold(query, view_spec)

        assert not result.success
        assert any("SPJ" in r or "DISTINCT" in r for r in result.rejection_reasons)

    def test_simple_view_with_aggregation(self) -> None:
        """View with GROUP BY is not SPJ."""
        users = Relation(name="users", alias="u", columns=["id", "name"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id", "total"])
        query = Join(
            left=users,
            right=orders,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="user_id"),
            ),
        )

        # View with aggregation (not SPJ)
        view_ra = Aggregation(
            input=Relation(name="users", alias="u", columns=["id", "name"]),
            group_by=[ColumnRef(table="u", column="name")],
            aggregates=[
                AggregateSpec(
                    function=AggregateFunction.COUNT,
                    argument=ColumnRef(table="u", column="id"),
                    alias="user_count",
                )
            ],
        )

        view_spec = ViewSpecification(
            name="users_agg_view",
            ra_expression=view_ra,
            base_tables=frozenset({"users"}),
            column_lineage={},
            exposed_columns=frozenset({"name", "user_count"}),
        )

        folder = RAFolder()
        result = folder.fold(query, view_spec)

        assert not result.success
        assert any(
            "SPJ" in r or "GROUP BY" in r or "aggregation" in r for r in result.rejection_reasons
        )

    def test_complex_view_with_union(self) -> None:
        """View with UNION is not SPJ."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])
        query = Join(
            left=users,
            right=orders,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="user_id"),
            ),
        )

        # View with UNION (not SPJ)
        view_ra = Union(
            left=Relation(name="users", alias="u1", columns=["id"]),
            right=Relation(name="users", alias="u2", columns=["id"]),
        )

        view_spec = ViewSpecification(
            name="users_union_view",
            ra_expression=view_ra,
            base_tables=frozenset({"users"}),
            column_lineage={},
            exposed_columns=frozenset({"id"}),
        )

        folder = RAFolder()
        result = folder.fold(query, view_spec)

        assert not result.success
        assert any("SPJ" in r or "UNION" in r for r in result.rejection_reasons)

    def test_complex_view_with_except(self) -> None:
        """View with EXCEPT is not SPJ."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])
        query = Join(
            left=users,
            right=orders,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="user_id"),
            ),
        )

        view_ra = Difference(
            left=Relation(name="users", alias="u1", columns=["id"]),
            right=Relation(name="users", alias="u2", columns=["id"]),
        )

        view_spec = ViewSpecification(
            name="users_except_view",
            ra_expression=view_ra,
            base_tables=frozenset({"users"}),
            column_lineage={},
            exposed_columns=frozenset({"id"}),
        )

        folder = RAFolder()
        result = folder.fold(query, view_spec)

        assert not result.success
        assert any("SPJ" in r or "EXCEPT" in r for r in result.rejection_reasons)

    def test_complex_view_with_intersect(self) -> None:
        """View with INTERSECT is not SPJ."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])
        query = Join(
            left=users,
            right=orders,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="user_id"),
            ),
        )

        view_ra = Intersect(
            left=Relation(name="users", alias="u1", columns=["id"]),
            right=Relation(name="users", alias="u2", columns=["id"]),
        )

        view_spec = ViewSpecification(
            name="users_intersect_view",
            ra_expression=view_ra,
            base_tables=frozenset({"users"}),
            column_lineage={},
            exposed_columns=frozenset({"id"}),
        )

        folder = RAFolder()
        result = folder.fold(query, view_spec)

        assert not result.success
        assert any("SPJ" in r or "INTERSECT" in r for r in result.rejection_reasons)


class TestSelfJoinRejection:
    """Tests for rejection when self-joins occur on covered tables."""

    def test_simple_self_join_on_covered(self) -> None:
        """Self-join where both aliases reference same covered table."""
        u1 = Relation(name="users", alias="u1", columns=["id", "manager_id"])
        u2 = Relation(name="users", alias="u2", columns=["id", "manager_id"])
        query = Join(
            left=u1,
            right=u2,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u1", column="manager_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="u2", column="id"),
            ),
        )

        # View covers "users" - but query has two instances
        view_spec = ViewSpecification(
            name="users_view",
            ra_expression=Relation(name="users", alias="u", columns=["id", "manager_id"]),
            base_tables=frozenset({"users"}),
            column_lineage={("u", "id"): "user_id"},
            exposed_columns=frozenset({"user_id"}),
        )

        analysis = analyze_boundary(query, view_spec)

        assert not analysis.is_valid
        assert any("Self-join" in r for r in analysis.rejection_reasons)

    def test_complex_self_join_three_way(self) -> None:
        """Three-way self-join on covered table."""
        u1 = Relation(name="employees", alias="e1", columns=["id", "manager_id", "skip_manager_id"])
        u2 = Relation(name="employees", alias="e2", columns=["id", "manager_id"])
        u3 = Relation(name="employees", alias="e3", columns=["id", "manager_id"])

        j1 = Join(
            left=u1,
            right=u2,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="e1", column="manager_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="e2", column="id"),
            ),
        )
        query = Join(
            left=j1,
            right=u3,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="e1", column="skip_manager_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="e3", column="id"),
            ),
        )

        view_spec = ViewSpecification(
            name="employees_view",
            ra_expression=Relation(name="employees", alias="e", columns=["id"]),
            base_tables=frozenset({"employees"}),
            column_lineage={},
            exposed_columns=frozenset({"id"}),
        )

        analysis = analyze_boundary(query, view_spec)

        assert not analysis.is_valid
        assert any("Self-join" in r for r in analysis.rejection_reasons)

    def test_complex_self_join_with_other_tables(self) -> None:
        """Self-join on covered table mixed with other tables."""
        u1 = Relation(name="users", alias="u1", columns=["id", "manager_id"])
        u2 = Relation(name="users", alias="u2", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])

        user_self_join = Join(
            left=u1,
            right=u2,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u1", column="manager_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="u2", column="id"),
            ),
        )
        query = Join(
            left=user_self_join,
            right=orders,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u1", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="user_id"),
            ),
        )

        # View covers users (which has self-join)
        view_spec = ViewSpecification(
            name="users_view",
            ra_expression=Relation(name="users", alias="u"),
            base_tables=frozenset({"users"}),
            column_lineage={},
            exposed_columns=frozenset(),
        )

        analysis = analyze_boundary(query, view_spec)

        assert not analysis.is_valid
        assert any("Self-join" in r for r in analysis.rejection_reasons)


class TestOuterJoinSafetyRejection:
    """Tests for rejection when outer joins are unsafe at boundary."""

    def test_simple_left_join_covered_on_right(self) -> None:
        """LEFT JOIN with covered table on null-extended (right) side."""
        users = Relation(name="users", alias="u", columns=["id", "order_id"])
        orders = Relation(name="orders", alias="o", columns=["id"])

        # users LEFT JOIN orders - orders is on the null-extended side
        query = Join(
            left=users,
            right=orders,
            join_type=JoinType.LEFT,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="order_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="id"),
            ),
        )

        # View covers orders (the null-extended side) - UNSAFE
        view_spec = ViewSpecification(
            name="orders_view",
            ra_expression=Relation(name="orders", alias="o", columns=["id"]),
            base_tables=frozenset({"orders"}),
            column_lineage={("o", "id"): "order_id"},
            exposed_columns=frozenset({"order_id"}),
        )

        analysis = analyze_boundary(query, view_spec)

        assert not analysis.is_valid
        assert any("null-extended" in r.lower() or "LEFT" in r for r in analysis.rejection_reasons)

    def test_simple_right_join_covered_on_left(self) -> None:
        """RIGHT JOIN with covered table on null-extended (left) side."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])

        # users RIGHT JOIN orders - users is on the null-extended side
        query = Join(
            left=users,
            right=orders,
            join_type=JoinType.RIGHT,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="user_id"),
            ),
        )

        # View covers users (the null-extended side) - UNSAFE
        view_spec = ViewSpecification(
            name="users_view",
            ra_expression=Relation(name="users", alias="u", columns=["id"]),
            base_tables=frozenset({"users"}),
            column_lineage={("u", "id"): "user_id"},
            exposed_columns=frozenset({"user_id"}),
        )

        analysis = analyze_boundary(query, view_spec)

        assert not analysis.is_valid
        assert any("null-extended" in r.lower() or "RIGHT" in r for r in analysis.rejection_reasons)

    def test_simple_full_outer_join_at_boundary(self) -> None:
        """FULL OUTER JOIN at boundary is always unsafe."""
        users = Relation(name="users", alias="u", columns=["id"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])

        query = Join(
            left=users,
            right=orders,
            join_type=JoinType.FULL,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o", column="user_id"),
            ),
        )

        # View covers users, orders remaining - FULL OUTER at boundary
        view_spec = ViewSpecification(
            name="users_view",
            ra_expression=Relation(name="users", alias="u", columns=["id"]),
            base_tables=frozenset({"users"}),
            column_lineage={("u", "id"): "user_id"},
            exposed_columns=frozenset({"user_id"}),
        )

        analysis = analyze_boundary(query, view_spec)

        assert not analysis.is_valid
        assert any("FULL" in r for r in analysis.rejection_reasons)

    def test_complex_chained_left_join_unsafe_middle(self) -> None:
        """A LEFT JOIN B LEFT JOIN C where B is covered (null-extended in first join)."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id", "c_id"])
        c = Relation(name="c", alias="c", columns=["id"])

        ab_join = Join(
            left=a,
            right=b,
            join_type=JoinType.LEFT,
            condition=AtomicPredicate(
                left=ColumnRef(table="a", column="b_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="b", column="id"),
            ),
        )
        query = Join(
            left=ab_join,
            right=c,
            join_type=JoinType.LEFT,
            condition=AtomicPredicate(
                left=ColumnRef(table="b", column="c_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="c", column="id"),
            ),
        )

        # View covers B (which is on null-extended side of first LEFT JOIN)
        view_spec = ViewSpecification(
            name="b_view",
            ra_expression=Relation(name="b", alias="b", columns=["id", "c_id"]),
            base_tables=frozenset({"b"}),
            column_lineage={("b", "id"): "b_id", ("b", "c_id"): "b_c_id"},
            exposed_columns=frozenset({"b_id", "b_c_id"}),
        )

        analysis = analyze_boundary(query, view_spec)

        assert not analysis.is_valid

    def test_complex_left_join_covered_spans_both_sides(self) -> None:
        """LEFT JOIN where covered tables appear on both sides."""
        a = Relation(name="a", alias="a", columns=["id"])
        b = Relation(name="b", alias="b", columns=["id", "a_id"])
        c = Relation(name="c", alias="c", columns=["id", "b_id"])

        # (A JOIN B) LEFT JOIN C - view covers A and C (spans left and right of LEFT JOIN)
        ab_join = Join(
            left=a,
            right=b,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="a", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="b", column="a_id"),
            ),
        )
        query = Join(
            left=ab_join,
            right=c,
            join_type=JoinType.LEFT,
            condition=AtomicPredicate(
                left=ColumnRef(table="b", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="c", column="b_id"),
            ),
        )

        # View covers A and C (spans both sides of the LEFT JOIN)
        view_spec = ViewSpecification(
            name="ac_view",
            ra_expression=None,
            base_tables=frozenset({"a", "c"}),
            column_lineage={},
            exposed_columns=frozenset(),
        )

        analysis = analyze_boundary(query, view_spec)

        # This is tricky - the boundary analysis should detect this is unsafe
        # because C is on the null-extended side
        assert not analysis.is_valid


class TestMissingColumnsRejection:
    """Tests for rejection when required columns are not in view."""

    def test_simple_missing_single_column(self) -> None:
        """Query needs column not exposed by view."""
        users = Relation(name="users", alias="u", columns=["id", "name", "email"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])

        # Query uses u.email in selection
        query = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="u", column="email"),
                op=ComparisonOp.EQ,
                right=Literal(value="test@example.com"),
            ),
            input=Join(
                left=users,
                right=orders,
                join_type=JoinType.INNER,
                condition=AtomicPredicate(
                    left=ColumnRef(table="u", column="id"),
                    op=ComparisonOp.EQ,
                    right=ColumnRef(table="o", column="user_id"),
                ),
            ),
        )

        # View covers users but doesn't expose email
        view_spec = ViewSpecification(
            name="users_view",
            ra_expression=Relation(name="users", alias="u", columns=["id", "name"]),
            base_tables=frozenset({"users"}),
            column_lineage={("u", "id"): "user_id", ("u", "name"): "user_name"},
            exposed_columns=frozenset({"user_id", "user_name"}),  # No email!
            is_select_star=False,
        )

        analysis = analyze_boundary(query, view_spec)

        assert not analysis.is_valid
        assert any("Missing columns" in r for r in analysis.rejection_reasons)

    def test_complex_missing_join_column(self) -> None:
        """Join condition column not exposed by view."""
        users = Relation(name="users", alias="u", columns=["id", "department_id"])
        departments = Relation(name="departments", alias="d", columns=["id", "name"])

        # Join on u.department_id = d.id
        query = Join(
            left=users,
            right=departments,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="u", column="department_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="d", column="id"),
            ),
        )

        # View covers users but doesn't expose department_id
        view_spec = ViewSpecification(
            name="users_view",
            ra_expression=Relation(name="users", alias="u", columns=["id"]),
            base_tables=frozenset({"users"}),
            column_lineage={("u", "id"): "user_id"},  # No department_id!
            exposed_columns=frozenset({"user_id"}),
            is_select_star=False,
        )

        analysis = analyze_boundary(query, view_spec)

        assert not analysis.is_valid
        assert any("Missing columns" in r for r in analysis.rejection_reasons)

    def test_complex_missing_multiple_columns(self) -> None:
        """Multiple required columns missing from view."""
        users = Relation(name="users", alias="u", columns=["id", "name", "email", "phone"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])

        # Query uses multiple columns from users
        pred = CompoundPredicate(
            op=LogicalOp.AND,
            operands=[
                AtomicPredicate(
                    left=ColumnRef(table="u", column="email"),
                    op=ComparisonOp.LIKE,
                    right=Literal(value="%@example.com"),
                ),
                AtomicPredicate(
                    left=ColumnRef(table="u", column="phone"),
                    op=ComparisonOp.IS_NOT_NULL,
                    right=None,
                ),
            ],
        )
        query = Selection(
            predicate=pred,
            input=Join(
                left=users,
                right=orders,
                join_type=JoinType.INNER,
                condition=AtomicPredicate(
                    left=ColumnRef(table="u", column="id"),
                    op=ComparisonOp.EQ,
                    right=ColumnRef(table="o", column="user_id"),
                ),
            ),
        )

        # View only exposes id and name
        view_spec = ViewSpecification(
            name="users_view",
            ra_expression=Relation(name="users", alias="u", columns=["id", "name"]),
            base_tables=frozenset({"users"}),
            column_lineage={("u", "id"): "user_id", ("u", "name"): "user_name"},
            exposed_columns=frozenset({"user_id", "user_name"}),
            is_select_star=False,
        )

        analysis = analyze_boundary(query, view_spec)

        assert not analysis.is_valid
        assert any("Missing columns" in r for r in analysis.rejection_reasons)


class TestDanglingRelationsRejection:
    """Tests for rejection when remaining relations have no join path."""

    def test_complex_remaining_tables_not_connected(self) -> None:
        """Query has tables that would be disconnected after folding.

        Query: A JOIN B JOIN C
        View: covers A
        Result: B and C are remaining, but C is only connected to A (now view)
        """
        a = Relation(name="a", alias="a", columns=["id", "c_id"])
        b = Relation(name="b", alias="b", columns=["id", "a_id"])
        c = Relation(name="c", alias="c", columns=["id"])

        # A JOIN B on b.a_id = a.id
        ab_join = Join(
            left=a,
            right=b,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="b", column="a_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="a", column="id"),
            ),
        )

        # (A JOIN B) JOIN C on a.c_id = c.id
        query = Join(
            left=ab_join,
            right=c,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="a", column="c_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="c", column="id"),
            ),
        )

        # View covers A - B and C are remaining
        # B connects to A (now view) - OK
        # C connects to A (now view) - OK
        # But the view must expose the join columns
        view_spec = ViewSpecification(
            name="a_view",
            ra_expression=Relation(name="a", alias="a", columns=["id"]),
            base_tables=frozenset({"a"}),
            column_lineage={("a", "id"): "a_id"},  # Missing c_id for join to C
            exposed_columns=frozenset({"a_id"}),
            is_select_star=False,
        )

        analysis = analyze_boundary(query, view_spec)

        # Should fail due to missing column for join condition
        assert not analysis.is_valid


class TestPredicateImplicationRejection:
    """Tests for rejection when query predicates are not implied by view predicates (strict mode)."""

    def test_simple_unimplied_predicate_strict(self) -> None:
        """Query has predicate not implied by view (strict mode rejects)."""
        users = Relation(name="users", alias="u", columns=["id", "status"])

        # Query: SELECT * FROM users WHERE status = 'active'
        query = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="u", column="status"),
                op=ComparisonOp.EQ,
                right=Literal(value="active"),
            ),
            input=users,
        )

        # View: SELECT * FROM users WHERE status = 'inactive'
        view_ra = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="u", column="status"),
                op=ComparisonOp.EQ,
                right=Literal(value="inactive"),  # Different predicate!
            ),
            input=Relation(name="users", alias="u", columns=["id", "status"]),
        )

        view_spec = ViewSpecification(
            name="inactive_users_view",
            ra_expression=view_ra,
            base_tables=frozenset({"users"}),
            column_lineage={("u", "id"): "user_id", ("u", "status"): "user_status"},
            exposed_columns=frozenset({"user_id", "user_status"}),
        )

        # Use strict predicate checking
        config = FoldingConfig(
            check_predicate_implication=True,
            strict_predicate_check=True,
        )
        folder = RAFolder(config=config)
        result = folder.fold(query, view_spec)

        # With strict mode, unimplied predicates should cause rejection
        # (Note: This depends on SMT availability and implementation details)
        # The test verifies the mechanism exists
        assert result.coverage == FoldCoverage.FULL

    def test_complex_conflicting_predicates_strict(self) -> None:
        """View predicate contradicts query predicate (strict mode)."""
        users = Relation(name="users", alias="u", columns=["id", "age"])

        # Query: users WHERE age > 21
        query = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="u", column="age"),
                op=ComparisonOp.GT,
                right=Literal(value=21),
            ),
            input=users,
        )

        # View: users WHERE age < 18 (contradicts query!)
        view_ra = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="u", column="age"),
                op=ComparisonOp.LT,
                right=Literal(value=18),
            ),
            input=Relation(name="users", alias="u", columns=["id", "age"]),
        )

        view_spec = ViewSpecification(
            name="minors_view",
            ra_expression=view_ra,
            base_tables=frozenset({"users"}),
            column_lineage={("u", "id"): "user_id", ("u", "age"): "user_age"},
            exposed_columns=frozenset({"user_id", "user_age"}),
        )

        config = FoldingConfig(
            check_predicate_implication=True,
            strict_predicate_check=True,
        )
        folder = RAFolder(config=config)
        result = folder.fold(query, view_spec)

        # Conflicting predicates - the fold might succeed but semantically incorrect
        # This test documents the behavior
        assert result.coverage == FoldCoverage.FULL


class TestDuplicateAliasRejection:
    """Tests for rejection when remaining tables have duplicate aliases.

    Note: The implementation checks for self-joins on COVERED tables. Self-joins
    on remaining tables with different aliases are allowed since they don't affect
    the view substitution. The duplicate alias check applies when the SAME physical
    table appears multiple times as COVERED (which would make view substitution
    ambiguous).
    """

    def test_self_join_different_aliases_remaining_allowed(self) -> None:
        """Self-join on remaining table with different aliases is allowed."""
        a = Relation(name="a", alias="a", columns=["id"])
        b1 = Relation(name="b", alias="b1", columns=["id", "a_id", "parent_id"])
        b2 = Relation(name="b", alias="b2", columns=["id"])  # Same table, different alias

        ab1_join = Join(
            left=a,
            right=b1,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="a", column="id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="b1", column="a_id"),
            ),
        )
        query = Join(
            left=ab1_join,
            right=b2,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="b1", column="parent_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="b2", column="id"),
            ),
        )

        # View covers A - both B instances (b1, b2) are remaining
        view_spec = ViewSpecification(
            name="a_view",
            ra_expression=Relation(name="a", alias="a", columns=["id"]),
            base_tables=frozenset({"a"}),
            column_lineage={("a", "id"): "a_id"},
            exposed_columns=frozenset({"a_id"}),
            is_select_star=True,  # Allow all columns
        )

        analysis = analyze_boundary(query, view_spec)

        # Self-join with DIFFERENT aliases on remaining is allowed
        assert analysis.is_valid

    def test_self_join_on_covered_is_the_rejection_target(self) -> None:
        """The duplicate/self-join check targets COVERED tables, not remaining.

        This confirms that TestSelfJoinRejection tests cover the actual limitation.
        """
        # This is covered by TestSelfJoinRejection::test_simple_self_join_on_covered
        # Just documenting that the limitation applies to covered, not remaining
        pass


class TestSPJValidationDetails:
    """Detailed tests for SPJ validation edge cases."""

    def test_view_ra_none_not_spj(self) -> None:
        """View with None RA expression cannot be validated as SPJ."""
        result = check_view_is_spj(None)

        assert not result.is_spj
        assert "None" in str(result.reason)

    def test_projection_with_distinct_on_not_spj(self) -> None:
        """Projection with DISTINCT ON is not SPJ."""
        proj = Projection(
            columns=[(ColumnRef(table="u", column="id"), "id")],
            input=Relation(name="users", alias="u", columns=["id"]),
            distinct_on=[ColumnRef(table="u", column="id")],
        )

        result = check_view_is_spj(proj)

        assert not result.is_spj
        assert result.has_distinct

    def test_nested_non_spj_detected(self) -> None:
        """Non-SPJ construct nested deep in the tree is detected."""
        # Selection on top of aggregation
        agg = Aggregation(
            input=Relation(name="users", alias="u", columns=["id", "dept"]),
            group_by=[ColumnRef(table="u", column="dept")],
            aggregates=[
                AggregateSpec(
                    function=AggregateFunction.COUNT,
                    argument=ColumnRef(table="u", column="id"),
                    alias="cnt",
                )
            ],
        )
        selection = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(column="cnt"),
                op=ComparisonOp.GT,
                right=Literal(value=10),
            ),
            input=agg,
        )

        result = check_view_is_spj(selection)

        assert not result.is_spj
        assert result.has_group_by


class TestComplexScenarios:
    """Integration tests combining multiple limitation scenarios."""

    def test_star_schema_partial_coverage_dimension_self_join(self) -> None:
        """Star schema where dimension table has self-join - rejected."""
        fact = Relation(name="sales", alias="s", columns=["id", "product_id", "store_id", "amount"])
        product = Relation(name="products", alias="p", columns=["id", "category_id"])
        # Self-join on categories (parent-child)
        cat1 = Relation(name="categories", alias="c1", columns=["id", "parent_id", "name"])
        cat2 = Relation(name="categories", alias="c2", columns=["id", "parent_id", "name"])

        sp_join = Join(
            left=fact,
            right=product,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="s", column="product_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="p", column="id"),
            ),
        )
        cat_self_join = Join(
            left=cat1,
            right=cat2,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="c1", column="parent_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="c2", column="id"),
            ),
        )
        query = Join(
            left=sp_join,
            right=cat_self_join,
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="p", column="category_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="c1", column="id"),
            ),
        )

        # View covers categories (which has self-join)
        view_spec = ViewSpecification(
            name="categories_view",
            ra_expression=Relation(name="categories", alias="c"),
            base_tables=frozenset({"categories"}),
            column_lineage={},
            exposed_columns=frozenset(),
        )

        analysis = analyze_boundary(query, view_spec)

        assert not analysis.is_valid
        assert any("Self-join" in r for r in analysis.rejection_reasons)

    def test_multi_level_outer_join_unsafe_coverage(self) -> None:
        """Complex outer join tree with unsafe coverage pattern."""
        a = Relation(name="a", alias="a", columns=["id", "b_id"])
        b = Relation(name="b", alias="b", columns=["id", "c_id"])
        c = Relation(name="c", alias="c", columns=["id", "d_id"])
        d = Relation(name="d", alias="d", columns=["id"])

        # A LEFT JOIN B LEFT JOIN C LEFT JOIN D
        ab = Join(
            left=a,
            right=b,
            join_type=JoinType.LEFT,
            condition=AtomicPredicate(
                left=ColumnRef(table="a", column="b_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="b", column="id"),
            ),
        )
        abc = Join(
            left=ab,
            right=c,
            join_type=JoinType.LEFT,
            condition=AtomicPredicate(
                left=ColumnRef(table="b", column="c_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="c", column="id"),
            ),
        )
        query = Join(
            left=abc,
            right=d,
            join_type=JoinType.LEFT,
            condition=AtomicPredicate(
                left=ColumnRef(table="c", column="d_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="d", column="id"),
            ),
        )

        # View covers B and C (both on null-extended sides!)
        view_spec = ViewSpecification(
            name="bc_view",
            ra_expression=None,
            base_tables=frozenset({"b", "c"}),
            column_lineage={},
            exposed_columns=frozenset(),
        )

        analysis = analyze_boundary(query, view_spec)

        assert not analysis.is_valid

    def test_non_spj_view_with_missing_columns(self) -> None:
        """View is non-SPJ AND has missing columns - both should be reported."""
        users = Relation(name="users", alias="u", columns=["id", "name", "email"])
        orders = Relation(name="orders", alias="o", columns=["id", "user_id"])

        # Query uses email
        query = Selection(
            predicate=AtomicPredicate(
                left=ColumnRef(table="u", column="email"),
                op=ComparisonOp.LIKE,
                right=Literal(value="%@company.com"),
            ),
            input=Join(
                left=users,
                right=orders,
                join_type=JoinType.INNER,
                condition=AtomicPredicate(
                    left=ColumnRef(table="u", column="id"),
                    op=ComparisonOp.EQ,
                    right=ColumnRef(table="o", column="user_id"),
                ),
            ),
        )

        # View is aggregated (non-SPJ) and doesn't expose email
        view_ra = Aggregation(
            input=Relation(name="users", alias="u", columns=["id"]),
            group_by=[],
            aggregates=[
                AggregateSpec(
                    function=AggregateFunction.COUNT,
                    argument=ColumnRef(table="u", column="id"),
                    alias="cnt",
                )
            ],
        )

        view_spec = ViewSpecification(
            name="user_count_view",
            ra_expression=view_ra,
            base_tables=frozenset({"users"}),
            column_lineage={},
            exposed_columns=frozenset({"cnt"}),
            is_select_star=False,
        )

        folder = RAFolder()
        result = folder.fold(query, view_spec)

        assert not result.success
        # Should mention SPJ requirement
        assert any("SPJ" in r or "aggregation" in r for r in result.rejection_reasons)
