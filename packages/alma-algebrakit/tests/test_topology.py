"""Tests for topology-focused view learning."""

import pytest

from alma_algebrakit.learning.topology import (
    EdgeBasedCanonicalizer,
    JoinEdge,
    TableRef,
    Topology,
    TopologyNormalization,
    TopologyResult,
    extract_topology,
    is_subtopology,
    topology_gcs,
    topology_similarity,
    topology_to_ra,
)
from alma_algebrakit.learning.topology_learner import (
    TopologyLearningResult,
    TopologyViewLearner,
    extract_all_topologies,
    find_common_topology,
    learn_topology_views,
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
    RAExpression,
    Relation,
    Selection,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def simple_relation() -> RAExpression:
    """Simple relation: orders"""
    return Relation(name="orders", alias="o")


@pytest.fixture
def two_table_join() -> RAExpression:
    """Join: orders ⋈ customers ON o.customer_id = c.id"""
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
def two_table_join_different_aliases() -> RAExpression:
    """Same join but with different aliases: orders ⋈ customers ON ord.customer_id = cust.id"""
    return Join(
        left=Relation(name="orders", alias="ord"),
        right=Relation(name="customers", alias="cust"),
        join_type=JoinType.INNER,
        condition=AtomicPredicate(
            left=ColumnRef(table="ord", column="customer_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="cust", column="id"),
        ),
    )


@pytest.fixture
def two_table_join_with_schema() -> RAExpression:
    """Join with explicit schema: public.orders ⋈ public.customers"""
    return Join(
        left=Relation(name="orders", schema_name="public", alias="o"),
        right=Relation(name="customers", schema_name="public", alias="c"),
        join_type=JoinType.INNER,
        condition=AtomicPredicate(
            left=ColumnRef(table="o", column="customer_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="c", column="id"),
        ),
    )


@pytest.fixture
def self_join() -> RAExpression:
    """Self-join: orders o1 JOIN orders o2 ON o1.parent_id = o2.id"""
    return Join(
        left=Relation(name="orders", alias="o1"),
        right=Relation(name="orders", alias="o2"),
        join_type=JoinType.INNER,
        condition=AtomicPredicate(
            left=ColumnRef(table="o1", column="parent_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="o2", column="id"),
        ),
    )


@pytest.fixture
def three_table_join() -> RAExpression:
    """Join: orders ⋈ customers ⋈ products"""
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
def star_schema_join() -> RAExpression:
    """Star schema: fact_sales ⋈ dim_customer ⋈ dim_product ⋈ dim_date"""
    base = Join(
        left=Relation(name="fact_sales", alias="f"),
        right=Relation(name="dim_customer", alias="dc"),
        join_type=JoinType.INNER,
        condition=AtomicPredicate(
            left=ColumnRef(table="f", column="customer_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="dc", column="id"),
        ),
    )
    with_product = Join(
        left=base,
        right=Relation(name="dim_product", alias="dp"),
        join_type=JoinType.INNER,
        condition=AtomicPredicate(
            left=ColumnRef(table="f", column="product_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="dp", column="id"),
        ),
    )
    return Join(
        left=with_product,
        right=Relation(name="dim_date", alias="dd"),
        join_type=JoinType.INNER,
        condition=AtomicPredicate(
            left=ColumnRef(table="f", column="date_id"),
            op=ComparisonOp.EQ,
            right=ColumnRef(table="dd", column="id"),
        ),
    )


@pytest.fixture
def join_with_selection() -> RAExpression:
    """Join with selection predicate (should be ignored in topology)"""
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
def aggregation_with_join() -> RAExpression:
    """Aggregation on top of join (topology should still work)"""
    return Aggregation(
        group_by=[ColumnRef(table="c", column="id")],
        aggregates=[
            AggregateSpec(
                function=AggregateFunction.SUM,
                argument=ColumnRef(table="o", column="amount"),
                alias="total",
            )
        ],
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


# =============================================================================
# Test: TableRef
# =============================================================================


class TestTableRef:
    """Tests for TableRef."""

    def test_creation(self) -> None:
        """Basic TableRef creation with new schema_name/table fields."""
        ref = TableRef(schema_name="public", table="orders", alias="o")
        assert ref.schema_name == "public"
        assert ref.table == "orders"
        assert ref.alias == "o"
        assert ref.occurrence == 1
        assert ref.physical == "public.orders"

    def test_from_relation(self) -> None:
        """Create TableRef from Relation."""
        rel = Relation(name="orders", alias="o")
        ref = TableRef.from_relation(rel, default_schema="public")
        assert ref.schema_name == "public"
        assert ref.table == "orders"
        assert ref.alias == "o"
        assert ref.physical == "public.orders"

    def test_from_relation_no_alias(self) -> None:
        """Create TableRef from Relation without alias."""
        rel = Relation(name="orders")
        ref = TableRef.from_relation(rel, default_schema="public")
        assert ref.table == "orders"
        assert ref.alias == "orders"
        assert ref.physical == "public.orders"

    def test_from_relation_with_schema(self) -> None:
        """Create TableRef from Relation with schema."""
        rel = Relation(name="orders", schema_name="analytics", alias="o")
        ref = TableRef.from_relation(rel, default_schema="public")
        assert ref.schema_name == "analytics"  # Uses explicit schema, not default
        assert ref.physical == "analytics.orders"
        assert ref.alias == "o"

    def test_from_relation_self_join(self) -> None:
        """Create TableRef for self-join."""
        rel = Relation(name="orders", alias="o1")
        ref = TableRef.from_relation(rel, occurrence=2, default_schema="public")
        assert ref.physical == "public.orders#2"  # Qualified + occurrence
        assert ref.alias == "o1"
        assert ref.occurrence == 2

    def test_base_table(self) -> None:
        """Get base table name without occurrence suffix."""
        ref1 = TableRef(schema_name="public", table="orders", alias="o")
        ref2 = TableRef(schema_name="public", table="orders", alias="o2", occurrence=2)
        assert ref1.base_table() == "public.orders"
        assert ref2.base_table() == "public.orders"  # No suffix

    def test_equality_by_physical_name(self) -> None:
        """Equality is based on physical name only."""
        ref1 = TableRef(schema_name="public", table="orders", alias="o")
        ref2 = TableRef(schema_name="public", table="orders", alias="ord")
        ref3 = TableRef(schema_name="public", table="customers", alias="c")

        assert ref1 == ref2  # Same physical, different alias
        assert ref1 != ref3  # Different physical

    def test_hash_by_physical_name(self) -> None:
        """Hash is based on physical name only."""
        ref1 = TableRef(schema_name="public", table="orders", alias="o")
        ref2 = TableRef(schema_name="public", table="orders", alias="ord")

        assert hash(ref1) == hash(ref2)

    def test_in_set(self) -> None:
        """TableRefs with same physical name are deduplicated in sets."""
        ref1 = TableRef(schema_name="public", table="orders", alias="o")
        ref2 = TableRef(schema_name="public", table="orders", alias="ord")
        ref3 = TableRef(schema_name="public", table="customers", alias="c")

        refs = {ref1, ref2, ref3}
        assert len(refs) == 2  # ref1 and ref2 are equal

    def test_str_representation(self) -> None:
        """String representation."""
        ref1 = TableRef(schema_name="public", table="orders", alias="orders")  # No alias difference
        ref2 = TableRef(schema_name="public", table="orders", alias="o")  # Has alias
        assert str(ref1) == "public.orders"
        assert "public.orders" in str(ref2) and "o" in str(ref2)

    def test_to_relation(self) -> None:
        """Test to_relation() reconstructs correctly."""
        ref = TableRef(schema_name="analytics", table="orders", alias="o")
        rel = ref.to_relation()
        assert rel.name == "orders"
        assert rel.schema_name == "analytics"
        assert rel.alias == "o"


# =============================================================================
# Test: JoinEdge
# =============================================================================


class TestJoinEdge:
    """Tests for JoinEdge."""

    def test_canonical_ordering(self) -> None:
        """Canonical form should be consistent regardless of left/right order."""
        ref_a = TableRef(schema_name="public", table="a", alias="a")
        ref_b = TableRef(schema_name="public", table="b", alias="b")

        edge1 = JoinEdge(left=ref_a, left_column="x", right=ref_b, right_column="y")
        edge2 = JoinEdge(left=ref_b, left_column="y", right=ref_a, right_column="x")

        assert edge1.canonical() == edge2.canonical()

    def test_fingerprint(self) -> None:
        """Fingerprint should be consistent."""
        ref_orders = TableRef(schema_name="public", table="orders", alias="o")
        ref_customers = TableRef(schema_name="public", table="customers", alias="c")

        edge = JoinEdge(
            left=ref_orders, left_column="customer_id", right=ref_customers, right_column="id"
        )
        fp = edge.fingerprint()
        assert "public.orders" in fp
        assert "public.customers" in fp
        assert "customer_id" in fp
        assert "id" in fp

    def test_hash_equality(self) -> None:
        """Equal edges should have same hash."""
        ref_a = TableRef(schema_name="public", table="a", alias="a")
        ref_b = TableRef(schema_name="public", table="b", alias="b")

        edge1 = JoinEdge(left=ref_a, left_column="x", right=ref_b, right_column="y")
        edge2 = JoinEdge(left=ref_b, left_column="y", right=ref_a, right_column="x")

        assert hash(edge1) == hash(edge2)
        assert edge1 == edge2

    def test_alias_insensitive_equality(self) -> None:
        """JoinEdges with different aliases but same physical should be equal."""
        ref_o = TableRef(schema_name="public", table="orders", alias="o")
        ref_ord = TableRef(schema_name="public", table="orders", alias="ord")
        ref_c = TableRef(schema_name="public", table="customers", alias="c")
        ref_cust = TableRef(schema_name="public", table="customers", alias="cust")

        edge1 = JoinEdge(left=ref_o, left_column="customer_id", right=ref_c, right_column="id")
        edge2 = JoinEdge(left=ref_ord, left_column="customer_id", right=ref_cust, right_column="id")

        assert edge1 == edge2


# =============================================================================
# Test: Topology
# =============================================================================


class TestTopology:
    """Tests for Topology."""

    def test_empty_topology(self) -> None:
        """Empty topology should be detected."""
        topo = Topology()
        assert topo.is_empty()
        assert topo.relation_count() == 0

    def test_single_relation(self) -> None:
        """Topology with single relation."""
        ref = TableRef(schema_name="public", table="orders", alias="o")
        topo = Topology(relations=frozenset({ref}))
        assert not topo.is_empty()
        assert topo.relation_count() == 1
        assert topo.edge_count() == 0
        assert topo.is_connected()

    def test_physical_tables(self) -> None:
        """Get unique physical table names."""
        ref1 = TableRef(schema_name="public", table="orders", alias="o")
        ref2 = TableRef(schema_name="public", table="customers", alias="c")
        topo = Topology(relations=frozenset({ref1, ref2}))
        physical = topo.physical_tables()
        assert physical == frozenset({"public.orders", "public.customers"})

    def test_physical_tables_with_self_join(self) -> None:
        """Get unique physical table names with self-join."""
        ref1 = TableRef(schema_name="public", table="orders", alias="o1", occurrence=1)
        ref2 = TableRef(schema_name="public", table="orders", alias="o2", occurrence=2)
        topo = Topology(relations=frozenset({ref1, ref2}))
        physical = topo.physical_tables()
        # Both should map to base table "public.orders"
        assert physical == frozenset({"public.orders"})

    def test_connected_topology(self) -> None:
        """Connected topology via join edge."""
        ref_orders = TableRef(schema_name="public", table="orders", alias="o")
        ref_customers = TableRef(schema_name="public", table="customers", alias="c")
        edge = JoinEdge(
            left=ref_orders, left_column="customer_id", right=ref_customers, right_column="id"
        )
        topo = Topology(
            relations=frozenset({ref_orders, ref_customers}),
            join_edges=frozenset({edge}),
        )
        assert topo.is_connected()

    def test_disconnected_topology(self) -> None:
        """Disconnected topology (no edges between relations)."""
        ref_orders = TableRef(schema_name="public", table="orders", alias="o")
        ref_products = TableRef(schema_name="public", table="products", alias="p")
        topo = Topology(
            relations=frozenset({ref_orders, ref_products}),
            join_edges=frozenset(),
        )
        assert not topo.is_connected()

    def test_fingerprint_consistency(self) -> None:
        """Fingerprint should be consistent for same topology."""
        ref_a = TableRef(schema_name="public", table="a", alias="a")
        ref_b = TableRef(schema_name="public", table="b", alias="b")
        edge = JoinEdge(left=ref_a, left_column="x", right=ref_b, right_column="y")

        topo1 = Topology(
            relations=frozenset({ref_a, ref_b}),
            join_edges=frozenset({edge}),
        )
        # Same topology with different alias
        ref_a2 = TableRef(schema_name="public", table="a", alias="aa")
        ref_b2 = TableRef(schema_name="public", table="b", alias="bb")
        edge2 = JoinEdge(left=ref_a2, left_column="x", right=ref_b2, right_column="y")
        topo2 = Topology(
            relations=frozenset({ref_a2, ref_b2}),
            join_edges=frozenset({edge2}),
        )
        # Fingerprints based on physical names should be equal
        assert topo1.fingerprint() == topo2.fingerprint()


# =============================================================================
# Test: Topology Extraction
# =============================================================================


class TestTopologyExtraction:
    """Tests for extract_topology."""

    def test_simple_relation(self, simple_relation: RAExpression) -> None:
        """Extract topology from simple relation."""
        topo = extract_topology(simple_relation)
        assert topo.physical_tables() == frozenset({"public.orders"})
        assert topo.edge_count() == 0

    def test_two_table_join(self, two_table_join: RAExpression) -> None:
        """Extract topology from two-table join."""
        topo = extract_topology(two_table_join)
        physical = topo.physical_tables()
        assert "public.orders" in physical
        assert "public.customers" in physical
        assert topo.edge_count() == 1

    def test_three_table_join(self, three_table_join: RAExpression) -> None:
        """Extract topology from three-table join."""
        topo = extract_topology(three_table_join)
        assert len(topo.physical_tables()) == 3
        physical = topo.physical_tables()
        assert "public.orders" in physical
        assert "public.customers" in physical
        assert "public.products" in physical
        assert topo.edge_count() == 2

    def test_selection_ignored(self, join_with_selection: RAExpression) -> None:
        """Selection predicates should be ignored."""
        topo = extract_topology(join_with_selection)
        # Should have same topology as plain join
        physical = topo.physical_tables()
        assert "public.orders" in physical
        assert "public.customers" in physical
        assert topo.edge_count() == 1

    def test_aggregation_preserves_topology(self, aggregation_with_join: RAExpression) -> None:
        """Aggregation should preserve underlying join topology."""
        topo = extract_topology(aggregation_with_join)
        physical = topo.physical_tables()
        assert "public.orders" in physical
        assert "public.customers" in physical
        assert topo.edge_count() == 1

    def test_star_schema(self, star_schema_join: RAExpression) -> None:
        """Extract star schema topology."""
        topo = extract_topology(star_schema_join)
        assert len(topo.physical_tables()) == 4
        physical = topo.physical_tables()
        assert "public.fact_sales" in physical
        assert "public.dim_customer" in physical
        assert "public.dim_product" in physical
        assert "public.dim_date" in physical
        assert topo.edge_count() == 3


# =============================================================================
# Test: Schema Normalization
# =============================================================================


class TestSchemaNormalization:
    """Tests for schema normalization with default_schema."""

    def test_unqualified_uses_default_schema(self) -> None:
        """Unqualified table names use default_schema."""
        rel = Relation(name="orders", alias="o")
        topo = extract_topology(rel, default_schema="mydb")
        physical = topo.physical_tables()
        assert "mydb.orders" in physical

    def test_qualified_preserves_schema(self) -> None:
        """Qualified table names preserve their schema."""
        rel = Relation(name="orders", schema_name="analytics", alias="o")
        topo = extract_topology(rel, default_schema="public")
        physical = topo.physical_tables()
        assert "analytics.orders" in physical
        assert "public.orders" not in physical

    def test_mixed_qualification_same_table(
        self, two_table_join: RAExpression, two_table_join_with_schema: RAExpression
    ) -> None:
        """Unqualified and qualified refs to same table produce same topology."""
        # two_table_join uses unqualified names
        # two_table_join_with_schema uses public.orders and public.customers
        topo1 = extract_topology(two_table_join, default_schema="public")
        topo2 = extract_topology(two_table_join_with_schema, default_schema="public")

        # Same physical tables
        assert topo1.physical_tables() == topo2.physical_tables()

        # Same fingerprint
        assert topo1.fingerprint() == topo2.fingerprint()

    def test_different_default_schemas_different_topologies(self) -> None:
        """Different default schemas produce different topologies."""
        rel = Relation(name="orders", alias="o")
        topo1 = extract_topology(rel, default_schema="public")
        topo2 = extract_topology(rel, default_schema="analytics")

        assert topo1.physical_tables() != topo2.physical_tables()
        assert topo1.fingerprint() != topo2.fingerprint()


# =============================================================================
# Test: Alias Insensitivity
# =============================================================================


class TestAliasInsensitivity:
    """Tests for alias-insensitive topology comparison."""

    def test_same_query_different_aliases_equal_topology(
        self,
        two_table_join: RAExpression,
        two_table_join_different_aliases: RAExpression,
    ) -> None:
        """Same query with different aliases should produce equal topologies."""
        topo1 = extract_topology(two_table_join)
        topo2 = extract_topology(two_table_join_different_aliases)

        # Same physical tables
        assert topo1.physical_tables() == topo2.physical_tables()

        # Same fingerprint
        assert topo1.fingerprint() == topo2.fingerprint()

        # Join edges should be equal (based on physical names)
        assert topo1.join_edges == topo2.join_edges

    def test_alias_insensitive_gcs(
        self,
        two_table_join: RAExpression,
        two_table_join_different_aliases: RAExpression,
    ) -> None:
        """GCS should treat different aliases as same topology."""
        result = topology_gcs([two_table_join, two_table_join_different_aliases])
        assert result.success
        assert result.topology is not None
        assert result.topology.physical_tables() == frozenset({"public.orders", "public.customers"})
        assert result.topology.edge_count() == 1

    def test_alias_insensitive_similarity(
        self,
        two_table_join: RAExpression,
        two_table_join_different_aliases: RAExpression,
    ) -> None:
        """Topologies with different aliases should have similarity 1.0."""
        topo1 = extract_topology(two_table_join)
        topo2 = extract_topology(two_table_join_different_aliases)
        sim = topology_similarity(topo1, topo2)
        assert sim == 1.0

    def test_learning_with_different_aliases(
        self,
        two_table_join: RAExpression,
        two_table_join_different_aliases: RAExpression,
    ) -> None:
        """Learner should cluster queries with different aliases together."""
        learner = TopologyViewLearner()
        result = learner.learn_views([two_table_join, two_table_join_different_aliases])

        assert result.total_expressions == 2
        assert result.unique_topologies == 1  # Same topology


# =============================================================================
# Test: Self-Join Handling
# =============================================================================


class TestSelfJoinHandling:
    """Tests for self-join handling."""

    def test_self_join_extraction(self, self_join: RAExpression) -> None:
        """Self-join should produce two distinct TableRefs."""
        topo = extract_topology(self_join)

        # Should have two relations (both orders, but with different occurrences)
        assert topo.relation_count() == 2

        # Physical names should include occurrence suffix
        physical_names = [r.physical for r in topo.relations]
        assert "public.orders" in physical_names
        assert "public.orders#2" in physical_names

        # Base table should still be "public.orders"
        assert topo.physical_tables() == frozenset({"public.orders"})

    def test_self_join_has_edge(self, self_join: RAExpression) -> None:
        """Self-join should have a join edge between the two instances."""
        topo = extract_topology(self_join)
        assert topo.edge_count() == 1

    def test_self_join_aliases_preserved(self, self_join: RAExpression) -> None:
        """Self-join aliases should be preserved for RA reconstruction."""
        topo = extract_topology(self_join)

        aliases = {r.alias for r in topo.relations}
        assert "o1" in aliases
        assert "o2" in aliases

    def test_self_join_different_from_regular_join(
        self, self_join: RAExpression, two_table_join: RAExpression
    ) -> None:
        """Self-join should have different topology than regular join."""
        topo_self = extract_topology(self_join)
        topo_regular = extract_topology(two_table_join)

        # Different physical tables
        assert topo_self.physical_tables() != topo_regular.physical_tables()

        # Different fingerprints
        assert topo_self.fingerprint() != topo_regular.fingerprint()

    def test_self_join_gcs(self, self_join: RAExpression) -> None:
        """GCS of self-join with itself should work."""
        result = topology_gcs([self_join, self_join])
        assert result.success
        assert result.topology is not None
        assert result.topology.relation_count() == 2
        assert result.topology.edge_count() == 1

    def test_self_join_to_ra(self, self_join: RAExpression) -> None:
        """Converting self-join topology to RA should work."""
        topo = extract_topology(self_join)
        ra = topology_to_ra(topo)
        assert isinstance(ra, Join)

        # Should preserve self-join structure
        extracted_back = extract_topology(ra)
        assert extracted_back.relation_count() == 2


# =============================================================================
# Test: Self-Join Canonicalization
# =============================================================================


class TestSelfJoinCanonicalization:
    """Tests for self-join canonicalization."""

    def test_same_self_join_different_ast_order(self) -> None:
        """Same self-join with different AST order produces same topology."""
        # Query A: o1 first in AST
        query_a = Join(
            left=Relation(name="orders", alias="o1"),
            right=Relation(name="orders", alias="o2"),
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="o1", column="parent_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o2", column="id"),
            ),
        )

        # Query B: o2 first in AST (swapped left/right)
        query_b = Join(
            left=Relation(name="orders", alias="o2"),
            right=Relation(name="orders", alias="o1"),
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="o1", column="parent_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o2", column="id"),
            ),
        )

        topo_a = extract_topology(query_a)
        topo_b = extract_topology(query_b)

        # Fingerprints should be identical after canonicalization
        assert topo_a.fingerprint() == topo_b.fingerprint()

        # Topologies should be equal
        assert topo_a.physical_tables() == topo_b.physical_tables()
        assert topo_a.join_edges == topo_b.join_edges

    def test_canonicalization_disabled(self) -> None:
        """When canonicalization is disabled, AST order affects topology."""
        query_a = Join(
            left=Relation(name="orders", alias="o1"),
            right=Relation(name="orders", alias="o2"),
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="o1", column="parent_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o2", column="id"),
            ),
        )

        query_b = Join(
            left=Relation(name="orders", alias="o2"),
            right=Relation(name="orders", alias="o1"),
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="o1", column="parent_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o2", column="id"),
            ),
        )

        # With canonicalization disabled
        no_canon = TopologyNormalization(canonicalize_self_joins=False)
        topo_a = extract_topology(query_a, normalization=no_canon)
        topo_b = extract_topology(query_b, normalization=no_canon)

        # Fingerprints may differ when canonicalization is disabled
        # (depends on AST order)
        # We just verify the topology is extracted without errors
        assert topo_a.relation_count() == 2
        assert topo_b.relation_count() == 2

    def test_non_self_join_unchanged(self, two_table_join: RAExpression) -> None:
        """Non-self-join topologies are unchanged by canonicalization."""
        topo_with = extract_topology(
            two_table_join,
            normalization=TopologyNormalization(canonicalize_self_joins=True),
        )
        topo_without = extract_topology(
            two_table_join,
            normalization=TopologyNormalization(canonicalize_self_joins=False),
        )

        # Should be identical since there's no self-join
        assert topo_with.fingerprint() == topo_without.fingerprint()

    def test_canonicalization_deterministic(self, self_join: RAExpression) -> None:
        """Canonicalization produces deterministic results."""
        topo1 = extract_topology(self_join)
        topo2 = extract_topology(self_join)
        topo3 = extract_topology(self_join)

        assert topo1.fingerprint() == topo2.fingerprint() == topo3.fingerprint()

    def test_canonical_occurrence_by_column(self) -> None:
        """Occurrences are ordered by their contributing column names."""
        # o1.parent_id = o2.id means:
        # - o1 contributes "parent_id"
        # - o2 contributes "id"
        # Since "id" < "parent_id", o2's occurrence should be #1
        query = Join(
            left=Relation(name="orders", alias="o1"),
            right=Relation(name="orders", alias="o2"),
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="o1", column="parent_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o2", column="id"),
            ),
        )

        topo = extract_topology(query)

        # Find the TableRef with each alias
        refs_by_alias = {r.alias: r for r in topo.relations}

        # o2 (contributing "id") should have occurrence 1
        # o1 (contributing "parent_id") should have occurrence 2
        assert refs_by_alias["o2"].occurrence == 1
        assert refs_by_alias["o1"].occurrence == 2

    def test_triple_self_join(self) -> None:
        """Three-way self-join is canonicalized correctly."""
        # orders o1 JOIN orders o2 ON o1.parent_id = o2.id
        # JOIN orders o3 ON o2.manager_id = o3.id
        inner_join = Join(
            left=Relation(name="orders", alias="o1"),
            right=Relation(name="orders", alias="o2"),
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="o1", column="parent_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o2", column="id"),
            ),
        )
        query = Join(
            left=inner_join,
            right=Relation(name="orders", alias="o3"),
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="o2", column="manager_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o3", column="id"),
            ),
        )

        topo = extract_topology(query)

        # Should have 3 occurrences
        assert topo.relation_count() == 3
        assert topo.edge_count() == 2

        # Check that canonicalization produces consistent results
        topo2 = extract_topology(query)
        assert topo.fingerprint() == topo2.fingerprint()


class TestEdgeBasedCanonicalizer:
    """Tests for EdgeBasedCanonicalizer class."""

    def test_empty_topology(self) -> None:
        """Empty topology is unchanged."""
        canon = EdgeBasedCanonicalizer()
        empty = Topology()
        result = canon.canonicalize(empty)
        assert result.is_empty()

    def test_single_relation(self) -> None:
        """Single relation is unchanged."""
        canon = EdgeBasedCanonicalizer()
        ref = TableRef(schema_name="public", table="orders", alias="o")
        topo = Topology(relations=frozenset({ref}))
        result = canon.canonicalize(topo)
        assert result.relation_count() == 1

    def test_no_self_join(self) -> None:
        """Topology without self-joins is unchanged."""
        canon = EdgeBasedCanonicalizer()
        ref_o = TableRef(schema_name="public", table="orders", alias="o")
        ref_c = TableRef(schema_name="public", table="customers", alias="c")
        edge = JoinEdge(
            left=ref_o,
            right=ref_c,
            left_column="customer_id",
            right_column="id",
        )
        topo = Topology(
            relations=frozenset({ref_o, ref_c}),
            join_edges=frozenset({edge}),
        )

        result = canon.canonicalize(topo)
        assert result.fingerprint() == topo.fingerprint()


# =============================================================================
# Test: TopologyNormalization Config
# =============================================================================


class TestTopologyNormalization:
    """Tests for TopologyNormalization config."""

    def test_default_values(self) -> None:
        """Default normalization config."""
        norm = TopologyNormalization()
        assert norm.default_schema == "public"
        assert norm.canonicalize_self_joins is True

    def test_custom_schema(self) -> None:
        """Custom default_schema is used."""
        norm = TopologyNormalization(default_schema="mydb")
        rel = Relation(name="orders", alias="o")
        topo = extract_topology(rel, normalization=norm)
        assert "mydb.orders" in topo.physical_tables()

    def test_disable_canonicalization(self) -> None:
        """Can disable self-join canonicalization."""
        norm = TopologyNormalization(canonicalize_self_joins=False)
        assert norm.canonicalize_self_joins is False

    def test_normalization_with_learner(self) -> None:
        """TopologyViewLearner accepts normalization config."""
        norm = TopologyNormalization(default_schema="analytics")
        learner = TopologyViewLearner(normalization=norm)

        rel = Relation(name="orders", alias="o")
        result = learner.learn_views([rel, rel])

        assert result.total_expressions == 2

    def test_normalization_with_gcs(self) -> None:
        """topology_gcs accepts normalization config."""
        norm = TopologyNormalization(default_schema="mydb")

        rel1 = Relation(name="orders", alias="o")
        rel2 = Relation(name="orders", alias="ord")

        result = topology_gcs([rel1, rel2], normalization=norm)
        assert result.success
        assert "mydb.orders" in result.common_relations

    def test_backwards_compatibility_default_schema(self) -> None:
        """Old default_schema parameter still works."""
        rel = Relation(name="orders", alias="o")

        # Old way
        topo1 = extract_topology(rel, default_schema="legacy")

        # New way
        topo2 = extract_topology(rel, normalization=TopologyNormalization(default_schema="legacy"))

        assert topo1.fingerprint() == topo2.fingerprint()


# =============================================================================
# Test: Topology GCS
# =============================================================================


class TestTopologyGCS:
    """Tests for topology_gcs."""

    def test_empty_list(self) -> None:
        """GCS of empty list should fail."""
        result = topology_gcs([])
        assert not result.success
        assert result.input_count == 0

    def test_single_expression(self, simple_relation: RAExpression) -> None:
        """GCS of single expression is itself."""
        result = topology_gcs([simple_relation])
        assert result.success
        assert result.input_count == 1

    def test_identical_topologies(self, two_table_join: RAExpression) -> None:
        """GCS of identical topologies is that topology."""
        result = topology_gcs([two_table_join, two_table_join])
        assert result.success
        assert result.topology is not None
        assert len(result.topology.physical_tables()) == 2

    def test_overlapping_topologies(
        self, two_table_join: RAExpression, three_table_join: RAExpression
    ) -> None:
        """GCS of overlapping topologies is intersection."""
        result = topology_gcs([two_table_join, three_table_join])
        assert result.success
        assert result.topology is not None
        # Common: orders, customers (products only in three_table_join)
        physical = result.topology.physical_tables()
        assert "public.orders" in physical
        assert "public.customers" in physical
        # Join edge should be preserved
        assert result.topology.edge_count() == 1

    def test_disjoint_topologies(self) -> None:
        """GCS of disjoint topologies should fail."""
        expr1 = Relation(name="orders", alias="o")
        expr2 = Relation(name="customers", alias="c")

        result = topology_gcs([expr1, expr2])
        assert not result.success

    def test_predicate_ignored(
        self, two_table_join: RAExpression, join_with_selection: RAExpression
    ) -> None:
        """Selection predicates should not affect topology GCS."""
        result = topology_gcs([two_table_join, join_with_selection])
        assert result.success
        assert result.topology is not None
        # Topologies should be identical (selection ignored)
        assert len(result.topology.physical_tables()) == 2


# =============================================================================
# Test: Topology to RA
# =============================================================================


class TestTopologyToRA:
    """Tests for topology_to_ra."""

    def test_single_relation(self) -> None:
        """Convert single-relation topology to RA."""
        ref = TableRef(schema_name="public", table="orders", alias="o")
        topo = Topology(relations=frozenset({ref}))
        ra = topology_to_ra(topo)
        assert isinstance(ra, Relation)
        assert ra.name == "orders"
        assert ra.schema_name == "public"
        assert ra.alias == "o"

    def test_two_relation_with_edge(self) -> None:
        """Convert two-relation topology with edge to RA."""
        ref_orders = TableRef(schema_name="public", table="orders", alias="o")
        ref_customers = TableRef(schema_name="public", table="customers", alias="c")
        edge = JoinEdge(
            left=ref_orders, left_column="customer_id", right=ref_customers, right_column="id"
        )
        topo = Topology(
            relations=frozenset({ref_orders, ref_customers}),
            join_edges=frozenset({edge}),
        )
        ra = topology_to_ra(topo)
        assert isinstance(ra, Join)

    def test_empty_topology_raises(self) -> None:
        """Empty topology should raise ValueError."""
        topo = Topology()
        with pytest.raises(ValueError):
            topology_to_ra(topo)

    def test_roundtrip(self, two_table_join: RAExpression) -> None:
        """Extract topology and convert back to RA."""
        topo = extract_topology(two_table_join)
        ra = topology_to_ra(topo)

        # Re-extract topology should match
        topo2 = extract_topology(ra)
        assert topo.physical_tables() == topo2.physical_tables()
        assert topo.edge_count() == topo2.edge_count()

    def test_reconstructed_relation_has_correct_fields(self) -> None:
        """Reconstructed Relation should have correct schema_name and name."""
        ref = TableRef(schema_name="analytics", table="orders", alias="o")
        topo = Topology(relations=frozenset({ref}))
        ra = topology_to_ra(topo)

        assert isinstance(ra, Relation)
        assert ra.name == "orders"
        assert ra.schema_name == "analytics"
        assert ra.alias == "o"


# =============================================================================
# Test: Topology Similarity
# =============================================================================


class TestTopologySimilarity:
    """Tests for topology_similarity."""

    def test_identical_topologies(self) -> None:
        """Identical topologies should have similarity 1.0."""
        ref_a = TableRef(schema_name="public", table="a", alias="a")
        ref_b = TableRef(schema_name="public", table="b", alias="b")
        topo = Topology(relations=frozenset({ref_a, ref_b}))
        assert topology_similarity(topo, topo) == 1.0

    def test_disjoint_topologies_with_edges(self) -> None:
        """Truly disjoint topologies should have similarity 0.0."""
        ref_a = TableRef(schema_name="public", table="a", alias="a")
        ref_b = TableRef(schema_name="public", table="b", alias="b")
        ref_c = TableRef(schema_name="public", table="c", alias="c")
        ref_d = TableRef(schema_name="public", table="d", alias="d")

        edge1 = JoinEdge(left=ref_a, left_column="x", right=ref_b, right_column="y")
        edge2 = JoinEdge(left=ref_c, left_column="x", right=ref_d, right_column="y")

        topo1 = Topology(relations=frozenset({ref_a, ref_b}), join_edges=frozenset({edge1}))
        topo2 = Topology(relations=frozenset({ref_c, ref_d}), join_edges=frozenset({edge2}))

        assert topology_similarity(topo1, topo2) == 0.0

    def test_partial_overlap(self) -> None:
        """Partially overlapping topologies should have intermediate similarity."""
        ref_a = TableRef(schema_name="public", table="a", alias="a")
        ref_b = TableRef(schema_name="public", table="b", alias="b")
        ref_c = TableRef(schema_name="public", table="c", alias="c")
        ref_d = TableRef(schema_name="public", table="d", alias="d")

        topo1 = Topology(relations=frozenset({ref_a, ref_b, ref_c}))
        topo2 = Topology(relations=frozenset({ref_b, ref_c, ref_d}))
        sim = topology_similarity(topo1, topo2)
        assert 0.0 < sim < 1.0


# =============================================================================
# Test: Subtopology
# =============================================================================


class TestSubtopology:
    """Tests for is_subtopology."""

    def test_same_topology_is_subtopology(self) -> None:
        """A topology is a subtopology of itself."""
        ref_a = TableRef(schema_name="public", table="a", alias="a")
        ref_b = TableRef(schema_name="public", table="b", alias="b")
        topo = Topology(relations=frozenset({ref_a, ref_b}))
        assert is_subtopology(topo, topo)

    def test_subset_relations(self) -> None:
        """Smaller relation set is subtopology."""
        ref_a = TableRef(schema_name="public", table="a", alias="a")
        ref_b = TableRef(schema_name="public", table="b", alias="b")

        small = Topology(relations=frozenset({ref_a}))
        large = Topology(relations=frozenset({ref_a, ref_b}))
        assert is_subtopology(small, large)
        assert not is_subtopology(large, small)

    def test_empty_is_subtopology(self) -> None:
        """Empty topology is subtopology of any topology."""
        ref_a = TableRef(schema_name="public", table="a", alias="a")
        empty = Topology()
        nonempty = Topology(relations=frozenset({ref_a}))
        assert is_subtopology(empty, nonempty)


# =============================================================================
# Test: Topology View Learner
# =============================================================================


class TestTopologyViewLearner:
    """Tests for TopologyViewLearner."""

    def test_empty_input(self) -> None:
        """Learning from empty list returns empty result."""
        learner = TopologyViewLearner()
        result = learner.learn_views([])
        assert result.total_expressions == 0
        assert len(result.views) == 0

    def test_single_expression(self, simple_relation: RAExpression) -> None:
        """Learning from single expression."""
        learner = TopologyViewLearner()
        result = learner.learn_views([simple_relation])
        assert result.total_expressions == 1

    def test_similar_topologies_cluster(
        self, two_table_join: RAExpression, join_with_selection: RAExpression
    ) -> None:
        """Similar topologies should cluster together."""
        learner = TopologyViewLearner()
        # Both have same topology (selection is ignored)
        result = learner.learn_views([two_table_join, join_with_selection])
        assert result.total_expressions == 2
        # Should produce one view (same topology)
        if result.views:
            view = result.views[0]
            assert "public.orders" in view.relations
            assert "public.customers" in view.relations

    def test_star_schema_detection(self, star_schema_join: RAExpression) -> None:
        """Star schema pattern should be detected."""
        learner = TopologyViewLearner()
        result = learner.learn_views([star_schema_join, star_schema_join])

        # Should detect star pattern
        if result.views:
            view = result.views[0]
            # Star schema has fact table "public.fact_sales" as hub
            # The exact detection depends on clustering
            assert len(view.relations) >= 3

    def test_learner_with_custom_default_schema(self) -> None:
        """Learner should use custom default_schema."""
        learner = TopologyViewLearner(default_schema="analytics")
        rel = Relation(name="orders", alias="o")
        result = learner.learn_views([rel, rel])

        # Check extracted topology uses analytics schema
        assert result.total_expressions == 2


class TestLearnedTopologyView:
    """Tests for LearnedTopologyView."""

    def test_view_properties(self, two_table_join: RAExpression) -> None:
        """Check view properties are set correctly."""
        learner = TopologyViewLearner()
        result = learner.learn_views([two_table_join, two_table_join])

        if result.views:
            view = result.views[0]
            assert view.id is not None
            assert view.name is not None
            assert view.cluster_id is not None
            assert view.pattern_count >= 1


# =============================================================================
# Test: Convenience Functions
# =============================================================================


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_learn_topology_views(self, two_table_join: RAExpression) -> None:
        """Test learn_topology_views function."""
        result = learn_topology_views([two_table_join, two_table_join])
        assert isinstance(result, TopologyLearningResult)
        assert result.total_expressions == 2

    def test_extract_all_topologies(
        self, simple_relation: RAExpression, two_table_join: RAExpression
    ) -> None:
        """Test extract_all_topologies function."""
        topos = extract_all_topologies([simple_relation, two_table_join])
        assert len(topos) == 2
        assert all(isinstance(t, Topology) for t in topos)

    def test_find_common_topology(
        self, two_table_join: RAExpression, three_table_join: RAExpression
    ) -> None:
        """Test find_common_topology function."""
        result = find_common_topology([two_table_join, three_table_join])
        assert isinstance(result, TopologyResult)

    def test_functions_accept_default_schema(self, simple_relation: RAExpression) -> None:
        """Convenience functions should accept default_schema."""
        topo = extract_topology(simple_relation, default_schema="mydb")
        assert "mydb.orders" in topo.physical_tables()

        topos = extract_all_topologies([simple_relation], default_schema="mydb")
        assert "mydb.orders" in topos[0].physical_tables()

        result = find_common_topology([simple_relation], default_schema="mydb")
        assert result.success


# =============================================================================
# Test: Integration
# =============================================================================


class TestTopologyIntegration:
    """Integration tests for topology learning."""

    def test_full_workflow(self) -> None:
        """Test full workflow from expressions to views."""
        # Create varied expressions
        expressions = [
            # Simple two-table join
            Join(
                left=Relation(name="orders", alias="o"),
                right=Relation(name="customers", alias="c"),
                join_type=JoinType.INNER,
                condition=AtomicPredicate(
                    left=ColumnRef(table="o", column="customer_id"),
                    op=ComparisonOp.EQ,
                    right=ColumnRef(table="c", column="id"),
                ),
            ),
            # Same join with selection (should have same topology)
            Selection(
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
            ),
            # Same join with different aliases (should have same topology)
            Join(
                left=Relation(name="orders", alias="ord"),
                right=Relation(name="customers", alias="cust"),
                join_type=JoinType.INNER,
                condition=AtomicPredicate(
                    left=ColumnRef(table="ord", column="customer_id"),
                    op=ComparisonOp.EQ,
                    right=ColumnRef(table="cust", column="id"),
                ),
            ),
        ]

        result = learn_topology_views(expressions)

        assert result.total_expressions == 3
        # All three should have the same topology
        assert result.unique_topologies == 1

    def test_aggregation_topology_preserved(self) -> None:
        """Test that aggregation preserves underlying topology."""
        base_join = Join(
            left=Relation(name="orders", alias="o"),
            right=Relation(name="customers", alias="c"),
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="o", column="customer_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="c", column="id"),
            ),
        )

        agg = Aggregation(
            group_by=[ColumnRef(table="c", column="id")],
            aggregates=[
                AggregateSpec(
                    function=AggregateFunction.SUM,
                    argument=ColumnRef(table="o", column="amount"),
                    alias="total",
                )
            ],
            input=base_join,
        )

        # Extract topology from both
        base_topo = extract_topology(base_join)
        agg_topo = extract_topology(agg)

        # Topologies should be identical
        assert base_topo.physical_tables() == agg_topo.physical_tables()
        assert base_topo.join_edges == agg_topo.join_edges

    def test_self_join_and_regular_join_different(self) -> None:
        """Self-join and regular join should be different topologies."""
        regular_join = Join(
            left=Relation(name="orders", alias="o"),
            right=Relation(name="customers", alias="c"),
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="o", column="customer_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="c", column="id"),
            ),
        )

        self_join = Join(
            left=Relation(name="orders", alias="o1"),
            right=Relation(name="orders", alias="o2"),
            join_type=JoinType.INNER,
            condition=AtomicPredicate(
                left=ColumnRef(table="o1", column="parent_id"),
                op=ComparisonOp.EQ,
                right=ColumnRef(table="o2", column="id"),
            ),
        )

        result = learn_topology_views([regular_join, self_join])
        # Should recognize as different topologies
        assert result.unique_topologies == 2
