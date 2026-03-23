"""Tests for the algebrakit.learning module.

Tests cover:
- Pattern extraction from RA expressions
- Pattern clustering
- Evidence computation
- View derivation
- Workload management
"""

import pytest

from alma_algebrakit.learning import (
    ClusteringConfig,
    DerivationConfig,
    EvidenceComputer,
    GlobalEvidenceComputer,
    JoinGraphAnalyzer,
    PatternCluster,
    PatternClusterer,
    PatternExtractor,
    PatternInstance,
    PatternSignature,
    ViewCandidate,
    ViewDeriver,
    Workload,
    compute_workload_statistics,
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
    Relation,
    Selection,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def simple_relation() -> Relation:
    """A simple base relation."""
    return Relation(name="orders", alias="o")


@pytest.fixture
def simple_selection(simple_relation: Relation) -> Selection:
    """A selection on the simple relation."""
    pred = AtomicPredicate(
        left=ColumnRef(column="status", table="o"),
        op=ComparisonOp.EQ,
        right=Literal(value="active"),
    )
    return Selection(predicate=pred, input=simple_relation)


@pytest.fixture
def simple_projection(simple_selection: Selection) -> Projection:
    """A projection over the selection."""
    return Projection(
        columns=[
            (ColumnRef(column="id", table="o"), "order_id"),
            (ColumnRef(column="total", table="o"), "amount"),
        ],
        input=simple_selection,
    )


@pytest.fixture
def two_table_join() -> Join:
    """A join between orders and customers."""
    orders = Relation(name="orders", alias="o")
    customers = Relation(name="customers", alias="c")
    join_cond = AtomicPredicate(
        left=ColumnRef(column="customer_id", table="o"),
        op=ComparisonOp.EQ,
        right=ColumnRef(column="id", table="c"),
    )
    return Join(
        left=orders,
        right=customers,
        join_type=JoinType.INNER,
        condition=join_cond,
    )


@pytest.fixture
def three_table_star_join() -> Join:
    """A star schema join: orders <- customers, orders <- products."""
    orders = Relation(name="orders", alias="o")
    customers = Relation(name="customers", alias="c")
    products = Relation(name="products", alias="p")

    orders_customers = Join(
        left=orders,
        right=customers,
        join_type=JoinType.INNER,
        condition=AtomicPredicate(
            left=ColumnRef(column="customer_id", table="o"),
            op=ComparisonOp.EQ,
            right=ColumnRef(column="id", table="c"),
        ),
    )

    return Join(
        left=orders_customers,
        right=products,
        join_type=JoinType.INNER,
        condition=AtomicPredicate(
            left=ColumnRef(column="product_id", table="o"),
            op=ComparisonOp.EQ,
            right=ColumnRef(column="id", table="p"),
        ),
    )


@pytest.fixture
def aggregation_query(two_table_join: Join) -> Aggregation:
    """An aggregation query with GROUP BY."""
    return Aggregation(
        group_by=[ColumnRef(column="customer_id", table="o")],
        aggregates=[
            AggregateSpec(
                function=AggregateFunction.SUM,
                arguments=[ColumnRef(column="total", table="o")],
                alias="total_amount",
            ),
            AggregateSpec(
                function=AggregateFunction.COUNT,
                arguments=[ColumnRef(column="id", table="o")],
                alias="order_count",
            ),
        ],
        input=two_table_join,
    )


# =============================================================================
# Pattern Extraction Tests
# =============================================================================


class TestPatternExtraction:
    """Tests for PatternExtractor."""

    def test_extract_simple_relation(self, simple_relation: Relation) -> None:
        """Test extracting pattern from a simple relation."""
        extractor = PatternExtractor()
        sig = extractor.extract(simple_relation)

        assert "o" in sig.relations
        assert sig.table_aliases["o"] == "orders"
        assert len(sig.join_edges) == 0
        assert len(sig.predicates) == 0

    def test_extract_selection(self, simple_selection: Selection) -> None:
        """Test extracting pattern with selection predicates."""
        extractor = PatternExtractor()
        sig = extractor.extract(simple_selection)

        assert "o" in sig.relations
        assert len(sig.predicates) > 0
        assert not sig.has_outer_join
        assert not sig.has_union

    def test_extract_projection(self, simple_projection: Projection) -> None:
        """Test extracting pattern with projected columns."""
        extractor = PatternExtractor()
        sig = extractor.extract(simple_projection)

        assert len(sig.projected_columns) == 2
        # Check that the column aliases are captured
        assert "order_id" in sig.projected_columns or "o.id" in sig.projected_columns

    def test_extract_join(self, two_table_join: Join) -> None:
        """Test extracting pattern with joins."""
        extractor = PatternExtractor()
        sig = extractor.extract(two_table_join)

        assert "o" in sig.relations
        assert "c" in sig.relations
        assert len(sig.join_edges) == 1
        edge = sig.join_edges[0]
        assert edge.left_table in ["o", "c"]
        assert edge.right_table in ["o", "c"]
        assert not sig.has_outer_join

    def test_extract_star_join(self, three_table_star_join: Join) -> None:
        """Test extracting pattern from a star schema."""
        extractor = PatternExtractor()
        sig = extractor.extract(three_table_star_join)

        assert "o" in sig.relations
        assert "c" in sig.relations
        assert "p" in sig.relations
        assert len(sig.join_edges) == 2

    def test_extract_aggregation(self, aggregation_query: Aggregation) -> None:
        """Test extracting pattern with aggregations."""
        extractor = PatternExtractor()
        sig = extractor.extract(aggregation_query)

        assert "o" in sig.relations
        assert "c" in sig.relations
        assert len(sig.group_by_columns) >= 1
        assert len(sig.aggregates) == 2

    def test_detect_outer_join(self) -> None:
        """Test detecting outer joins."""
        orders = Relation(name="orders", alias="o")
        customers = Relation(name="customers", alias="c")
        left_join = Join(
            left=orders,
            right=customers,
            join_type=JoinType.LEFT,
            condition=AtomicPredicate(
                left=ColumnRef(column="customer_id", table="o"),
                op=ComparisonOp.EQ,
                right=ColumnRef(column="id", table="c"),
            ),
        )

        extractor = PatternExtractor()
        sig = extractor.extract(left_join)

        assert sig.has_outer_join is True


class TestPatternSignature:
    """Tests for PatternSignature."""

    def test_feature_set(self) -> None:
        """Test computing feature set from signature."""
        sig = PatternSignature(
            relations=["orders", "customers"],
            join_edges=[],
            predicates=["status = 'active'"],
            projected_columns=["o.id", "c.name"],
            group_by_columns=["o.customer_id"],
        )

        features = sig.feature_set()

        assert "rel:orders" in features
        assert "rel:customers" in features
        assert "pred:status = 'active'" in features
        assert "proj:o.id" in features
        assert "group:o.customer_id" in features

    def test_weighted_features(self) -> None:
        """Test computing weighted features."""
        sig = PatternSignature(
            relations=["orders"],
            predicates=["status = 'active'"],
        )

        weights = {"relation": 2.0, "predicate": 1.5, "column": 1.0}
        weighted = sig.weighted_features(weights)

        assert weighted["rel:orders"] == 2.0
        assert weighted["pred:status = 'active'"] == 1.5


# =============================================================================
# Clustering Tests
# =============================================================================


class TestPatternClustering:
    """Tests for PatternClusterer."""

    def test_cluster_empty_list(self) -> None:
        """Test clustering an empty list."""
        clusterer = PatternClusterer()
        clusters = clusterer.cluster([])
        assert len(clusters) == 0

    def test_cluster_single_pattern(self) -> None:
        """Test clustering a single pattern."""
        sig = PatternSignature(relations=["orders"])
        pattern = PatternInstance(id="p1", signature=sig)

        clusterer = PatternClusterer()
        clusters = clusterer.cluster([pattern])

        assert len(clusters) == 1
        assert "p1" in clusters[0].pattern_ids

    def test_cluster_similar_patterns(self) -> None:
        """Test that similar patterns are clustered together."""
        sig1 = PatternSignature(
            relations=["orders", "customers"],
            predicates=["status = 'active'"],
        )
        sig2 = PatternSignature(
            relations=["orders", "customers"],
            predicates=["status = 'pending'"],
        )
        sig3 = PatternSignature(
            relations=["products"],
            predicates=["category = 'electronics'"],
        )

        patterns = [
            PatternInstance(id="p1", signature=sig1),
            PatternInstance(id="p2", signature=sig2),
            PatternInstance(id="p3", signature=sig3),
        ]

        config = ClusteringConfig(similarity_threshold=0.3, min_cluster_size=1)
        clusterer = PatternClusterer(config)
        clusters = clusterer.cluster(patterns)

        # p1 and p2 should be in the same cluster (similar relations)
        # p3 should be in a different cluster
        assert len(clusters) >= 1
        # Find the cluster containing p1
        p1_cluster = None
        for c in clusters:
            if "p1" in c.pattern_ids:
                p1_cluster = c
                break
        assert p1_cluster is not None
        # p2 should be in the same cluster
        assert "p2" in p1_cluster.pattern_ids

    def test_compute_similarity(self) -> None:
        """Test computing similarity between signatures."""
        sig1 = PatternSignature(
            relations=["orders", "customers"],
            predicates=["a = b"],
        )
        sig2 = PatternSignature(
            relations=["orders", "customers"],
            predicates=["a = b"],
        )
        sig3 = PatternSignature(
            relations=["products"],
            predicates=["x = y"],
        )

        clusterer = PatternClusterer()

        # Identical signatures should have high similarity
        sim_same = clusterer.compute_similarity(sig1, sig2)
        assert sim_same > 0.9

        # Different signatures should have low similarity
        sim_diff = clusterer.compute_similarity(sig1, sig3)
        assert sim_diff < 0.3


class TestClusteringConfig:
    """Tests for ClusteringConfig."""

    def test_join_focused_config(self) -> None:
        """Test join-focused configuration."""
        config = ClusteringConfig.join_focused()

        assert config.join_weight > config.column_weight
        assert config.relation_weight > config.column_weight


# =============================================================================
# Evidence Tests
# =============================================================================


class TestEvidenceComputer:
    """Tests for EvidenceComputer."""

    def test_compute_cluster_feature_support(self) -> None:
        """Test computing feature support within a cluster."""
        sig1 = PatternSignature(relations=["orders"], predicates=["a = b"])
        sig2 = PatternSignature(relations=["orders"], predicates=["c = d"])

        patterns = [
            PatternInstance(id="p1", signature=sig1, weight=1.0),
            PatternInstance(id="p2", signature=sig2, weight=1.0),
        ]

        cluster = PatternCluster(
            id="c1",
            pattern_ids=["p1", "p2"],
            total_weight=2.0,
        )

        computer = EvidenceComputer()
        support = computer.compute_cluster_feature_support(cluster, patterns)

        # rel:orders should have 100% support
        assert support.get("rel:orders", 0) == 1.0
        # Each predicate should have 50% support
        assert support.get("pred:a = b", 0) == 0.5
        assert support.get("pred:c = d", 0) == 0.5

    def test_classify_predicates(self) -> None:
        """Test classifying predicates as invariant/optional."""
        sig1 = PatternSignature(predicates=["status = 'active'", "date > '2024-01-01'"])
        sig2 = PatternSignature(predicates=["status = 'active'"])
        sig3 = PatternSignature(predicates=["status = 'active'"])

        patterns = [
            PatternInstance(id="p1", signature=sig1, weight=1.0),
            PatternInstance(id="p2", signature=sig2, weight=1.0),
            PatternInstance(id="p3", signature=sig3, weight=1.0),
        ]

        cluster = PatternCluster(
            id="c1",
            pattern_ids=["p1", "p2", "p3"],
            total_weight=3.0,
        )

        computer = EvidenceComputer()
        invariant, optional = computer.classify_predicates(
            cluster, patterns, invariant_threshold=0.6
        )

        # status = 'active' appears in all 3 patterns -> invariant
        assert "status = 'active'" in invariant
        # date > '2024-01-01' appears in only 1 pattern -> optional
        assert "date > '2024-01-01'" in optional


class TestJoinGraphAnalyzer:
    """Tests for JoinGraphAnalyzer."""

    def test_analyze_empty_patterns(self) -> None:
        """Test analyzing empty pattern list."""
        analyzer = JoinGraphAnalyzer()
        evidence = analyzer.analyze([])

        assert len(evidence.nodes) == 0
        assert len(evidence.edges) == 0

    def test_analyze_star_schema(self) -> None:
        """Test detecting star schema."""
        # Create patterns that form a star schema: fact table joins to many dims
        from alma_algebrakit.learning.patterns import JoinEdgePattern

        sig = PatternSignature(
            relations=["fact", "dim1", "dim2", "dim3"],
            join_edges=[
                JoinEdgePattern(
                    left_table="fact",
                    right_table="dim1",
                    left_column="dim1_id",
                    right_column="id",
                    join_type="INNER",
                    predicate_fingerprint="fact.dim1_id=dim1.id",
                ),
                JoinEdgePattern(
                    left_table="fact",
                    right_table="dim2",
                    left_column="dim2_id",
                    right_column="id",
                    join_type="INNER",
                    predicate_fingerprint="fact.dim2_id=dim2.id",
                ),
                JoinEdgePattern(
                    left_table="fact",
                    right_table="dim3",
                    left_column="dim3_id",
                    right_column="id",
                    join_type="INNER",
                    predicate_fingerprint="fact.dim3_id=dim3.id",
                ),
            ],
        )

        patterns = [
            PatternInstance(id="p1", signature=sig, weight=10.0),
        ]

        analyzer = JoinGraphAnalyzer(hub_threshold=0.3)
        evidence = analyzer.analyze(patterns)

        assert evidence.is_star_schema
        assert evidence.star_center == "fact"
        assert "fact" in evidence.hub_tables

    def test_suggest_boundaries(self) -> None:
        """Test suggesting view boundaries."""
        from alma_algebrakit.learning.patterns import JoinEdgePattern

        sig = PatternSignature(
            relations=["orders", "customers"],
            join_edges=[
                JoinEdgePattern(
                    left_table="orders",
                    right_table="customers",
                    left_column="customer_id",
                    right_column="id",
                    join_type="INNER",
                    predicate_fingerprint="orders.customer_id=customers.id",
                ),
            ],
        )

        patterns = [
            PatternInstance(id="p1", signature=sig, weight=1.0),
        ]

        analyzer = JoinGraphAnalyzer()
        evidence = analyzer.analyze(patterns)

        # Should suggest at least one boundary
        assert len(evidence.suggested_boundaries) >= 1


# =============================================================================
# View Derivation Tests
# =============================================================================


class TestViewDeriver:
    """Tests for ViewDeriver."""

    def test_derive_empty_clusters(self) -> None:
        """Test deriving from empty cluster list."""
        deriver = ViewDeriver()
        candidates = deriver.derive_candidates([], [])
        assert len(candidates) == 0

    def test_derive_single_cluster(self) -> None:
        """Test deriving from a single cluster."""
        sig1 = PatternSignature(
            relations=["orders", "customers"],
            predicates=["status = 'active'"],
            projected_columns=["o.id", "c.name"],
        )
        sig2 = PatternSignature(
            relations=["orders", "customers"],
            predicates=["status = 'active'"],
            projected_columns=["o.id", "o.total"],
        )

        patterns = [
            PatternInstance(id="p1", signature=sig1, weight=1.0),
            PatternInstance(id="p2", signature=sig2, weight=1.0),
        ]

        cluster = PatternCluster(
            id="c1",
            pattern_ids=["p1", "p2"],
            total_weight=2.0,
            feature_support={
                "rel:orders": 1.0,
                "rel:customers": 1.0,
                "pred:status = 'active'": 1.0,
            },
        )

        config = DerivationConfig(
            join_support_threshold=0.5,
            predicate_support_threshold=0.5,
            attribute_support_threshold=0.3,
        )
        deriver = ViewDeriver(config)
        candidates = deriver.derive_candidates([cluster], patterns)

        assert len(candidates) == 1
        candidate = candidates[0]
        assert "orders" in candidate.core_relations
        assert "customers" in candidate.core_relations


class TestViewCandidate:
    """Tests for ViewCandidate."""

    def test_to_view_specification(self) -> None:
        """Test converting ViewCandidate to ViewSpecification."""
        candidate = ViewCandidate(
            suggested_name="test_view",
            cluster_id="c1",
            core_relations=["orders", "customers"],
            table_aliases={"orders": "public.orders", "customers": "public.customers"},
            core_attributes=["orders.id", "customers.name"],
        )

        spec = candidate.to_view_specification()

        assert spec.name == "test_view"
        assert len(spec.base_tables) == 2
        assert "id" in spec.column_lineage or "orders.id" in spec.column_lineage


# =============================================================================
# Workload Tests
# =============================================================================


class TestWorkload:
    """Tests for Workload."""

    def test_add_pattern_from_ra(self, simple_projection: Projection) -> None:
        """Test adding a pattern from an RA expression."""
        workload = Workload(name="test")
        pattern_id = workload.add_pattern(simple_projection)

        assert len(workload.patterns) == 1
        pattern = workload.get_pattern(pattern_id)
        assert pattern is not None
        assert "o" in pattern.signature.relations

    def test_add_signature_directly(self) -> None:
        """Test adding a pre-extracted signature."""
        sig = PatternSignature(relations=["orders"])
        workload = Workload(name="test")
        pattern_id = workload.add_signature(sig)

        assert len(workload.patterns) == 1
        pattern = workload.get_pattern(pattern_id)
        assert pattern is not None
        assert pattern.signature == sig

    def test_cluster_patterns(self) -> None:
        """Test clustering patterns in a workload."""
        sig1 = PatternSignature(relations=["orders", "customers"])
        sig2 = PatternSignature(relations=["orders", "customers"])
        sig3 = PatternSignature(relations=["products"])

        workload = Workload(name="test")
        workload.add_signature(sig1, weight=1.0)
        workload.add_signature(sig2, weight=1.0)
        workload.add_signature(sig3, weight=1.0)

        clusters = workload.cluster()

        assert len(clusters) >= 1
        assert len(workload.clusters) == len(clusters)

    def test_derive_views(self) -> None:
        """Test deriving views from a workload."""
        sig1 = PatternSignature(
            relations=["orders", "customers"],
            projected_columns=["o.id"],
        )
        sig2 = PatternSignature(
            relations=["orders", "customers"],
            projected_columns=["o.total"],
        )

        workload = Workload(name="test")
        workload.add_signature(sig1, weight=1.0)
        workload.add_signature(sig2, weight=1.0)

        # This should cluster first, then derive
        workload.derive_views()

        # Should have clustered
        assert len(workload.clusters) > 0

    def test_total_weight(self) -> None:
        """Test computing total weight."""
        sig = PatternSignature(relations=["orders"])

        workload = Workload(name="test")
        workload.add_signature(sig, weight=1.5)
        workload.add_signature(sig, weight=2.5)

        assert workload.total_weight() == 4.0


class TestWorkloadStatistics:
    """Tests for compute_workload_statistics."""

    def test_compute_statistics(self) -> None:
        """Test computing workload statistics."""
        from alma_algebrakit.learning.patterns import JoinEdgePattern

        sig1 = PatternSignature(
            relations=["orders", "customers"],
            join_edges=[
                JoinEdgePattern(
                    left_table="orders",
                    right_table="customers",
                    left_column="customer_id",
                    right_column="id",
                    join_type="INNER",
                    predicate_fingerprint="fp1",
                ),
            ],
            predicates=["status = 'active'"],
            projected_columns=["o.id", "c.name"],
        )
        sig2 = PatternSignature(
            relations=["orders"],
            predicates=["date > '2024'"],
            projected_columns=["o.total"],
        )

        workload = Workload(name="test")
        workload.add_signature(sig1, weight=1.0)
        workload.add_signature(sig2, weight=1.0)
        workload.cluster()

        stats = compute_workload_statistics(workload)

        assert stats.total_patterns == 2
        assert stats.total_weight == 2.0
        assert stats.unique_tables >= 2  # orders, customers
        assert stats.avg_tables_per_pattern == 1.5  # (2 + 1) / 2


# =============================================================================
# Global Evidence Tests
# =============================================================================


class TestGlobalEvidenceComputer:
    """Tests for GlobalEvidenceComputer."""

    def test_compute_global_feature_support(self) -> None:
        """Test computing global feature support."""
        sig1 = PatternSignature(relations=["orders", "customers"])
        sig2 = PatternSignature(relations=["orders", "products"])

        patterns = [
            PatternInstance(id="p1", signature=sig1, weight=1.0),
            PatternInstance(id="p2", signature=sig2, weight=1.0),
        ]

        clusters = [
            PatternCluster(id="c1", pattern_ids=["p1"], total_weight=1.0),
            PatternCluster(id="c2", pattern_ids=["p2"], total_weight=1.0),
        ]

        computer = GlobalEvidenceComputer()
        support = computer.compute_global_feature_support(patterns, clusters)

        # orders appears in both patterns
        orders_dist = support.features.get("rel:orders")
        assert orders_dist is not None
        assert orders_dist.global_support == 1.0

        # customers appears in only one pattern
        customers_dist = support.features.get("rel:customers")
        assert customers_dist is not None
        assert customers_dist.global_support == 0.5

    def test_identify_base_view_features(self) -> None:
        """Test identifying base view features."""
        sig1 = PatternSignature(relations=["orders", "customers"])
        sig2 = PatternSignature(relations=["orders", "products"])
        sig3 = PatternSignature(relations=["orders", "inventory"])

        patterns = [
            PatternInstance(id="p1", signature=sig1, weight=1.0),
            PatternInstance(id="p2", signature=sig2, weight=1.0),
            PatternInstance(id="p3", signature=sig3, weight=1.0),
        ]

        clusters = [
            PatternCluster(id="c1", pattern_ids=["p1"], total_weight=1.0),
            PatternCluster(id="c2", pattern_ids=["p2"], total_weight=1.0),
            PatternCluster(id="c3", pattern_ids=["p3"], total_weight=1.0),
        ]

        computer = GlobalEvidenceComputer()
        global_support = computer.compute_global_feature_support(patterns, clusters)

        base_features = computer.identify_base_view_features(
            global_support,
            support_threshold=0.9,
            min_clusters=2,
        )

        # orders should be a base feature (appears in all 3 clusters)
        assert "orders" in base_features["relations"]
