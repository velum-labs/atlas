"""View derivation from pattern clusters.

Derives candidate views from pattern clusters using support thresholds:
- Core joins: J* = {j ∈ J_C | supp_C(j) >= θ_J}
- Core predicates: P* = {p ∈ P_C | supp_C(p) >= θ_P}
- Core attributes: A* = {a ∈ A_C | supp_C(a) >= θ_A}

Constructs candidate view expression:
    E_C := π_{A* ∪ K}(σ_{∧P*}(⋈_{J*}({R ∈ R_C})))

Hierarchical derivation:
- Base views: High global support features shared across clusters
- Specialized views: Cluster-specific refinements extending base views

This module provides SQL-agnostic view derivation operating purely on
algebrakit's RA primitives.
"""

from __future__ import annotations

from uuid import uuid4

from pydantic import BaseModel, Field

from alma_algebrakit.folding.types import ViewSpecification
from alma_algebrakit.learning.clustering import PatternCluster, PatternInstance
from alma_algebrakit.learning.evidence import (
    DEFAULT_HIGH_SUPPORT_THRESHOLD,
    DEFAULT_MIN_CLUSTERS_FOR_BASE,
    FEATURE_PREFIX_GROUP_BY,
    FEATURE_PREFIX_JOIN,
    FEATURE_PREFIX_PREDICATE,
    FEATURE_PREFIX_PROJECTION,
    FEATURE_PREFIX_RELATION,
    EvidenceComputer,
    GlobalEvidenceComputer,
    GlobalFeatureSupport,
    JoinGraphAnalyzer,
    JoinGraphEvidence,
)

# =============================================================================
# Configuration
# =============================================================================


class DerivationConfig(BaseModel):
    """Configuration for view derivation."""

    join_support_threshold: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Support threshold for core joins (θ_J)",
    )
    predicate_support_threshold: float = Field(
        default=0.8,
        ge=0.0,
        le=1.0,
        description="Support threshold for invariant predicates (θ_P)",
    )
    attribute_support_threshold: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="Support threshold for core attributes (θ_A)",
    )
    max_views: int = Field(default=50, ge=1, description="Maximum views to derive")
    include_key_fields: bool = Field(
        default=True, description="Always include key fields for joinability"
    )
    allow_singletons: bool = Field(
        default=False,
        description="Allow deriving views from singleton clusters (single pattern)",
    )


class HierarchicalConfig(BaseModel):
    """Configuration for hierarchical view derivation."""

    base_support_threshold: float = Field(
        default=DEFAULT_HIGH_SUPPORT_THRESHOLD,
        ge=0.0,
        le=1.0,
        description="Global support threshold for base view features",
    )
    min_clusters_for_base: int = Field(
        default=DEFAULT_MIN_CLUSTERS_FOR_BASE,
        ge=1,
        description="Minimum clusters that must use a feature for base views",
    )
    specialized_global_threshold: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="Max global support for specialized features",
    )
    specialized_local_threshold: float = Field(
        default=0.7,
        ge=0.0,
        le=1.0,
        description="Min local support for specialized features",
    )
    hub_threshold: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="Fraction of total degree above which a table is a hub",
    )
    min_edge_weight: float = Field(
        default=1.0,
        ge=0.0,
        description="Minimum edge weight to consider in join graph",
    )


# =============================================================================
# View Candidate
# =============================================================================


class ViewCandidate(BaseModel):
    """A candidate view derived from a pattern cluster.

    Represents the core components that could form a reusable view:
    - Core relations (tables)
    - Core joins (join conditions)
    - Core predicates (invariant filters)
    - Core attributes (projected columns)
    """

    id: str = Field(default_factory=lambda: str(uuid4()), description="Candidate ID")
    suggested_name: str = Field(description="Suggested view name")
    cluster_id: str = Field(description="Source cluster ID")

    # Core components
    core_relations: list[str] = Field(default_factory=list, description="Core relation names")
    table_aliases: dict[str, str] = Field(
        default_factory=dict, description="Mapping from alias to actual table name"
    )
    core_joins: list[str] = Field(
        default_factory=list, description="Core join condition fingerprints"
    )
    core_predicates: list[str] = Field(
        default_factory=list, description="Core predicate fingerprints"
    )
    core_attributes: list[str] = Field(
        default_factory=list, description="Core projected column references"
    )

    # Key information
    candidate_keys: list[list[str]] = Field(
        default_factory=list, description="Candidate key columns"
    )
    proposed_grain: str | None = Field(default=None, description="Proposed grain description")

    # Statistics
    support_score: float = Field(default=0.0, description="Overall support score")
    coverage_weight: float = Field(default=0.0, description="Total weight of covered patterns")
    pattern_count: int = Field(default=0, description="Number of patterns covered")

    # Predicate classification
    invariant_predicates: list[str] = Field(
        default_factory=list, description="Predicates that are always present"
    )
    optional_predicates: list[str] = Field(
        default_factory=list, description="Predicates that are sometimes present"
    )

    # Additional metadata
    metadata: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Additional metadata"
    )

    def to_view_specification(self) -> ViewSpecification:
        """Convert to a ViewSpecification for use with RAFolder.

        Note: This creates a basic ViewSpecification. The caller may need
        to provide additional information like the actual RA expression.
        """
        # Import here to avoid circular imports
        from alma_algebrakit.models.algebra import Relation, RelationRef

        # Determine column lineage
        column_lineage: dict[str, str] = {}
        for attr in self.core_attributes:
            if "." in attr:
                parts = attr.split(".", 1)
                col = parts[1]
                column_lineage[col] = attr
            else:
                column_lineage[attr] = attr

        return ViewSpecification(
            name=self.suggested_name,
            ra_expression=Relation(name=self.suggested_name),  # Placeholder
            base_tables=[
                RelationRef(
                    physical_table=self.table_aliases.get(rel, rel),
                    alias=rel,
                )
                for rel in self.core_relations
            ],
            column_lineage=column_lineage,
            exposed_columns=list(column_lineage.keys()),
            is_select_star=False,
        )


# =============================================================================
# View Deriver
# =============================================================================


class ViewDeriver:
    """Derives view candidates from pattern clusters.

    This is the SQL-agnostic core of view derivation, operating purely
    on algebrakit primitives.
    """

    def __init__(self, config: DerivationConfig | None = None) -> None:
        self.config = config or DerivationConfig()
        self.evidence_computer = EvidenceComputer()

    def derive_candidates(
        self,
        clusters: list[PatternCluster],
        patterns: list[PatternInstance],
    ) -> list[ViewCandidate]:
        """Derive view candidates from pattern clusters.

        Args:
            clusters: List of pattern clusters
            patterns: All patterns in the workload

        Returns:
            List of view candidates
        """
        candidates: list[ViewCandidate] = []

        for cluster in clusters:
            if len(cluster.pattern_ids) < 2 and not self.config.allow_singletons:
                # Skip singleton clusters unless explicitly allowed
                continue

            candidate = self._derive_candidate(cluster, patterns)
            if candidate:
                candidates.append(candidate)

        # Sort by coverage weight
        candidates.sort(key=lambda c: -c.coverage_weight)

        # Limit to max views
        return candidates[: self.config.max_views]

    def _derive_candidate(
        self,
        cluster: PatternCluster,
        patterns: list[PatternInstance],
    ) -> ViewCandidate | None:
        """Derive a view candidate from a single cluster."""
        cluster_patterns = [p for p in patterns if p.id in cluster.pattern_ids]
        if not cluster_patterns:
            return None

        # Compute feature support
        support = self.evidence_computer.compute_cluster_feature_support(cluster, patterns)

        # Extract core components based on thresholds
        core_relations = self._extract_core_features(
            support, FEATURE_PREFIX_RELATION, self.config.join_support_threshold
        )
        core_joins = self._extract_core_features(
            support, FEATURE_PREFIX_JOIN, self.config.join_support_threshold
        )
        core_predicates = self._extract_core_features(
            support, FEATURE_PREFIX_PREDICATE, self.config.predicate_support_threshold
        )
        core_attributes_raw = self._extract_core_features(
            support, FEATURE_PREFIX_PROJECTION, self.config.attribute_support_threshold
        )
        core_groupby_raw = self._extract_core_features(
            support, FEATURE_PREFIX_GROUP_BY, self.config.attribute_support_threshold
        )

        if not core_relations:
            return None

        # Filter attributes and group-by to only those from core relations
        def is_from_core_relation(attr: str) -> bool:
            if "." not in attr:
                return True
            return any(attr.startswith(f"{rel}.") for rel in core_relations)

        core_attributes = [attr for attr in core_attributes_raw if is_from_core_relation(attr)]
        core_groupby = [col for col in core_groupby_raw if is_from_core_relation(col)]

        # If no attributes from core relations, use defaults
        if not core_attributes:
            if core_groupby:
                core_attributes = list(core_groupby)
            else:
                core_attributes = [f"{rel}.*" for rel in core_relations[:1]]

        # Collect table aliases from cluster patterns
        table_aliases: dict[str, str] = {}
        for p in cluster_patterns:
            if p.signature.table_aliases:
                table_aliases.update(p.signature.table_aliases)

        # Classify predicates
        invariant_preds, optional_preds = self.evidence_computer.classify_predicates(
            cluster, patterns, self.config.predicate_support_threshold
        )

        # Identify candidate keys
        candidate_keys = self.evidence_computer.identify_candidate_keys(cluster, patterns)

        # Generate suggested name
        actual_table_names = [table_aliases.get(r, r) for r in core_relations]
        suggested_name = self._generate_name(actual_table_names)

        # Compute support score
        total_support = sum(support.values()) / max(len(support), 1)

        return ViewCandidate(
            id=str(uuid4()),
            suggested_name=suggested_name,
            cluster_id=cluster.id,
            core_relations=core_relations,
            table_aliases=table_aliases,
            core_joins=core_joins,
            core_predicates=core_predicates,
            core_attributes=core_attributes,
            candidate_keys=candidate_keys,
            proposed_grain=(
                f"One row per {', '.join(candidate_keys[0])}"
                if candidate_keys
                else "One row per record"
            ),
            support_score=total_support,
            coverage_weight=cluster.total_weight,
            pattern_count=len(cluster.pattern_ids),
            invariant_predicates=invariant_preds,
            optional_predicates=optional_preds,
        )

    def _extract_core_features(
        self,
        support: dict[str, float],
        prefix: str,
        threshold: float,
    ) -> list[str]:
        """Extract features with support above threshold."""
        return [
            f.split(":", 1)[1]
            for f, s in support.items()
            if f.startswith(prefix) and s >= threshold
        ]

    def _generate_name(self, relations: list[str]) -> str:
        """Generate a suggested view name from relations."""
        if not relations:
            return "unnamed_view"

        # Use primary relation name
        primary = relations[0].lower()

        if len(relations) > 1:
            return f"{primary}_with_{len(relations) - 1}_joins"

        return f"{primary}_view"


# =============================================================================
# Hierarchical View Derivation
# =============================================================================


class HierarchicalViewResult:
    """Result of hierarchical view derivation."""

    def __init__(
        self,
        base_views: list[ViewCandidate],
        specialized_views: list[ViewCandidate],
        global_support: GlobalFeatureSupport,
        join_graph: JoinGraphEvidence | None = None,
    ):
        self.base_views = base_views
        self.specialized_views = specialized_views
        self.global_support = global_support
        self.join_graph = join_graph

    @property
    def all_views(self) -> list[ViewCandidate]:
        """Get all views (base + specialized)."""
        return self.base_views + self.specialized_views

    @property
    def base_count(self) -> int:
        return len(self.base_views)

    @property
    def specialized_count(self) -> int:
        return len(self.specialized_views)


class HierarchicalViewDeriver:
    """Derives hierarchical views: base views + specialized extensions.

    Strategy:
    1. Analyze global feature support across all patterns and clusters
    2. Identify features with high cross-cluster usage for base views
    3. Identify cluster-specific features for specialized views
    4. Use join graph analysis to find optimal view boundaries

    Base views:
    - Features with global_support >= base_threshold
    - Used by min_clusters clusters
    - Centered on hub tables from join graph

    Specialized views:
    - Cluster-specific features (low global, high local support)
    - Additional predicates on base views
    - Extend base views, don't duplicate
    """

    def __init__(
        self,
        derivation_config: DerivationConfig | None = None,
        hierarchical_config: HierarchicalConfig | None = None,
    ) -> None:
        self.derivation_config = derivation_config or DerivationConfig()
        self.hierarchical_config = hierarchical_config or HierarchicalConfig()
        self.global_evidence = GlobalEvidenceComputer()
        self.join_analyzer = JoinGraphAnalyzer(
            hub_threshold=self.hierarchical_config.hub_threshold,
            min_edge_weight=self.hierarchical_config.min_edge_weight,
        )
        self.basic_deriver = ViewDeriver(derivation_config)

    def derive_hierarchical(
        self,
        patterns: list[PatternInstance],
        clusters: list[PatternCluster],
    ) -> HierarchicalViewResult:
        """Derive hierarchical views from patterns and clusters.

        Args:
            patterns: All patterns in the workload
            clusters: All pattern clusters

        Returns:
            HierarchicalViewResult with base and specialized views
        """
        # Step 1: Compute global feature support
        global_support = self.global_evidence.compute_global_feature_support(patterns, clusters)

        # Step 2: Analyze join graph
        join_graph = self.join_analyzer.analyze(patterns)

        # Step 3: Derive base views
        base_views = self._derive_base_views(patterns, clusters, global_support, join_graph)

        # Step 4: Derive specialized views
        specialized_views = self._derive_specialized_views(
            patterns, clusters, global_support, base_views
        )

        return HierarchicalViewResult(
            base_views=base_views,
            specialized_views=specialized_views,
            global_support=global_support,
            join_graph=join_graph,
        )

    def _derive_base_views(
        self,
        patterns: list[PatternInstance],
        clusters: list[PatternCluster],
        global_support: GlobalFeatureSupport,
        join_graph: JoinGraphEvidence,
    ) -> list[ViewCandidate]:
        """Derive base views from high global support features."""
        base_views: list[ViewCandidate] = []

        # Get high-support features
        base_features = self.global_evidence.identify_base_view_features(
            global_support,
            support_threshold=self.hierarchical_config.base_support_threshold,
            min_clusters=self.hierarchical_config.min_clusters_for_base,
        )

        if not base_features["relations"]:
            return base_views

        # Strategy 1: Create base view per hub table
        for hub in join_graph.hub_tables:
            if hub in base_features["relations"]:
                candidate = self._create_base_candidate(
                    hub_table=hub,
                    features=base_features,
                    patterns=patterns,
                    clusters=clusters,
                    global_support=global_support,
                )
                if candidate:
                    base_views.append(candidate)

        # Strategy 2: If no hubs, create one base view from all high-support features
        if not base_views and base_features["relations"]:
            candidate = self._create_base_candidate(
                hub_table=base_features["relations"][0],
                features=base_features,
                patterns=patterns,
                clusters=clusters,
                global_support=global_support,
            )
            if candidate:
                base_views.append(candidate)

        return base_views

    def _create_base_candidate(
        self,
        hub_table: str,
        features: dict[str, list[str]],
        patterns: list[PatternInstance],
        clusters: list[PatternCluster],
        global_support: GlobalFeatureSupport,
    ) -> ViewCandidate | None:
        """Create a base view candidate centered on a hub table."""
        # Filter features related to the hub table
        core_relations = [hub_table]
        for rel in features["relations"]:
            if rel != hub_table:
                # Check if this relation is connected to hub via joins
                for join in features["joins"]:
                    if hub_table in join and rel in join:
                        core_relations.append(rel)
                        break

        core_joins = [j for j in features["joins"] if any(rel in j for rel in core_relations)]
        core_predicates = [
            p for p in features["predicates"] if any(rel in p for rel in core_relations)
        ]
        core_attributes = [
            a for a in features["attributes"] if any(rel in a for rel in core_relations)
        ]

        if not core_relations:
            return None

        # Collect table aliases from patterns
        table_aliases: dict[str, str] = {}
        covered_pattern_ids: list[str] = []
        total_weight = 0.0

        for pattern in patterns:
            if pattern.signature.table_aliases:
                table_aliases.update(pattern.signature.table_aliases)
            pattern_rels = set(pattern.signature.relations)
            if set(core_relations) <= pattern_rels or any(
                r in pattern_rels for r in core_relations
            ):
                covered_pattern_ids.append(pattern.id)
                total_weight += pattern.weight

        # Compute support score
        support_score = 0.0
        for rel in core_relations:
            feat = global_support.features.get(f"rel:{rel}")
            if feat:
                support_score += feat.global_support
        support_score = support_score / len(core_relations) if core_relations else 0.0

        actual_hub_table = table_aliases.get(hub_table, hub_table)

        return ViewCandidate(
            id=str(uuid4()),
            suggested_name=f"base_{actual_hub_table}",
            cluster_id="base",
            core_relations=core_relations,
            table_aliases=table_aliases,
            core_joins=core_joins,
            core_predicates=core_predicates,
            core_attributes=core_attributes,
            candidate_keys=[[f"{hub_table}.id"]] if hub_table else [],
            proposed_grain=f"One row per {actual_hub_table}",
            support_score=support_score,
            coverage_weight=total_weight,
            pattern_count=len(covered_pattern_ids),
            invariant_predicates=core_predicates,
            optional_predicates=[],
            metadata={
                "view_type": "base",
                "hub_table": actual_hub_table,
            },
        )

    def _derive_specialized_views(
        self,
        patterns: list[PatternInstance],
        clusters: list[PatternCluster],
        global_support: GlobalFeatureSupport,
        base_views: list[ViewCandidate],
    ) -> list[ViewCandidate]:
        """Derive specialized views for cluster-specific features."""
        specialized: list[ViewCandidate] = []

        # Get base tables for reference
        base_tables = set()
        for bv in base_views:
            base_tables.update(bv.core_relations)

        for cluster in clusters:
            # Get cluster-specific features
            cluster_features = self.global_evidence.identify_specialized_features(
                global_support,
                cluster_id=cluster.id,
                global_threshold=self.hierarchical_config.specialized_global_threshold,
                local_threshold=self.hierarchical_config.specialized_local_threshold,
            )

            # Check if there are meaningful specialized features
            has_specialized = (
                cluster_features["predicates"]
                or cluster_features["joins"]
                or cluster_features["attributes"]
            )

            if not has_specialized:
                continue

            # Find which base view this specialization extends
            base_view = self._find_base_view(cluster, base_views, patterns)

            # Create specialized candidate
            candidate = self._create_specialized_candidate(
                cluster=cluster,
                base_view=base_view,
                specialized_features=cluster_features,
                patterns=patterns,
                global_support=global_support,
            )

            if candidate:
                specialized.append(candidate)

        return specialized

    def _find_base_view(
        self,
        cluster: PatternCluster,
        base_views: list[ViewCandidate],
        patterns: list[PatternInstance],
    ) -> ViewCandidate | None:
        """Find the best base view for a cluster."""
        if not base_views:
            return None

        # Get cluster's relations
        cluster_patterns = [p for p in patterns if p.id in cluster.pattern_ids]
        cluster_rels = set()
        for p in cluster_patterns:
            cluster_rels.update(p.signature.relations)

        # Find base view with most overlap
        best_view = None
        best_overlap = 0

        for bv in base_views:
            overlap = len(set(bv.core_relations) & cluster_rels)
            if overlap > best_overlap:
                best_overlap = overlap
                best_view = bv

        return best_view

    def _create_specialized_candidate(
        self,
        cluster: PatternCluster,
        base_view: ViewCandidate | None,
        specialized_features: dict[str, list[str]],
        patterns: list[PatternInstance],
        global_support: GlobalFeatureSupport,
    ) -> ViewCandidate | None:
        """Create a specialized view extending a base view."""
        cluster_patterns = [p for p in patterns if p.id in cluster.pattern_ids]

        if not cluster_patterns:
            return None

        # Start with base view features if available
        if base_view:
            core_relations = list(base_view.core_relations)
            core_joins = list(base_view.core_joins)
            base_predicates = list(base_view.invariant_predicates)
            base_attributes = list(base_view.core_attributes)
        else:
            core_relations = []
            core_joins = []
            base_predicates = []
            base_attributes = []

        # Add specialized relations
        for rel in specialized_features["relations"]:
            if rel not in core_relations:
                core_relations.append(rel)

        # Add specialized joins
        for join in specialized_features["joins"]:
            if join not in core_joins:
                core_joins.append(join)

        # Specialized predicates are the key differentiator
        specialized_predicates = specialized_features["predicates"]

        # Add specialized attributes
        for attr in specialized_features["attributes"]:
            if attr not in base_attributes:
                base_attributes.append(attr)

        if not core_relations:
            return None

        # Collect table aliases from cluster patterns
        table_aliases: dict[str, str] = {}
        for p in cluster_patterns:
            if p.signature.table_aliases:
                table_aliases.update(p.signature.table_aliases)
        # Also inherit from base view if available
        if base_view and base_view.table_aliases:
            merged_aliases = dict(table_aliases)
            merged_aliases.update(base_view.table_aliases)
            table_aliases = merged_aliases

        # Generate name
        base_name = base_view.suggested_name if base_view else "spec"
        name = f"{base_name}_{cluster.id}"

        all_predicates = base_predicates + specialized_predicates

        return ViewCandidate(
            id=str(uuid4()),
            suggested_name=name,
            cluster_id=cluster.id,
            core_relations=core_relations,
            table_aliases=table_aliases,
            core_joins=core_joins,
            core_predicates=specialized_predicates,
            core_attributes=base_attributes,
            candidate_keys=base_view.candidate_keys if base_view else [],
            proposed_grain=base_view.proposed_grain if base_view else "Specialized grain",
            support_score=0.0,
            coverage_weight=cluster.total_weight,
            pattern_count=len(cluster_patterns),
            invariant_predicates=all_predicates,
            optional_predicates=[],
            metadata={
                "view_type": "specialized",
                "cluster_id": cluster.id,
                **({"extends_base": base_view.suggested_name} if base_view else {}),
            },
        )
