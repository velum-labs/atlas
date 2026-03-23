"""Learning module for algebrakit.

This module provides SQL-agnostic view learning from query workloads.
The primary approach is **topology-based learning**, which extracts join
structure (relations + join edges) and ignores filter predicates.

## Quick Start

```python
from alma_algebrakit.learning import learn_views, TopologyViewLearner

# Learn views from RA expressions
result = learn_views(expressions)
for view in result.views:
    print(f"{view.name}: {view.relations}")

# Or use the learner directly for more control
learner = TopologyViewLearner()
result = learner.learn_views(expressions, weights=[1.0, 2.0, 1.5])
```

## Why Topology?

Topology-based learning produces maximally reusable views:
- **No predicates**: Views capture join structure, not query-specific filters
- **Simple & fast**: Set intersection instead of NP-complete containment
- **Universal**: Works with any RA (joins, aggregations, unions, etc.)
- **Stable**: Views don't change when filter patterns change

## Deprecated Approaches

The following are deprecated and will be removed in a future release:
- `HybridViewLearner`, `learn_views_hybrid` - Use `TopologyViewLearner` instead
- `ViewDeriver`, `HierarchicalViewDeriver` - Use `TopologyViewLearner` instead
- `gcs_all`, `greatest_common_subexpression` - Use `topology_gcs` instead

The GCS and feature-based approaches attempted to capture predicates in views,
but this reduces reusability and adds complexity without clear benefit.
"""

import warnings
from typing import TYPE_CHECKING

# Clustering (used internally)
from alma_algebrakit.learning.clustering import (
    ClusteringConfig,
    PatternCluster,
    PatternClusterer,
    PatternInstance,
)

# Evidence and analysis (still useful for topology)
from alma_algebrakit.learning.evidence import (
    JoinChain,
    JoinGraphAnalyzer,
    JoinGraphEdge,
    JoinGraphEvidence,
    JoinGraphNode,
    StarSchemaResult,
    TopologySuggestion,
    ViewBoundary,
)

# Pattern extraction (used internally, still useful)
from alma_algebrakit.learning.patterns import (
    JoinEdgePattern,
    PatternExtractionVisitor,
    PatternExtractor,
    PatternSignature,
)

# =============================================================================
# Primary API: Topology Learning (Recommended)
# =============================================================================
# Topology types
from alma_algebrakit.learning.topology import (
    DEFAULT_NORMALIZATION,
    EdgeBasedCanonicalizer,
    JoinEdge,
    SelfJoinCanonicalizer,
    TableRef,
    Topology,
    TopologyExtractor,
    TopologyNormalization,
    TopologyResult,
    extract_topology,
    is_subtopology,
    topology_gcs,
    topology_similarity,
    topology_to_ra,
)

# Topology learner
from alma_algebrakit.learning.topology_learner import (
    LearnedTopologyView,
    TopologyCluster,
    TopologyClusteringConfig,
    TopologyLearningResult,
    TopologyViewLearner,
    extract_all_topologies,
    find_common_topology,
    learn_topology_views,
)

# Workload management
from alma_algebrakit.learning.workload import (
    Workload,
    WorkloadStatistics,
    compute_workload_statistics,
)

# =============================================================================
# Convenience Aliases
# =============================================================================

# Make topology the default
ViewLearner = TopologyViewLearner
learn_views = learn_topology_views
LearningResult = TopologyLearningResult
LearnedView = LearnedTopologyView


# =============================================================================
# Deprecated: GCS-Based Learning
# =============================================================================


def _warn_gcs_deprecated(name: str) -> None:
    warnings.warn(
        f"{name} is deprecated. Use topology-based learning instead: "
        "TopologyViewLearner or learn_views(). "
        "Topology learning is simpler, faster, and produces more reusable views.",
        DeprecationWarning,
        stacklevel=3,
    )


# Lazy imports for deprecated modules to avoid import overhead
if TYPE_CHECKING:
    from alma_algebrakit.learning.derivation import (
        DerivationConfig as _DerivationConfig,
    )
    from alma_algebrakit.learning.derivation import (
        HierarchicalConfig as _HierarchicalConfig,
    )
    from alma_algebrakit.learning.derivation import (
        HierarchicalViewDeriver as _HierarchicalViewDeriver,
    )
    from alma_algebrakit.learning.derivation import (
        HierarchicalViewResult as _HierarchicalViewResult,
    )
    from alma_algebrakit.learning.derivation import (
        ViewCandidate as _ViewCandidate,
    )
    from alma_algebrakit.learning.derivation import (
        ViewDeriver as _ViewDeriver,
    )
    from alma_algebrakit.learning.evidence import (
        DEFAULT_GLOBAL_THRESHOLD as _DEFAULT_GLOBAL_THRESHOLD,
    )
    from alma_algebrakit.learning.evidence import (
        DEFAULT_HIGH_SUPPORT_THRESHOLD as _DEFAULT_HIGH_SUPPORT_THRESHOLD,
    )
    from alma_algebrakit.learning.evidence import (
        DEFAULT_INVARIANT_THRESHOLD as _DEFAULT_INVARIANT_THRESHOLD,
    )
    from alma_algebrakit.learning.evidence import (
        DEFAULT_KEY_THRESHOLD as _DEFAULT_KEY_THRESHOLD,
    )
    from alma_algebrakit.learning.evidence import (
        DEFAULT_LOCAL_THRESHOLD as _DEFAULT_LOCAL_THRESHOLD,
    )
    from alma_algebrakit.learning.evidence import (
        DEFAULT_MIN_CLUSTERS as _DEFAULT_MIN_CLUSTERS,
    )
    from alma_algebrakit.learning.evidence import (
        DEFAULT_MIN_CLUSTERS_FOR_BASE as _DEFAULT_MIN_CLUSTERS_FOR_BASE,
    )
    from alma_algebrakit.learning.evidence import (
        FEATURE_PREFIX_GROUP_BY as _FEATURE_PREFIX_GROUP_BY,
    )
    from alma_algebrakit.learning.evidence import (
        FEATURE_PREFIX_JOIN as _FEATURE_PREFIX_JOIN,
    )
    from alma_algebrakit.learning.evidence import (
        FEATURE_PREFIX_PREDICATE as _FEATURE_PREFIX_PREDICATE,
    )
    from alma_algebrakit.learning.evidence import (
        FEATURE_PREFIX_PROJECTION as _FEATURE_PREFIX_PROJECTION,
    )
    from alma_algebrakit.learning.evidence import (
        FEATURE_PREFIX_RELATION as _FEATURE_PREFIX_RELATION,
    )
    from alma_algebrakit.learning.evidence import (
        FEATURE_PREFIX_SEPARATOR as _FEATURE_PREFIX_SEPARATOR,
    )
    from alma_algebrakit.learning.evidence import (
        EvidenceComputer as _EvidenceComputer,
    )
    from alma_algebrakit.learning.evidence import (
        FeatureDistribution as _FeatureDistribution,
    )
    from alma_algebrakit.learning.evidence import (
        FeatureSupport as _FeatureSupport,
    )
    from alma_algebrakit.learning.evidence import (
        GlobalEvidenceComputer as _GlobalEvidenceComputer,
    )
    from alma_algebrakit.learning.evidence import (
        GlobalFeatureSupport as _GlobalFeatureSupport,
    )
    from alma_algebrakit.learning.gcs import (
        CQExtractionResult as _CQExtractionResult,
    )
    from alma_algebrakit.learning.gcs import (
        GCSResult as _GCSResult,
    )
    from alma_algebrakit.learning.gcs import (
        cq_to_ra as _cq_to_ra,
    )
    from alma_algebrakit.learning.gcs import (
        extract_common_structure as _extract_common_structure,
    )
    from alma_algebrakit.learning.gcs import (
        gcs_all as _gcs_all,
    )
    from alma_algebrakit.learning.gcs import (
        greatest_common_subexpression as _greatest_common_subexpression,
    )
    from alma_algebrakit.learning.gcs import (
        is_spj_query as _is_spj_query,
    )
    from alma_algebrakit.learning.gcs import (
        ra_to_cq as _ra_to_cq,
    )
    from alma_algebrakit.learning.hybrid import (
        HybridLearningResult as _HybridLearningResult,
    )
    from alma_algebrakit.learning.hybrid import (
        HybridViewLearner as _HybridViewLearner,
    )
    from alma_algebrakit.learning.hybrid import (
        LearnedView as _LearnedViewHybrid,
    )
    from alma_algebrakit.learning.hybrid import (
        learn_verified_views as _learn_verified_views,
    )
    from alma_algebrakit.learning.hybrid import (
        learn_views_hybrid as _learn_views_hybrid,
    )


def __getattr__(name: str):  # noqa: C901
    """Lazy loading for deprecated symbols with deprecation warnings."""

    # GCS module (deprecated)
    if name == "CQExtractionResult":
        _warn_gcs_deprecated("CQExtractionResult")
        from alma_algebrakit.learning.gcs import CQExtractionResult

        return CQExtractionResult
    if name == "GCSResult":
        _warn_gcs_deprecated("GCSResult")
        from alma_algebrakit.learning.gcs import GCSResult

        return GCSResult
    if name == "cq_to_ra":
        _warn_gcs_deprecated("cq_to_ra")
        from alma_algebrakit.learning.gcs import cq_to_ra

        return cq_to_ra
    if name == "extract_common_structure":
        _warn_gcs_deprecated("extract_common_structure")
        from alma_algebrakit.learning.gcs import extract_common_structure

        return extract_common_structure
    if name == "gcs_all":
        _warn_gcs_deprecated("gcs_all")
        from alma_algebrakit.learning.gcs import gcs_all

        return gcs_all
    if name == "greatest_common_subexpression":
        _warn_gcs_deprecated("greatest_common_subexpression")
        from alma_algebrakit.learning.gcs import greatest_common_subexpression

        return greatest_common_subexpression
    if name == "is_spj_query":
        _warn_gcs_deprecated("is_spj_query")
        from alma_algebrakit.learning.gcs import is_spj_query

        return is_spj_query
    if name == "ra_to_cq":
        _warn_gcs_deprecated("ra_to_cq")
        from alma_algebrakit.learning.gcs import ra_to_cq

        return ra_to_cq

    # Hybrid module (deprecated)
    if name == "HybridLearningResult":
        _warn_gcs_deprecated("HybridLearningResult")
        from alma_algebrakit.learning.hybrid import HybridLearningResult

        return HybridLearningResult
    if name == "HybridViewLearner":
        _warn_gcs_deprecated("HybridViewLearner")
        from alma_algebrakit.learning.hybrid import HybridViewLearner

        return HybridViewLearner
    if name == "learn_verified_views":
        _warn_gcs_deprecated("learn_verified_views")
        from alma_algebrakit.learning.hybrid import learn_verified_views

        return learn_verified_views
    if name == "learn_views_hybrid":
        _warn_gcs_deprecated("learn_views_hybrid")
        from alma_algebrakit.learning.hybrid import learn_views_hybrid

        return learn_views_hybrid

    # Derivation module (deprecated)
    if name == "DerivationConfig":
        _warn_gcs_deprecated("DerivationConfig")
        from alma_algebrakit.learning.derivation import DerivationConfig

        return DerivationConfig
    if name == "HierarchicalConfig":
        _warn_gcs_deprecated("HierarchicalConfig")
        from alma_algebrakit.learning.derivation import HierarchicalConfig

        return HierarchicalConfig
    if name == "HierarchicalViewDeriver":
        _warn_gcs_deprecated("HierarchicalViewDeriver")
        from alma_algebrakit.learning.derivation import HierarchicalViewDeriver

        return HierarchicalViewDeriver
    if name == "HierarchicalViewResult":
        _warn_gcs_deprecated("HierarchicalViewResult")
        from alma_algebrakit.learning.derivation import HierarchicalViewResult

        return HierarchicalViewResult
    if name == "ViewCandidate":
        _warn_gcs_deprecated("ViewCandidate")
        from alma_algebrakit.learning.derivation import ViewCandidate

        return ViewCandidate
    if name == "ViewDeriver":
        _warn_gcs_deprecated("ViewDeriver")
        from alma_algebrakit.learning.derivation import ViewDeriver

        return ViewDeriver

    # Feature-based evidence (deprecated, but keep some for internal use)
    if name in (
        "DEFAULT_GLOBAL_THRESHOLD",
        "DEFAULT_HIGH_SUPPORT_THRESHOLD",
        "DEFAULT_INVARIANT_THRESHOLD",
        "DEFAULT_KEY_THRESHOLD",
        "DEFAULT_LOCAL_THRESHOLD",
        "DEFAULT_MIN_CLUSTERS",
        "DEFAULT_MIN_CLUSTERS_FOR_BASE",
        "FEATURE_PREFIX_GROUP_BY",
        "FEATURE_PREFIX_JOIN",
        "FEATURE_PREFIX_PREDICATE",
        "FEATURE_PREFIX_PROJECTION",
        "FEATURE_PREFIX_RELATION",
        "FEATURE_PREFIX_SEPARATOR",
        "EvidenceComputer",
        "FeatureDistribution",
        "FeatureSupport",
        "GlobalEvidenceComputer",
        "GlobalFeatureSupport",
    ):
        _warn_gcs_deprecated(name)
        from alma_algebrakit.learning import evidence

        return getattr(evidence, name)

    raise AttributeError(f"module 'algebrakit.learning' has no attribute '{name}'")


__all__ = [
    # ==========================================================================
    # Primary API (Topology-Based)
    # ==========================================================================
    # Convenience aliases (recommended entry points)
    "ViewLearner",
    "learn_views",
    "LearningResult",
    "LearnedView",
    # Normalization config
    "TopologyNormalization",
    "DEFAULT_NORMALIZATION",
    "SelfJoinCanonicalizer",
    "EdgeBasedCanonicalizer",
    # Topology types
    "JoinEdge",
    "TableRef",
    "Topology",
    "TopologyExtractor",
    "TopologyResult",
    "extract_topology",
    "is_subtopology",
    "topology_gcs",
    "topology_similarity",
    "topology_to_ra",
    # Topology learner
    "LearnedTopologyView",
    "TopologyCluster",
    "TopologyClusteringConfig",
    "TopologyLearningResult",
    "TopologyViewLearner",
    "extract_all_topologies",
    "find_common_topology",
    "learn_topology_views",
    # Evidence/Analysis (still useful)
    "JoinChain",
    "JoinGraphAnalyzer",
    "JoinGraphEdge",
    "JoinGraphEvidence",
    "JoinGraphNode",
    "StarSchemaResult",
    "TopologySuggestion",
    "ViewBoundary",
    # Workload management
    "Workload",
    "WorkloadStatistics",
    "compute_workload_statistics",
    # Pattern types (internal, but exported)
    "JoinEdgePattern",
    "PatternExtractor",
    "PatternExtractionVisitor",
    "PatternSignature",
    # Clustering (internal, but exported)
    "ClusteringConfig",
    "PatternCluster",
    "PatternClusterer",
    "PatternInstance",
    # ==========================================================================
    # Deprecated (still exported for backwards compatibility)
    # ==========================================================================
    # GCS
    "CQExtractionResult",
    "GCSResult",
    "cq_to_ra",
    "extract_common_structure",
    "gcs_all",
    "greatest_common_subexpression",
    "is_spj_query",
    "ra_to_cq",
    # Hybrid
    "HybridLearningResult",
    "HybridViewLearner",
    "learn_verified_views",
    "learn_views_hybrid",
    # Derivation
    "DerivationConfig",
    "HierarchicalConfig",
    "HierarchicalViewDeriver",
    "HierarchicalViewResult",
    "ViewCandidate",
    "ViewDeriver",
    # Feature-based evidence
    "DEFAULT_GLOBAL_THRESHOLD",
    "DEFAULT_HIGH_SUPPORT_THRESHOLD",
    "DEFAULT_INVARIANT_THRESHOLD",
    "DEFAULT_KEY_THRESHOLD",
    "DEFAULT_LOCAL_THRESHOLD",
    "DEFAULT_MIN_CLUSTERS",
    "DEFAULT_MIN_CLUSTERS_FOR_BASE",
    "FEATURE_PREFIX_GROUP_BY",
    "FEATURE_PREFIX_JOIN",
    "FEATURE_PREFIX_PREDICATE",
    "FEATURE_PREFIX_PROJECTION",
    "FEATURE_PREFIX_RELATION",
    "FEATURE_PREFIX_SEPARATOR",
    "EvidenceComputer",
    "FeatureDistribution",
    "FeatureSupport",
    "GlobalEvidenceComputer",
    "GlobalFeatureSupport",
]
