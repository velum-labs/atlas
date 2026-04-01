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

"""

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
]
