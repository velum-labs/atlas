"""Relational algebra expression normalizer package.

This package provides normalization and transformation utilities for
RA expressions.

Modules:
- core: Main RANormalizer class and helper functions

All public APIs are re-exported from this module for backward compatibility.
"""

# Re-export everything from core for backward compatibility
from alma_algebrakit.normalizer.core import (
    JoinEdge,
    RANormalizer,
    TopLevelOperators,
    extract_detailed_join_graph,
    extract_top_level_operators,
    wrap_with_operators,
)

__all__ = [
    "RANormalizer",
    "JoinEdge",
    "TopLevelOperators",
    "extract_detailed_join_graph",
    "extract_top_level_operators",
    "wrap_with_operators",
]
