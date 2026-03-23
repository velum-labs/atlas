"""alma-analysis — Pure analysis functions for Alma Atlas.

Provides stateless, side-effect-free functions for computing lineage,
extracting edges, and identifying consumers from raw connector observations.

All functions take plain data in, return plain data out — no I/O, no store
dependencies. The alma-atlas pipeline orchestrates the store writes.
"""

__version__ = "0.1.0"

from alma_analysis.consumers import ConsumerObservation, identify_consumers
from alma_analysis.edges import Edge, extract_edges
from alma_analysis.lineage import LineageGraph, compute_lineage

__all__ = [
    "ConsumerObservation",
    "Edge",
    "LineageGraph",
    "compute_lineage",
    "extract_edges",
    "identify_consumers",
]
