"""alma-analysis — Pure analysis functions for Alma Atlas.

Provides stateless, side-effect-free functions for computing lineage,
extracting edges, and identifying consumers from raw connector observations.

All functions take plain data in, return plain data out — no I/O, no store
dependencies.
"""

__version__ = "0.1.0"

from alma_analysis.consumer_identity import ConsumerIdentity, identify_pg_consumer
from alma_analysis.edge_discovery import EdgeDiscoveryConfig, EdgeDiscoveryEngine
from alma_analysis.extract_tables import extract_tables_from_sql
from alma_analysis.lineage_extractor import LineageResult, extract_lineage

__all__ = [
    "ConsumerIdentity",
    "EdgeDiscoveryConfig",
    "EdgeDiscoveryEngine",
    "LineageResult",
    "extract_lineage",
    "extract_tables_from_sql",
    "identify_pg_consumer",
]
