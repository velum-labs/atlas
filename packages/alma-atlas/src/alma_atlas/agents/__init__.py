"""Agent enrichment package.

Exports the public, schema-level types that other modules import.

- P1: Edge transport enrichment (pipeline analysis)
- P2: Asset annotations (business metadata enrichment)
"""

from __future__ import annotations

from alma_atlas.agents.schemas import (
    AssetAnnotation,
    AssetEnrichmentResult,
    EdgeEnrichment,
    PipelineAnalysisResult,
)

__all__ = [
    "AssetAnnotation",
    "AssetEnrichmentResult",
    "EdgeEnrichment",
    "PipelineAnalysisResult",
]
