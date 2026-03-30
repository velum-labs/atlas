"""Pydantic schemas for pipeline analysis agent output.

These models define the structured output contract between the LLM provider
and the Atlas learning pipeline.  All fields are validated before any
learning data is written to the store.
"""

from __future__ import annotations

from pydantic import BaseModel


class EdgeEnrichment(BaseModel):
    """Enrichment metadata inferred by the pipeline analysis agent for one edge."""

    source_table: str
    dest_table: str
    transport_kind: str  # one of TransportKind values or UNKNOWN
    schedule: str | None = None
    strategy: str | None = None
    write_disposition: str | None = None
    watermark_column: str | None = None
    owner: str | None = None
    confidence_note: str


class PipelineAnalysisResult(BaseModel):
    """Full result returned by the pipeline analysis agent for a batch of edges."""

    edges: list[EdgeEnrichment] = []
    repo_summary: str | None = None


class AssetAnnotation(BaseModel):
    """Business metadata annotation inferred by the annotator agent for one asset."""

    asset_id: str
    ownership: str | None = None          # team or person
    granularity: str | None = None        # 'one row per user per day'
    join_keys: list[str] = []             # ['user_id', 'date']
    freshness_guarantee: str | None = None  # 'updated hourly' / 'SLA: 6h'
    business_logic_summary: str | None = None  # 1-2 sentence description
    sensitivity: str | None = None        # 'PII', 'financial', 'public'


class AnnotationResult(BaseModel):
    """Full result returned by the annotator agent for a batch of assets."""

    annotations: list[AssetAnnotation]
    repo_summary: str | None = None


# Backward compatibility alias.
AssetEnrichmentResult = AnnotationResult


class FileRelevance(BaseModel):
    """Relevance score for a single repository file, as returned by the codebase explorer."""

    path: str  # relative path within the repository
    relevance_score: float  # 0.0 (not relevant) to 1.0 (highly relevant)
    reason: str


class ExplorerResult(BaseModel):
    """Full result returned by the codebase explorer agent."""

    files: list[FileRelevance]
    repo_structure_summary: str
