"""Pydantic schemas for pipeline analysis agent output.

These models define the structured output contract between the LLM provider
and the Atlas enrichment pipeline.  All fields are validated before any
enrichment data is written to the store.
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

    edges: list[EdgeEnrichment]
    repo_summary: str | None = None
