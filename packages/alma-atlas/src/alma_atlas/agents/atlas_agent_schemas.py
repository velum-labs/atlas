"""Pydantic output schemas for the Atlas inner agent (Phase 4)."""

from __future__ import annotations

from pydantic import BaseModel


class ColumnContext(BaseModel):
    """Context for one column relevant to a data question."""

    column_name: str
    type: str
    annotation: str | None = None
    top_values: list[str] = []
    null_fraction: float | None = None
    warning: str | None = None


class JoinRecommendation(BaseModel):
    """A recommended join path between two or more tables."""

    tables: list[str]
    join_path: str  # e.g. "cards.setCode = set_translations.setCode"
    guidance: str | None = None  # from join_guidance annotation


class ContextPackage(BaseModel):
    """Curated context package returned by atlas_context."""

    relevant_tables: list[str] = []
    recommended_joins: list[JoinRecommendation] = []
    column_context: list[ColumnContext] = []
    warnings: list[str] = []
    evidence_interpretation: str | None = None
    summary: str | None = None  # 1-2 sentence overview


class AskResult(BaseModel):
    """Result returned by atlas_ask -- an explanation grounded in Atlas knowledge."""

    answer: str = ""
    sources: list[str] = []  # tables/columns referenced
    caveats: list[str] = []


class VerificationResult(BaseModel):
    """Result returned by atlas_verify (static or deep mode)."""

    valid: bool = True
    warnings: list[str] = []
    suggestions: list[str] = []
    analysis: str | None = None  # LLM analysis (deep mode only)
