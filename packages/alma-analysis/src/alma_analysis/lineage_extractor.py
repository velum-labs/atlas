"""Compatibility wrapper around the canonical sqlkit lineage helpers."""

from alma_sqlkit.lineage import ColumnEdge, LineageResult, extract_lineage

__all__ = ["ColumnEdge", "LineageResult", "extract_lineage"]
