"""Lineage inference engine — SQL parsing, confidence scoring, and declared merge.

Post-processing step that:
1. Parses ObservedQueryEvent SQL to infer table-level lineage edges (INFERRED_SQL).
2. Scores inferred edges by extraction quality, query frequency, and recency.
3. Matches tables across adapters by normalized name (cross-system, HEURISTIC).
4. Merges inferred edges with declared edges, keeping declared over inferred for
   the same source→target pair.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

from alma_connectors.source_adapter import ObservedQueryEvent
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    ExtractionMeta,
    ExtractionScope,
    LineageEdge,
    LineageEdgeKind,
    LineageSnapshot,
    ScopeContext,
    SourceAdapterKindV2,
)

from alma_analysis.lineage_extractor import extract_lineage

logger = logging.getLogger(__name__)

_RECENCY_30_DAYS = timedelta(days=30)
_RECENCY_90_DAYS = timedelta(days=90)

_BASE_CONFIDENCE: dict[str, float] = {
    "column": 0.9,
    "table": 0.7,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_table_name(canonical: str) -> str:
    """Lowercase and strip schema prefix — returns just the unqualified table name."""
    return canonical.lower().rsplit(".", 1)[-1]


def _recency_multiplier(age: timedelta) -> float:
    """Return the recency decay factor for an event of the given age."""
    if age > _RECENCY_90_DAYS:
        return 0.7
    if age > _RECENCY_30_DAYS:
        return 0.9
    return 1.0


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


class _EdgeKey(NamedTuple):
    source_object: str
    target_object: str


# ---------------------------------------------------------------------------
# InferredLineageEngine
# ---------------------------------------------------------------------------


class InferredLineageEngine:
    """Builds inferred LineageEdge lists from ObservedQueryEvent traffic.

    Two types of edges are produced:

    * **INFERRED_SQL** — edges derived from SQL parsing of individual events.
      Confidence is weighted by extraction quality, query frequency, and recency.
    * **HEURISTIC** — cross-system edges where the same normalised table name
      appears under different source systems.
    """

    def __init__(
        self,
        events: list[ObservedQueryEvent],
        *,
        dialect: str = "postgres",
        now: datetime | None = None,
    ) -> None:
        self._events = events
        self._dialect = dialect
        self._now = _ensure_utc(now or datetime.now(UTC))

    # ------------------------------------------------------------------
    # INFERRED_SQL edges
    # ------------------------------------------------------------------

    def build_edges(self) -> list[LineageEdge]:
        """Parse all events and return INFERRED_SQL edges with confidence scores.

        Grouping key is (source_object, target_object).  For each pair the best
        extraction method, total occurrence count, and most-recent timestamp are
        tracked.  Confidence formula::

            freq_boost = log(count) / log(max_count) * 0.1   # 0–0.1
            confidence = min((base + freq_boost) * recency, 1.0)

        When all pairs have count == 1 (max_count == 1) the freq_boost is set to
        the maximum (0.1) since relative frequency cannot be computed.
        """
        # key → {count, best_method, most_recent}
        agg: dict[_EdgeKey, dict] = {}

        for event in self._events:
            try:
                result = extract_lineage(event.sql, dialect=self._dialect)
            except Exception:
                logger.debug("SQL parse failed for event %s, skipping", event.event_id)
                continue

            target = result.target_table or event.source_name

            for table_ref in result.source_tables:
                src = table_ref.canonical_name
                if src.lower() in result.cte_names:
                    continue

                key = _EdgeKey(source_object=src, target_object=target)
                if key not in agg:
                    agg[key] = {
                        "count": 0,
                        "best_method": "table",
                        "most_recent": None,
                    }
                rec = agg[key]
                rec["count"] += 1
                if result.extraction_method == "column":
                    rec["best_method"] = "column"
                captured = _ensure_utc(event.captured_at)
                if rec["most_recent"] is None or captured > rec["most_recent"]:
                    rec["most_recent"] = captured

        if not agg:
            return []

        max_count = max(rec["count"] for rec in agg.values())

        edges: list[LineageEdge] = []
        for key, rec in agg.items():
            base = _BASE_CONFIDENCE[rec["best_method"]]
            count = rec["count"]

            if max_count <= 1:
                freq_boost = 0.1
            else:
                freq_boost = math.log(count) / math.log(max_count) * 0.1

            most_recent: datetime = rec["most_recent"]
            age = self._now - most_recent
            recency = _recency_multiplier(age)

            confidence = min((base + freq_boost) * recency, 1.0)

            edges.append(
                LineageEdge(
                    source_object=key.source_object,
                    target_object=key.target_object,
                    edge_kind=LineageEdgeKind.INFERRED_SQL,
                    confidence=confidence,
                )
            )

        return edges

    # ------------------------------------------------------------------
    # Cross-system HEURISTIC edges
    # ------------------------------------------------------------------

    def build_cross_system_edges(self) -> list[LineageEdge]:
        """Find tables with the same normalised name appearing in different source systems.

        Normalisation: lowercase and strip schema prefix (``public.orders`` → ``orders``).
        When the same normalised name appears under ≥ 2 distinct *source_name* values a
        HEURISTIC edge is emitted for every distinct pair (deterministic order).
        """
        # norm_name → {source_name: canonical_name}  (first canonical wins per system)
        seen: dict[str, dict[str, str]] = defaultdict(dict)

        for event in self._events:
            try:
                result = extract_lineage(event.sql, dialect=self._dialect)
            except Exception:
                continue

            if result.target_table:
                norm = _normalize_table_name(result.target_table)
                seen[norm].setdefault(event.source_name, result.target_table)

            for table_ref in result.source_tables:
                if table_ref.canonical_name.lower() in result.cte_names:
                    continue
                norm = _normalize_table_name(table_ref.canonical_name)
                seen[norm].setdefault(event.source_name, table_ref.canonical_name)

        edges: list[LineageEdge] = []
        for norm_name, by_system in seen.items():
            if len(by_system) < 2:
                continue
            entries = sorted(by_system.items())  # deterministic via sort
            for i in range(len(entries)):
                for j in range(i + 1, len(entries)):
                    _, src_canonical = entries[i]
                    _, tgt_canonical = entries[j]
                    edges.append(
                        LineageEdge(
                            source_object=src_canonical,
                            target_object=tgt_canonical,
                            edge_kind=LineageEdgeKind.HEURISTIC,
                            confidence=0.5,
                            metadata={
                                "normalized_name": norm_name,
                                "cross_system": True,
                            },
                        )
                    )

        return edges


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def infer_lineage(
    events: list[ObservedQueryEvent],
    declared_edges: list[LineageEdge],
    dialect: str,
    *,
    now: datetime | None = None,
) -> LineageSnapshot:
    """Infer lineage from traffic events and merge with declared edges.

    Algorithm:
    1. Parse each event's SQL to produce INFERRED_SQL edges.
    2. Produce HEURISTIC edges for cross-system table-name matches.
    3. Discard any inferred/heuristic edge whose (source, target) pair is already
       covered by a declared edge — declared always wins.
    4. Return a LineageSnapshot containing declared + filtered inferred edges.

    Args:
        events:         Observed query events (e.g. from TrafficExtractionResult).
        declared_edges: Authoritative edges from adapters (e.g. dbt LINEAGE).
        dialect:        SQL dialect for parsing (``"postgres"``, ``"bigquery"``…).
        now:            Reference datetime for recency decay (defaults to UTC now).

    Returns:
        LineageSnapshot with merged edges and synthetic ExtractionMeta.
    """
    engine = InferredLineageEngine(events, dialect=dialect, now=now)
    inferred = engine.build_edges()
    cross_system = engine.build_cross_system_edges()

    declared_pairs: set[tuple[str, str]] = {
        (e.source_object, e.target_object) for e in declared_edges
    }

    filtered_inferred = [
        e for e in inferred if (e.source_object, e.target_object) not in declared_pairs
    ]
    filtered_cross = [
        e for e in cross_system if (e.source_object, e.target_object) not in declared_pairs
    ]

    merged = list(declared_edges) + filtered_inferred + filtered_cross
    captured_at = _ensure_utc(now or datetime.now(UTC))
    meta = ExtractionMeta(
        adapter_key="lineage_inference",
        adapter_kind=SourceAdapterKindV2.POSTGRES,
        capability=AdapterCapability.LINEAGE,
        scope_context=ScopeContext(scope=ExtractionScope.GLOBAL),
        captured_at=captured_at,
        duration_ms=0.0,
        row_count=len(merged),
    )

    return LineageSnapshot(meta=meta, edges=tuple(merged))
