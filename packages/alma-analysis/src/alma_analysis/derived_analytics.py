"""Derived analytics engine — post-processing step for scanner TRAFFIC results.

Computes four summary types from a list of ObservedQueryEvents:

  QuerySourceBreakdown  — distribution of traffic by source type
  FrequentQuery         — top recurring query patterns by normalised hash
  TableAccessSummary    — per-table access aggregates
  UserActivitySummary   — per-user query counts and DML vs SELECT split

All four are assembled into a DerivedAnalytics container by compute_analytics().

Server-side aggregation
-----------------------
compute_analytics_server_side() accepts a SourceAdapterV2 adapter and attempts
to push rollup SQL down to the source via execute_query(). If the adapter does
not support execute_query() or raises any exception, it falls back to
compute_analytics() transparently.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from alma_connectors.source_adapter import ObservedQueryEvent
from alma_sqlkit.normalize import normalize_sql as canonicalize_sql

if TYPE_CHECKING:
    from alma_connectors.source_adapter import PersistedSourceAdapter
    from alma_connectors.source_adapter_v2 import SourceAdapterV2

__all__ = [
    "QuerySourceBreakdown",
    "FrequentQuery",
    "TableAccessSummary",
    "UserActivitySummary",
    "DerivedAnalytics",
    "compute_analytics",
    "compute_analytics_server_side",
]

# ---------------------------------------------------------------------------
# Source-type classification
# ---------------------------------------------------------------------------

_SOURCE_TYPES = ("ad_hoc", "scheduled", "service_account", "batch")

# Patterns that suggest a service account database user
_SERVICE_ACCOUNT_RE = re.compile(
    r"(svc[_\-]|service[_\-]|sa[_\-]|robot[_\-]|bot[_\-]|automation)",
    re.IGNORECASE,
)

# Patterns that suggest a batch / ETL job user or application name
_BATCH_RE = re.compile(
    r"(etl|batch|pipeline|loader|importer|sync|worker)",
    re.IGNORECASE,
)


def _classify_source_type(event: ObservedQueryEvent) -> str:
    """Return one of the four source-type labels for a single event.

    Classification priority:
    1. Airflow / scheduled — dag_id present, or consumer_source_type == "airflow"
    2. Batch — batch/ETL keywords in application name or user, or consumer_source_type label
    3. Service account — service-account patterns in user or app label
    4. Ad-hoc — everything else (interactive users, unknown consumers)
    """
    meta: dict[str, Any] = dict(event.metadata) if event.metadata else {}

    # --- scheduled (Airflow / cron) ---
    if meta.get("dag_id") or meta.get("consumer_source_type") == "airflow":
        return "scheduled"

    # Check labels dict for Airflow keys
    labels: dict[str, str] = meta.get("labels") or {}
    if labels.get("dag_id") or labels.get("airflow_dag"):
        return "scheduled"

    # --- service_account / batch from consumer_source_type label ---
    consumer_type: str = str(meta.get("consumer_source_type") or "")

    # Check application name / source_name for batch or service-account keywords
    app_hint = event.source_name or ""
    user_hint = event.database_user or ""

    if _BATCH_RE.search(app_hint) or _BATCH_RE.search(user_hint):
        return "batch"

    if consumer_type == "application":
        if _SERVICE_ACCOUNT_RE.search(app_hint) or _SERVICE_ACCOUNT_RE.search(user_hint):
            return "service_account"
        # Generic application consumer → service_account (non-interactive)
        return "service_account"

    if _SERVICE_ACCOUNT_RE.search(app_hint) or _SERVICE_ACCOUNT_RE.search(user_hint):
        return "service_account"

    return "ad_hoc"


# ---------------------------------------------------------------------------
# Query normalisation / hashing
# ---------------------------------------------------------------------------

_WHITESPACE = re.compile(r"\s+")


def _normalize_sql(sql: str) -> str:
    """Return a normalised version of *sql* suitable for grouping.

    Delegates literal anonymization and AST-level cleanup to `alma_sqlkit`,
    then applies a small amount of formatting normalization for stable hashes.
    """
    text = canonicalize_sql(sql).strip().lower()
    text = _WHITESPACE.sub(" ", text)
    return text


def _query_hash(normalized_sql: str) -> str:
    """Return a 16-char hex hash of the normalised SQL."""
    return hashlib.sha256(normalized_sql.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class QuerySourceBreakdown:
    """Distribution of traffic events across source types.

    Attributes:
        ad_hoc:         Interactive / one-off queries by human users.
        scheduled:      Queries executed by schedulers (Airflow DAGs, cron).
        service_account: Queries from non-human service/robot accounts.
        batch:          ETL / bulk-load pipeline queries.
        total:          Total number of events analysed.
    """

    ad_hoc: int = 0
    scheduled: int = 0
    service_account: int = 0
    batch: int = 0
    total: int = 0

    @property
    def fractions(self) -> dict[str, float]:
        """Return each source type as a fraction of total (0.0–1.0)."""
        if self.total == 0:
            return {t: 0.0 for t in _SOURCE_TYPES}
        return {t: getattr(self, t) / self.total for t in _SOURCE_TYPES}


@dataclass
class FrequentQuery:
    """Aggregated statistics for a group of structurally identical queries.

    Attributes:
        query_hash:    Short hex identifier of the normalised SQL form.
        execution_count: Number of times this query pattern was observed.
        sample_sql:    One representative (raw) SQL string from the group.
        avg_duration_ms: Average execution duration across all occurrences.
            ``None`` if no duration data is available.
        avg_bytes_processed: Average bytes processed across all occurrences.
            ``None`` if no bytes data is available.
        source_types:  Set of source-type labels that ran this query pattern.
    """

    query_hash: str
    execution_count: int
    sample_sql: str
    avg_duration_ms: float | None
    avg_bytes_processed: float | None
    source_types: set[str] = field(default_factory=set)


@dataclass
class TableAccessSummary:
    """Per-table access aggregates derived from referenced_tables metadata.

    Attributes:
        table_name:     Fully-qualified table reference as reported by the adapter.
        access_count:   Number of events that referenced this table.
        distinct_users: Set of database_user values that accessed this table.
        query_types:    Set of query_type values seen for this table.
    """

    table_name: str
    access_count: int = 0
    distinct_users: set[str] = field(default_factory=set)
    query_types: set[str] = field(default_factory=set)


@dataclass
class UserActivitySummary:
    """Per-user query activity aggregates.

    Attributes:
        user:           database_user value (or ``"<unknown>"`` when absent).
        total_queries:  Total number of queries attributed to this user.
        select_count:   Queries whose query_type is ``"select"`` (case-insensitive).
        dml_count:      Queries whose query_type is INSERT / UPDATE / DELETE / MERGE.
        total_bytes_processed: Sum of bytes_processed from metadata where available.
    """

    user: str
    total_queries: int = 0
    select_count: int = 0
    dml_count: int = 0
    total_bytes_processed: float = 0.0


@dataclass
class DerivedAnalytics:
    """Top-level container for all derived analytics over a set of traffic events.

    Attributes:
        source_breakdown: Distribution of events by source type.
        frequent_queries: Top recurring query patterns sorted by execution count (desc).
        table_access:     Per-table access summaries sorted by access count (desc).
        user_activity:    Per-user activity summaries sorted by total_queries (desc).
        event_count:      Total number of events processed.
    """

    source_breakdown: QuerySourceBreakdown
    frequent_queries: list[FrequentQuery]
    table_access: list[TableAccessSummary]
    user_activity: list[UserActivitySummary]
    event_count: int


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_DML_TYPES = frozenset({"insert", "update", "delete", "merge", "truncate"})


def _get_bytes(event: ObservedQueryEvent) -> float | None:
    """Extract bytes_processed from event metadata, if present."""
    meta = event.metadata or {}
    raw = meta.get("bytes_processed")
    if raw is None:
        raw = meta.get("total_bytes_processed")
    if raw is None or not isinstance(raw, str | int | float):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _build_source_breakdown(events: list[ObservedQueryEvent]) -> QuerySourceBreakdown:
    counts: dict[str, int] = {t: 0 for t in _SOURCE_TYPES}
    for ev in events:
        counts[_classify_source_type(ev)] += 1
    return QuerySourceBreakdown(
        ad_hoc=counts["ad_hoc"],
        scheduled=counts["scheduled"],
        service_account=counts["service_account"],
        batch=counts["batch"],
        total=len(events),
    )


def _build_frequent_queries(events: list[ObservedQueryEvent]) -> list[FrequentQuery]:
    # hash → (count, sample_sql, durations, bytes, source_types)
    groups: dict[str, dict[str, Any]] = {}

    for ev in events:
        norm = _normalize_sql(ev.sql)
        h = _query_hash(norm)
        if h not in groups:
            groups[h] = {
                "count": 0,
                "sample_sql": ev.sql,
                "durations": [],
                "bytes": [],
                "source_types": set(),
            }
        g = groups[h]
        g["count"] += 1
        if ev.duration_ms is not None:
            g["durations"].append(ev.duration_ms)
        b = _get_bytes(ev)
        if b is not None:
            g["bytes"].append(b)
        g["source_types"].add(_classify_source_type(ev))

    result = []
    for h, g in groups.items():
        durations = g["durations"]
        bytes_list = g["bytes"]
        result.append(
            FrequentQuery(
                query_hash=h,
                execution_count=g["count"],
                sample_sql=g["sample_sql"],
                avg_duration_ms=sum(durations) / len(durations) if durations else None,
                avg_bytes_processed=sum(bytes_list) / len(bytes_list) if bytes_list else None,
                source_types=g["source_types"],
            )
        )

    result.sort(key=lambda q: q.execution_count, reverse=True)
    return result


def _build_table_access(events: list[ObservedQueryEvent]) -> list[TableAccessSummary]:
    tables: dict[str, TableAccessSummary] = {}

    for ev in events:
        meta = ev.metadata or {}
        referenced_raw = meta.get("referenced_tables")
        referenced = (
            [item for item in referenced_raw if isinstance(item, str)]
            if isinstance(referenced_raw, list)
            else []
        )

        for table_name in referenced:
            if not table_name:
                continue
            if table_name not in tables:
                tables[table_name] = TableAccessSummary(table_name=table_name)
            summary = tables[table_name]
            summary.access_count += 1
            if ev.database_user:
                summary.distinct_users.add(ev.database_user)
            if ev.query_type:
                summary.query_types.add(ev.query_type.lower())

    result = list(tables.values())
    result.sort(key=lambda t: t.access_count, reverse=True)
    return result


def _build_user_activity(events: list[ObservedQueryEvent]) -> list[UserActivitySummary]:
    users: dict[str, UserActivitySummary] = {}

    for ev in events:
        user_key = ev.database_user or "<unknown>"
        if user_key not in users:
            users[user_key] = UserActivitySummary(user=user_key)
        s = users[user_key]
        s.total_queries += 1

        qtype = (ev.query_type or "").lower()
        if qtype == "select":
            s.select_count += 1
        elif qtype in _DML_TYPES:
            s.dml_count += 1

        b = _get_bytes(ev)
        if b is not None:
            s.total_bytes_processed += b

    result = list(users.values())
    result.sort(key=lambda u: u.total_queries, reverse=True)
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_analytics(events: list[ObservedQueryEvent]) -> DerivedAnalytics:
    """Compute all derived analytics from a list of ObservedQueryEvents.

    Runs all four summary computations client-side (no adapter calls required).

    Args:
        events: Traffic events previously extracted via TRAFFIC capability.

    Returns:
        DerivedAnalytics container with all four summaries populated.
    """
    return DerivedAnalytics(
        source_breakdown=_build_source_breakdown(events),
        frequent_queries=_build_frequent_queries(events),
        table_access=_build_table_access(events),
        user_activity=_build_user_activity(events),
        event_count=len(events),
    )


async def compute_analytics_server_side(
    adapter: SourceAdapterV2,
    persisted: PersistedSourceAdapter,
    events: list[ObservedQueryEvent],
) -> DerivedAnalytics:
    """Compatibility wrapper around client-side analytics computation.

    The analysis package is intentionally pure and does not perform adapter I/O.
    This async wrapper preserves the historical public API while delegating
    directly to :func:`compute_analytics`.
    """
    del adapter, persisted
    return compute_analytics(events)
