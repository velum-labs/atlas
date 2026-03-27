"""Build Atlas analysis workloads from snapshot query records."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from alma_algebrakit.learning.workload import Workload
from alma_sqlkit import ParsingConfig, SQLParser

from alma_atlas.analysis.models import SkippedQuery
from alma_atlas.analysis.snapshot import QueryRecord

_SIMPLE_BOOTSTRAP_QUERY_PATTERN = re.compile(
    r"^\s*select\s+\w+\s+from\s+[\w.]+\s+limit\s+\d+\s*$",
    re.IGNORECASE,
)


@dataclass
class WorkloadBuildResult:
    workload: Workload
    total_input_queries: int
    parsed_query_count: int
    skipped_queries: list[SkippedQuery] = field(default_factory=list)


def build_workload(
    queries: list[QueryRecord],
) -> WorkloadBuildResult:
    """Convert snapshot query records into a weighted algebrakit workload."""

    parser_cache: dict[str, SQLParser] = {}
    workload = Workload(
        name="atlas-analysis",
        description="Atlas-native workload derived from stored query observations",
    )
    skipped_queries: list[SkippedQuery] = []

    for query in queries:
        if _should_skip_query(query):
            skipped_queries.append(
                SkippedQuery(
                    fingerprint=query.fingerprint,
                    source=query.source,
                    reason="bootstrap_or_trivial",
                    sql_text=query.sql_text,
                )
            )
            continue

        try:
            expression = _parse_query(query, parser_cache)
        except ValueError as error:
            skipped_queries.append(
                SkippedQuery(
                    fingerprint=query.fingerprint,
                    source=query.source,
                    reason=f"parse_error:{error}",
                    sql_text=query.sql_text,
                )
            )
            continue

        workload.add_pattern(
            expression,
            weight=float(query.execution_count),
            pattern_id=query.fingerprint,
            store_expression=True,
            source=query.source,
            sample_sql=query.sql_text,
            execution_count=query.execution_count,
        )

    return WorkloadBuildResult(
        workload=workload,
        total_input_queries=len(queries),
        parsed_query_count=len(workload.patterns),
        skipped_queries=skipped_queries,
    )


def _should_skip_query(query: QueryRecord) -> bool:
    sql = query.sql_text.strip()
    if not sql or not query.tables:
        return True

    sql_lower = sql.lower()

    if sql_lower == "select 1":
        return True

    if (
        sql_lower.startswith("select id from")
        or sql_lower.startswith("select\n")
        and "select id from" in sql_lower.replace("\n", " ").replace("  ", " ")
    ):
        if "limit" in sql_lower and "join" not in sql_lower:
            return True

    if "drizzle" in sql_lower and "__drizzle_migrations" in sql_lower:
        return True

    return _SIMPLE_BOOTSTRAP_QUERY_PATTERN.match(sql_lower) is not None


def _parse_query(
    query: QueryRecord,
    parser_cache: dict[str, SQLParser],
):
    dialects = _candidate_dialects(query.source)
    last_error: ValueError | None = None

    for dialect in dialects:
        parser = parser_cache.get(dialect)
        if parser is None:
            parser = SQLParser(ParsingConfig(dialect=dialect))
            parser_cache[dialect] = parser

        try:
            return parser.parse(query.sql_text)
        except ValueError as error:
            last_error = error

    if last_error is None:
        raise ValueError("no SQL parser dialects configured")
    raise last_error


def _candidate_dialects(source: str) -> list[str]:
    preferred = _infer_dialect(source)
    dialects = [preferred, "postgres", "bigquery", "snowflake", "duckdb"]
    seen: set[str] = set()
    ordered: list[str] = []
    for dialect in dialects:
        if dialect in seen:
            continue
        seen.add(dialect)
        ordered.append(dialect)
    return ordered


def _infer_dialect(source: str) -> str:
    lower_source = source.lower()
    if lower_source.startswith(("bigquery", "bq")):
        return "bigquery"
    if lower_source.startswith(("snowflake", "sf")):
        return "snowflake"
    if lower_source.startswith(("duckdb",)):
        return "duckdb"
    return "postgres"
