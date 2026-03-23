"""Consumer identification for alma-analysis.

Infers data consumers (users, services, dashboards) from query traffic
observations. Groups queries by their source user/service and maps them
to the assets they depend on.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from alma_algebrakit.bound.fingerprint import fingerprint_sql
from alma_sqlkit.dialect import Dialect
from alma_sqlkit.parse import extract_tables


@dataclass
class ConsumerObservation:
    """An inferred consumer derived from query traffic."""

    consumer_id: str
    kind: str
    name: str
    asset_ids: list[str] = field(default_factory=list)
    query_fingerprints: list[str] = field(default_factory=list)


def identify_consumers(
    queries: list[dict],
    source_type: str = "unknown",
    dialect: Dialect | str = Dialect.ANSI,
    known_asset_ids: set[str] | None = None,
) -> list[ConsumerObservation]:
    """Identify consumers from a list of query observation dicts.

    Each query dict should have:
    - ``sql``: The SQL string.
    - ``user`` (optional): The user/service that executed the query.

    Args:
        queries: List of query observation dicts from a source connector.
        source_type: The source platform type (for consumer ID namespacing).
        dialect: SQL dialect for parsing.
        known_asset_ids: Optional set of known asset IDs to filter table references.

    Returns:
        List of ConsumerObservation instances, one per unique user/service.
    """
    # Group by user/consumer identifier
    consumer_map: dict[str, ConsumerObservation] = {}

    for query in queries:
        sql = query.get("sql", "")
        user = query.get("user") or "unknown"
        consumer_id = f"{source_type}:user:{user}"

        if consumer_id not in consumer_map:
            consumer_map[consumer_id] = ConsumerObservation(
                consumer_id=consumer_id,
                kind="user",
                name=user,
            )

        obs = consumer_map[consumer_id]

        try:
            fingerprint = fingerprint_sql(sql, dialect=dialect)
            if fingerprint not in obs.query_fingerprints:
                obs.query_fingerprints.append(fingerprint)

            tables = extract_tables(sql, dialect=dialect)
            for table in tables:
                if known_asset_ids is not None and table not in known_asset_ids:
                    continue
                if table not in obs.asset_ids:
                    obs.asset_ids.append(table)
        except Exception:
            continue

    return list(consumer_map.values())
