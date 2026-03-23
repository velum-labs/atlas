"""alma-connectors — Source adapters for Alma Atlas.

Provides ``SourceAdapter`` implementations for each supported data platform:

- ``BigQueryAdapter``  — Google BigQuery (requires ``alma-connectors[bigquery]``)
- ``PostgresAdapter``  — PostgreSQL (requires ``alma-connectors[postgres]``)
- ``SnowflakeAdapter`` — Snowflake (requires ``alma-connectors[snowflake]``)
- ``DbtAdapter``       — dbt projects (manifest.json, no extra deps)

All adapters implement the ``SourceAdapter`` protocol defined in
``alma_connectors.protocol`` and produce domain objects from
``alma_connectors.domain``.
"""

__version__ = "0.1.0"

from alma_connectors.domain import ColumnDef, SchemaSnapshot, TrafficObservationResult
from alma_connectors.protocol import SourceAdapter

__all__ = [
    "ColumnDef",
    "SchemaSnapshot",
    "SourceAdapter",
    "TrafficObservationResult",
]
