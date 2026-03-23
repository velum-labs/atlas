"""BigQuery source adapter for alma-connectors.

Connects to Google BigQuery to discover datasets, tables, views, and
query traffic from INFORMATION_SCHEMA and JOBS history.

Requires: ``alma-connectors[bigquery]`` (google-cloud-bigquery)
"""

from __future__ import annotations

from typing import Any

from alma_connectors.domain import ColumnDef, QueryRecord, SchemaSnapshot, TrafficObservationResult


class BigQueryAdapter:
    """Source adapter for Google BigQuery.

    Discovers all tables and views across datasets in a GCP project,
    captures schemas via INFORMATION_SCHEMA.COLUMNS, and retrieves
    query traffic from INFORMATION_SCHEMA.JOBS.
    """

    def __init__(self, project: str, credentials: Any | None = None) -> None:
        """Initialize the BigQuery adapter.

        Args:
            project: GCP project ID.
            credentials: Optional google.oauth2.credentials.Credentials instance.
                         If None, uses Application Default Credentials.
        """
        try:
            from google.cloud import bigquery  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError(
                "BigQuery support requires google-cloud-bigquery. Install with: pip install alma-connectors[bigquery]"
            ) from e

        self._project = project
        self._client = bigquery.Client(project=project, credentials=credentials)

    @property
    def source_id(self) -> str:
        return f"bigquery:{self._project}"

    @property
    def source_type(self) -> str:
        return "bigquery"

    def list_assets(self) -> list[dict]:
        """List all tables and views across all datasets in the project."""
        assets: list[dict] = []
        for dataset in self._client.list_datasets():
            for table_ref in self._client.list_tables(dataset.reference):
                assets.append(
                    {
                        "id": f"{self._project}.{dataset.dataset_id}.{table_ref.table_id}",
                        "name": table_ref.table_id,
                        "kind": table_ref.table_type.lower() if table_ref.table_type else "table",
                        "source": self.source_id,
                        "metadata": {
                            "project": self._project,
                            "dataset": dataset.dataset_id,
                        },
                    }
                )
        return assets

    def get_schema(self, asset_id: str) -> SchemaSnapshot | None:
        """Retrieve the schema for a given asset ID (project.dataset.table)."""
        try:
            table = self._client.get_table(asset_id)
        except Exception:
            return None

        columns = [
            ColumnDef(
                name=field.name,
                type=field.field_type,
                nullable=field.mode != "REQUIRED",
                description=field.description,
                mode=field.mode,
            )
            for field in table.schema
        ]
        return SchemaSnapshot(asset_id=asset_id, source_type=self.source_type, columns=columns)

    def get_traffic(self) -> TrafficObservationResult:
        """Retrieve recent query jobs from the project's JOBS history."""
        queries: list[QueryRecord] = []
        try:
            job_config = self._client.query(
                f"""
                SELECT query, user_email, creation_time, total_bytes_processed, total_slot_ms
                FROM `{self._project}`.`region-us`.INFORMATION_SCHEMA.JOBS
                WHERE job_type = 'QUERY'
                  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                ORDER BY creation_time DESC
                LIMIT 10000
                """
            )
            for row in job_config.result():
                queries.append(
                    QueryRecord(
                        sql=row.query,
                        source_type=self.source_type,
                        user=row.user_email,
                        timestamp=str(row.creation_time),
                        bytes_processed=row.total_bytes_processed,
                    )
                )
        except Exception as e:
            return TrafficObservationResult(source_id=self.source_id, source_type=self.source_type, error=str(e))

        return TrafficObservationResult(source_id=self.source_id, source_type=self.source_type, queries=queries)
