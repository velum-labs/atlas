"""BigQuery source adapter implementation."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import re
import time
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar
from uuid import NAMESPACE_URL, uuid5

from alma_connectors.source_adapter import (
    BigQueryAdapterConfig,
    ConnectionTestResult,
    ExternalSecretRef,
    ManagedSecret,
    ObservedQueryEvent,
    PersistedSourceAdapter,
    QueryResult,
    SchemaObjectKind,
    SchemaSnapshot,
    SetupInstructions,
    SourceAdapterCapabilities,
    SourceAdapterKind,
    SourceColumnSchema,
    SourceTableSchema,
    TrafficObservationResult,
)
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    CapabilityProbeResult,
    DefinitionSnapshot,
    DiscoveredContainer,
    DiscoverySnapshot,
    ExtractionMeta,
    ExtractionScope,
    ObjectDefinition,
    SchemaObject,
    SchemaSnapshotV2,
    ScopeContext,
    SourceAdapterKindV2,
    TrafficExtractionResult,
)
from alma_connectors.source_adapter_v2 import (
    ColumnSchema as V2ColumnSchema,
)
from alma_connectors.source_adapter_v2 import (
    SchemaObjectKind as V2SchemaObjectKind,
)
from alma_ports.sql_safety import quote_bq_identifier

logger = logging.getLogger(__name__)

# INFORMATION_SCHEMA.JOBS_BY_PROJECT retains at most 180 days of history.
_BQ_JOBS_RETENTION_DAYS = 180

# ---------------------------------------------------------------------------
# Input validation helpers
# ---------------------------------------------------------------------------

_BQ_PROJECT_ID_RE = re.compile(r"^[a-z][a-z0-9\-]{4,28}[a-z0-9]$")
_BQ_VALID_LOCATION_RE = re.compile(r"^[a-z][a-z0-9\-]{1,}$")


def _validate_bq_project_id(project_id: str) -> None:
    if not _BQ_PROJECT_ID_RE.fullmatch(project_id):
        raise ValueError(
            f"Invalid BigQuery project_id {project_id!r}: must match "
            r"'^[a-z][a-z0-9-]{4,28}[a-z0-9]$'"
        )


def _validate_bq_location(location: str) -> None:
    if not _BQ_VALID_LOCATION_RE.fullmatch(location):
        raise ValueError(
            f"Invalid BigQuery location {location!r}: must be lowercase alphanumeric with hyphens"
        )


def _map_bq_table_type(table_type: str) -> V2SchemaObjectKind:
    normalized = table_type.strip().upper()
    if normalized == "VIEW":
        return V2SchemaObjectKind.VIEW
    if normalized == "MATERIALIZED VIEW":
        return V2SchemaObjectKind.MATERIALIZED_VIEW
    if normalized in {"EXTERNAL", "EXTERNAL TABLE"}:
        return V2SchemaObjectKind.EXTERNAL_TABLE
    return V2SchemaObjectKind.TABLE


# ---------------------------------------------------------------------------
# Retry with exponential backoff
# ---------------------------------------------------------------------------

_T = TypeVar("_T")


async def _retry_with_backoff(  # noqa: UP047
    fn: Callable[[], _T],
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    retryable: Callable[[Exception], bool],
) -> _T:
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return await asyncio.to_thread(fn)
        except Exception as exc:
            if not retryable(exc):
                raise
            last_exc = exc
            if attempt < max_attempts - 1:
                delay = min(base_delay * (2 ** attempt), max_delay)
                logger.warning(
                    "Retryable BQ error (attempt %d/%d): %s. Retrying in %.1fs",
                    attempt + 1,
                    max_attempts,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
    if last_exc is None:
        raise RuntimeError("retry loop exited without a captured exception")
    raise last_exc


def _is_bq_retryable(exc: Exception) -> bool:
    with contextlib.suppress(ImportError):
        from google.api_core import exceptions as google_exceptions  # type: ignore[import-untyped]

        retryable_types = (
            google_exceptions.TooManyRequests,
            google_exceptions.ServiceUnavailable,
            google_exceptions.InternalServerError,
            google_exceptions.BadGateway,
            google_exceptions.GatewayTimeout,
        )
        if isinstance(exc, retryable_types):
            return True
    msg = str(exc)
    codes = ("429", "503", "rateLimitExceeded", "backendError", "serviceUnavailable")
    return any(c in msg for c in codes)


def _is_403(exc: Exception) -> bool:
    with contextlib.suppress(ImportError):
        from google.api_core import exceptions as google_exceptions  # type: ignore[import-untyped]

        if isinstance(exc, google_exceptions.Forbidden):
            return True
    msg = str(exc)
    return "403" in msg or "Access Denied" in msg or "Forbidden" in msg


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    with contextlib.suppress(TypeError, ValueError):
        return int(value)  # ty: ignore[invalid-argument-type]
    return None


def _fetch_columns_per_dataset(
    client: Any,
    project_id: str,
    bigquery: Any,
    config: BigQueryAdapterConfig,
    max_rows: int,
) -> list[dict[str, Any]]:
    """Query INFORMATION_SCHEMA.COLUMNS per dataset when the region-level query fails (403)."""
    datasets = list(client.list_datasets(project=project_id))
    all_rows: list[dict[str, Any]] = []
    remaining = max_rows
    for ds in datasets:
        dataset_id = _get_dataset_id(ds)
        if not dataset_id or remaining <= 0:
            break

        # TODO(@000alen): injection risk?
        per_ds_sql = f"""
            SELECT
              table_schema,
              table_name,
              column_name,
              data_type,
              is_nullable,
              ordinal_position,
              is_partitioning_column,
              clustering_ordinal_position
            FROM {quote_bq_identifier(project_id)}.{quote_bq_identifier(dataset_id)}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema <> 'INFORMATION_SCHEMA'
            ORDER BY table_name, ordinal_position
            LIMIT @max_rows
        """
        per_ds_config = _make_query_job_config(
            bigquery,
            config,
            query_parameters=[
                bigquery.ScalarQueryParameter("max_rows", "INT64", remaining),
            ],
        )
        try:
            batch = [_row_to_dict(row) for row in client.query(per_ds_sql, job_config=per_ds_config).result()]
            all_rows.extend(batch)
            remaining -= len(batch)
        except Exception as exc:
            logger.warning(
                "Per-dataset COLUMNS query failed for dataset=%s error=%s; skipping.",
                dataset_id,
                exc,
            )
    return all_rows


def _get_bigquery_module() -> Any:
    try:
        from google.cloud import bigquery  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError("google-cloud-bigquery is required for the BigQuery source adapter") from exc
    return bigquery


def _normalize_labels(raw_labels: Any) -> dict[str, str]:
    """Normalize BigQuery job labels into a plain string dict."""
    if raw_labels is None:
        return {}
    if isinstance(raw_labels, dict):
        return {str(k): str(v) for k, v in raw_labels.items()}
    if isinstance(raw_labels, list):
        labels: dict[str, str] = {}
        for entry in raw_labels:
            if not isinstance(entry, dict):
                continue
            key = entry.get("key")
            value = entry.get("value")
            if key is None or value is None:
                continue
            labels[str(key)] = str(value)
        return labels
    return {}


def _make_query_job_config(
    bigquery: Any,
    config: BigQueryAdapterConfig,
    *,
    query_parameters: list[Any] | None = None,
    dry_run: bool = False,
    timeout_ms: int | None = None,
) -> Any:
    job_config_args: dict[str, Any] = {}
    if query_parameters:
        job_config_args["query_parameters"] = query_parameters
    effective_timeout_ms = config.default_job_timeout_ms if timeout_ms is None else timeout_ms
    if effective_timeout_ms > 0:
        job_config_args["job_timeout_ms"] = effective_timeout_ms
    if config.maximum_bytes_billed is not None:
        job_config_args["maximum_bytes_billed"] = config.maximum_bytes_billed
    if dry_run:
        job_config_args["dry_run"] = True
    return bigquery.QueryJobConfig(**job_config_args)


def _extract_referenced_tables(raw_tables: Any) -> list[dict[str, str]]:
    """Extract structured table references from a JOBS_BY_PROJECT referenced_tables field."""
    if raw_tables is None:
        return []
    tables: list[dict[str, str]] = []
    for table in raw_tables:
        if isinstance(table, dict):
            project_id = str(table.get("project_id", ""))
            dataset_id = str(table.get("dataset_id", ""))
            table_id = str(table.get("table_id", ""))
        else:
            project_id = str(getattr(table, "project_id", ""))
            dataset_id = str(getattr(table, "dataset_id", ""))
            table_id = str(getattr(table, "table_id", ""))
        if not dataset_id or not table_id:
            continue
        tables.append({"project_id": project_id, "dataset_id": dataset_id, "table_id": table_id})
    return tables


def _build_consumer_identity(
    *,
    user_email: str | None,
    labels: dict[str, str],
    fallback_job_id: str,
) -> dict[str, Any]:
    """Infer consumer identity from BQ job labels and user email.

    Labels are the primary identification mechanism in BigQuery, analogous to
    application_name in PostgreSQL. Airflow labels (dag_id, task_id) yield the
    highest-confidence identities.
    """
    dag_id = labels.get("dag_id") or labels.get("airflow_dag")
    task_id = labels.get("task_id") or labels.get("airflow_task")
    source_type = "unknown"
    confidence = 0.4

    if dag_id:
        source_type = "airflow"
        # TODO(@000alen): magic number?
        confidence = 0.95
    elif user_email:
        source_type = "user"
        # TODO(@000alen): magic number?
        confidence = 0.7

    if dag_id and task_id:
        consumer_key = f"airflow:{dag_id}:{task_id}"
    elif dag_id:
        consumer_key = f"airflow:{dag_id}"
    elif user_email:
        consumer_key = f"user:{user_email}"
    else:
        consumer_key = f"job:{fallback_job_id}"

    return {
        "consumer_key": consumer_key,
        "source_type": source_type,
        "confidence": confidence,
        "dag_id": dag_id,
        "task_id": task_id,
    }


def _row_to_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "items"):
        return dict(row.items())
    raw = getattr(row, "__dict__", None)
    if isinstance(raw, dict):
        return dict(raw)
    return {}


def _normalize_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _cursor_creation_time(adapter: PersistedSourceAdapter) -> datetime | None:
    cursor = adapter.observation_cursor
    if not isinstance(cursor, dict):
        return None
    raw_cursor = cursor.get("bq_creation_time")
    if not isinstance(raw_cursor, str):
        return None
    try:
        parsed_cursor = datetime.fromisoformat(raw_cursor)
    except ValueError:
        return None
    return _normalize_timestamp(parsed_cursor)


def _compute_duration_ms(
    *,
    creation_time: datetime | None,
    end_time: datetime | None,
) -> float | None:
    if creation_time is None or end_time is None:
        return None
    return max(0.0, (end_time - creation_time).total_seconds() * 1000.0)


def _effective_since(since: datetime | None, lookback_hours: int) -> datetime:
    """Compute the effective high-water mark, clamped to the JOBS_BY_PROJECT retention window.

    When since is None (full scan), we fall back to lookback_hours from config.
    Either way we clamp to 180 days to avoid querying beyond the retention limit,
    which would return an empty result set without a useful error.
    """
    retention_boundary = datetime.now(tz=UTC) - timedelta(days=_BQ_JOBS_RETENTION_DAYS)
    if since is not None:
        return max(since, retention_boundary)
    return datetime.now(tz=UTC) - timedelta(hours=lookback_hours)


# Backtick-quoted identifiers: `a.b` or `a.b.c`
_BQ_BACKTICK_REF = re.compile(r"`([^`]+\.[^`]+)`")
# FROM/JOIN followed by an unquoted dotted identifier (2 or 3 parts)
_BQ_FROM_JOIN_REF = re.compile(
    r"(?:FROM|JOIN)\s+([a-zA-Z0-9_\-]+(?:\.[a-zA-Z0-9_]+){1,2})\b",
    re.IGNORECASE,
)


def _parse_dotted_ref(ref: str) -> dict[str, str] | None:
    """Parse a dotted table reference into project/dataset/table parts."""
    parts = ref.split(".")
    if len(parts) == 3:
        return {"project_id": parts[0], "dataset_id": parts[1], "table_id": parts[2]}
    if len(parts) == 2:
        return {"project_id": "", "dataset_id": parts[0], "table_id": parts[1]}
    return None


def _extract_tables_from_sql(sql: str) -> list[dict[str, str]]:
    """Fallback: extract table references from SQL text using regex.

    Used when JOBS_BY_PROJECT.referenced_tables is empty, which happens for
    failed queries, DDL statements, and some scripting constructs. Best-effort:
    covers backtick-quoted identifiers and bare FROM/JOIN clauses; will miss
    complex CTEs or dynamic SQL.
    """
    tables: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()

    def _add(ref: str) -> None:
        if "INFORMATION_SCHEMA" in ref.upper():
            return
        parsed = _parse_dotted_ref(ref)
        if parsed is None:
            return
        key = (
            parsed["project_id"].lower(),
            parsed["dataset_id"].lower(),
            parsed["table_id"].lower(),
        )
        if key not in seen:
            seen.add(key)
            tables.append(parsed)

    for m in _BQ_BACKTICK_REF.finditer(sql):
        _add(m.group(1))

    for m in _BQ_FROM_JOIN_REF.finditer(sql):
        # Skip if this position is already covered by a backtick match
        ref = m.group(1)
        if "`" not in sql[max(0, m.start() - 1) : m.end() + 1]:
            _add(ref)

    return tables


def _get_dataset_id(ds: Any) -> str:
    """Extract dataset_id string from a BQ DatasetListItem (or any object with .dataset_id)."""
    val = getattr(ds, "dataset_id", None)
    if isinstance(val, str):
        return val.strip()
    return ""


def _missing_permissions_from_exc(exc: Exception) -> tuple[str, ...]:
    """Infer missing BigQuery IAM permissions from an exception message."""
    msg = str(exc)
    lmsg = msg.lower()
    if "403" not in msg and "permissiondenied" not in lmsg and "permission" not in lmsg:
        return ()
    if "jobs" in lmsg:
        return ("bigquery.jobs.listAll",)
    if "routines" in lmsg:
        return ("bigquery.routines.list",)
    if "information_schema" in lmsg or "columns" in lmsg or "views" in lmsg:
        return ("bigquery.tables.getData",)
    if "dataset" in lmsg:
        return ("bigquery.datasets.list",)
    return ("bigquery.dataViewer",)


class BigQueryAdapter:
    """Runtime BigQuery source adapter."""

    kind = SourceAdapterKind.BIGQUERY
    capabilities = SourceAdapterCapabilities(
        can_test_connection=True,
        can_introspect_schema=True,
        can_observe_traffic=True,
        can_execute_query=True,
    )

    def __init__(
        self,
        *,
        resolve_secret: Callable[[ManagedSecret | ExternalSecretRef], str],
        client_factory: Callable[[str, str | None], Any] | None = None,
    ) -> None:
        self._resolve_secret = resolve_secret
        # client_factory(project_id, service_account_json_or_none) → BQ client
        # Injected in tests to avoid real GCP calls.
        self._client_factory = client_factory

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_config(self, adapter: PersistedSourceAdapter) -> BigQueryAdapterConfig:
        if not isinstance(adapter.config, BigQueryAdapterConfig):
            raise ValueError(f"adapter '{adapter.key}' is not configured as bigquery")
        return adapter.config

    def _get_client(
        self,
        project_id: str,
        service_account_json: str | None,
        *,
        location: str | None = None,
    ) -> Any:
        if self._client_factory is not None:
            return self._client_factory(project_id, service_account_json)
        bigquery = _get_bigquery_module()
        if service_account_json:
            from google.oauth2 import service_account  # type: ignore[import-untyped]

            info = json.loads(service_account_json)
            credentials = service_account.Credentials.from_service_account_info(
                info,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            return bigquery.Client(project=project_id, credentials=credentials, location=location)
        return bigquery.Client(project=project_id, location=location)

    def _credentials(self, adapter: PersistedSourceAdapter) -> tuple[str, str | None]:
        """Return (project_id, service_account_json)."""
        config = self._get_config(adapter)
        sa_json = (
            self._resolve_secret(config.service_account_secret)
            if config.service_account_secret is not None
            else None
        )
        return config.project_id, sa_json

    def _validate_config(self, config: BigQueryAdapterConfig) -> None:
        """Validate project_id and location format."""
        _validate_bq_project_id(config.project_id)
        _validate_bq_location(config.location)

    # ------------------------------------------------------------------
    # Async context manager support
    # ------------------------------------------------------------------

    async def __aenter__(self) -> BigQueryAdapter:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass  # BQ client is stateless (per-call); nothing to close

    # ------------------------------------------------------------------
    # Protocol methods
    # ------------------------------------------------------------------

    async def _validate_connection(self, adapter: PersistedSourceAdapter) -> ConnectionTestResult:
        """Verify connectivity and that the configured identity can list datasets and run queries."""
        config = self._get_config(adapter)  # raises ValueError for wrong kind — intentional
        self._validate_config(config)
        try:
            project_id, sa_json = self._credentials(adapter)
            client = self._get_client(project_id, sa_json, location=config.location)
            bigquery = _get_bigquery_module()

            # Verify bigquery.jobs.create permission via a trivial query
            probe = client.query(
                "SELECT 1 AS probe",
                job_config=_make_query_job_config(bigquery, config),
            )
            probe.result()

            # Verify bigquery.datasets.list permission
            datasets = list(client.list_datasets())
            dataset_count = len(datasets)

            return ConnectionTestResult(
                success=True,
                message=(f"Connected to project '{config.project_id}'. Found {dataset_count} dataset(s)."),
                resource_count=dataset_count,
                resource_label="datasets",
            )
        except Exception as exc:
            msg = str(exc)
            if "403" in msg or "PermissionDenied" in msg or "permission" in msg.lower():
                return ConnectionTestResult(
                    success=False,
                    message=(
                        f"Permission denied for adapter '{adapter.key}': {exc}."
                        " Ensure the configured identity has the BigQuery Job User and"
                        " BigQuery Metadata Viewer roles, or equivalent permissions."
                    ),
                )
            if "404" in msg or "Not Found" in msg or "not found" in msg.lower():
                return ConnectionTestResult(
                    success=False,
                    message=f"Project not found for adapter '{adapter.key}': {exc}.",
                )
            return ConnectionTestResult(
                success=False,
                message=f"Connection failed for adapter '{adapter.key}': {exc}.",
            )

    async def _build_schema_snapshot_data(self, adapter: PersistedSourceAdapter) -> SchemaSnapshot:
        """Build schema object data from INFORMATION_SCHEMA."""
        config = self._get_config(adapter)
        self._validate_config(config)
        project_id, sa_json = self._credentials(adapter)
        client = self._get_client(project_id, sa_json, location=config.location)
        bigquery = _get_bigquery_module()

        # --- COLUMNS query (includes BQ partition/clustering extensions) ---
        columns_sql = f"""
            SELECT
              table_schema,
              table_name,
              column_name,
              data_type,
              is_nullable,
              ordinal_position,
              is_partitioning_column,
              clustering_ordinal_position
            FROM {quote_bq_identifier(config.project_id)}.{quote_bq_identifier(f"region-{config.location}")}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema <> 'INFORMATION_SCHEMA'
            ORDER BY table_schema, table_name, ordinal_position
            LIMIT @max_rows
        """
        column_config = _make_query_job_config(
            bigquery,
            config,
            query_parameters=[
                bigquery.ScalarQueryParameter("max_rows", "INT64", config.max_column_rows),
            ],
        )
        try:
            query = client.query(columns_sql, job_config=column_config)
            rows = [_row_to_dict(row) for row in query.result()]
        except Exception as _col_exc:
            if _is_403(_col_exc):
                logger.debug(
                    "Region-level INFORMATION_SCHEMA.COLUMNS failed (%s). "
                    "Falling back to per-dataset queries.",
                    _col_exc,
                )
                rows = _fetch_columns_per_dataset(
                    client,
                    config.project_id,
                    bigquery,
                    config,
                    config.max_column_rows,
                )
            else:
                raise

        grouped: dict[tuple[str, str], list[SourceColumnSchema]] = defaultdict(list)
        # partition_col: first column with is_partitioning_column = 'YES'
        table_partition: dict[tuple[str, str], str] = {}
        # clustering: column name keyed by ordinal position (1-based), per table
        table_clustering: dict[tuple[str, str], dict[int, str]] = defaultdict(dict)

        for row in rows:
            schema_name = str(row.get("table_schema", "")).strip()
            table_name = str(row.get("table_name", "")).strip()
            column_name = str(row.get("column_name", "")).strip()
            data_type = str(row.get("data_type", "")).strip()
            if not schema_name or not table_name or not column_name or not data_type:
                continue

            key = (schema_name, table_name)
            grouped[key].append(
                SourceColumnSchema(
                    name=column_name,
                    data_type=data_type,
                    is_nullable=str(row.get("is_nullable", "YES")).upper() == "YES",
                )
            )

            if str(row.get("is_partitioning_column", "")).upper() == "YES":
                table_partition.setdefault(key, column_name)

            clustering_pos = row.get("clustering_ordinal_position")
            if clustering_pos is not None:
                with contextlib.suppress(TypeError, ValueError):
                    table_clustering[key][int(clustering_pos)] = column_name

        # --- TABLE_STORAGE query (row counts + logical bytes) ---
        storage_lookup: dict[tuple[str, str], dict[str, int]] = {}
        try:
            _region = f"{quote_bq_identifier(config.project_id)}.{quote_bq_identifier(f'region-{config.location}')}"
            storage_sql = f"""
                SELECT dataset_id, table_id, row_count, total_logical_bytes
                FROM {_region}.INFORMATION_SCHEMA.TABLE_STORAGE
                WHERE deleted = FALSE
            """
            storage_job = client.query(
                storage_sql,
                job_config=_make_query_job_config(bigquery, config),
            )
            for srow in storage_job.result():
                sdict = _row_to_dict(srow)
                ds = str(sdict.get("dataset_id", "")).strip()
                tbl = str(sdict.get("table_id", "")).strip()
                if not ds or not tbl:
                    continue
                rc = sdict.get("row_count")
                sb = sdict.get("total_logical_bytes")
                storage_lookup[(ds, tbl)] = {
                    "row_count": int(rc) if rc is not None else 0,
                    "size_bytes": int(sb) if sb is not None else 0,
                }
        except Exception as exc:
            logger.debug(
                "BigQueryAdapter._build_schema_snapshot_data: TABLE_STORAGE query failed, "
                "row_count/size_bytes will be omitted. adapter=%s error=%s",
                adapter.key,
                exc,
            )

        objects = tuple(
            SourceTableSchema(
                schema_name=schema_name,
                object_name=table_name,
                object_kind=SchemaObjectKind.TABLE,
                columns=tuple(cols),
                partition_column=table_partition.get((schema_name, table_name)),
                clustering_columns=tuple(
                    col for _, col in sorted(table_clustering.get((schema_name, table_name), {}).items())
                ),
                row_count=storage_lookup.get((schema_name, table_name), {}).get("row_count"),
                size_bytes=storage_lookup.get((schema_name, table_name), {}).get("size_bytes"),
            )
            for (schema_name, table_name), cols in grouped.items()
        )
        return SchemaSnapshot(captured_at=datetime.now(tz=UTC), objects=objects)

    async def _observe_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficObservationResult:
        """Observe query traffic from INFORMATION_SCHEMA.JOBS_BY_PROJECT."""
        config = self._get_config(adapter)
        self._validate_config(config)
        project_id, sa_json = self._credentials(adapter)
        client = self._get_client(project_id, sa_json, location=config.location)
        bigquery = _get_bigquery_module()
        jobs_view = f"{quote_bq_identifier(config.project_id)}.{quote_bq_identifier(f'region-{config.location}')}.INFORMATION_SCHEMA.JOBS_BY_PROJECT"

        cursor_creation_time = _cursor_creation_time(adapter)
        if cursor_creation_time is not None:
            high_water_mark = _effective_since(cursor_creation_time, config.lookback_hours)
            jobs_sql = f"""
                SELECT
                  job_id,
                  creation_time,
                  end_time,
                  user_email,
                  labels,
                  query,
                  referenced_tables,
                  total_bytes_processed,
                  total_slot_ms,
                  cache_hit
                FROM {jobs_view}
                WHERE creation_time > @cursor
                  AND state = 'DONE'
                  AND job_type = 'QUERY'
                  AND query IS NOT NULL
                ORDER BY creation_time ASC
                LIMIT @max_rows
            """
            query_parameters = [
                bigquery.ScalarQueryParameter("cursor", "TIMESTAMP", high_water_mark),
                bigquery.ScalarQueryParameter("max_rows", "INT64", config.max_job_rows),
            ]
        else:
            high_water_mark = _effective_since(since, config.lookback_hours)
            jobs_sql = f"""
                SELECT
                  job_id,
                  creation_time,
                  end_time,
                  user_email,
                  labels,
                  query,
                  referenced_tables,
                  total_bytes_processed,
                  total_slot_ms,
                  cache_hit
                FROM {jobs_view}
                WHERE creation_time >= @since
                  AND state = 'DONE'
                  AND job_type = 'QUERY'
                  AND query IS NOT NULL
                ORDER BY creation_time DESC
                LIMIT @max_rows
            """
            query_parameters = [
                bigquery.ScalarQueryParameter("since", "TIMESTAMP", high_water_mark),
                bigquery.ScalarQueryParameter("max_rows", "INT64", config.max_job_rows),
            ]

        job_config = _make_query_job_config(
            bigquery,
            config,
            query_parameters=query_parameters,
        )
        logger.debug(
            "BigQuery query started: capability=traffic adapter=%s", adapter.key
        )
        _t0 = time.perf_counter()
        try:
            rows = await _retry_with_backoff(
                lambda: [_row_to_dict(row) for row in client.query(jobs_sql, job_config=job_config).result()],
                retryable=_is_bq_retryable,
                max_attempts=3,
                base_delay=2.0,
            )
        except Exception as _jobs_exc:
            if _is_403(_jobs_exc):
                logger.warning(
                    "JOBS_BY_PROJECT query failed (%s). "
                    "Falling back to INFORMATION_SCHEMA.JOBS. adapter=%s",
                    _jobs_exc,
                    adapter.key,
                )
                jobs_view_fallback = (
                    f"{quote_bq_identifier(config.project_id)}"
                    f".{quote_bq_identifier(f'region-{config.location}')}"
                    f".INFORMATION_SCHEMA.JOBS"
                )
                jobs_sql_fallback = jobs_sql.replace(jobs_view, jobs_view_fallback)
                rows = await _retry_with_backoff(
                    lambda: [_row_to_dict(row) for row in client.query(jobs_sql_fallback, job_config=job_config).result()],
                    retryable=_is_bq_retryable,
                    max_attempts=3,
                    base_delay=2.0,
                )
            else:
                raise
        logger.debug(
            "BigQuery query finished: capability=traffic adapter=%s duration_ms=%.1f rows=%d",
            adapter.key,
            (time.perf_counter() - _t0) * 1000.0,
            len(rows),
        )

        events: list[ObservedQueryEvent] = []
        latest_creation_time: datetime | None = None
        for row in rows:
            creation_time = _normalize_timestamp(row.get("creation_time"))
            captured_at = creation_time or datetime.now(tz=UTC)
            if latest_creation_time is None or captured_at > latest_creation_time:
                latest_creation_time = captured_at

            sql = str(row.get("query", "")).strip()
            if not sql:
                continue

            job_id = str(row.get("job_id", "")).strip()
            event_id = str(uuid5(NAMESPACE_URL, f"{adapter.id}:{project_id}:{job_id}"))

            normalized_end = _normalize_timestamp(row.get("end_time"))
            labels = _normalize_labels(row.get("labels"))
            identity = _build_consumer_identity(
                user_email=row.get("user_email"),
                labels=labels,
                fallback_job_id=job_id,
            )
            referenced_tables = _extract_referenced_tables(row.get("referenced_tables"))
            # Fall back to SQL parsing when BQ did not populate referenced_tables
            # (happens for failed queries, DDL statements, and some scripted jobs).
            if not referenced_tables:
                referenced_tables = _extract_tables_from_sql(sql)
            cost_metadata: dict[str, object] = {}
            if config.include_job_cost_stats:
                total_bytes_processed = _int_or_none(row.get("total_bytes_processed"))
                total_slot_ms = _int_or_none(row.get("total_slot_ms"))
                cache_hit = row.get("cache_hit")
                if total_bytes_processed is not None:
                    cost_metadata["total_bytes_processed"] = total_bytes_processed
                if total_slot_ms is not None:
                    cost_metadata["total_slot_ms"] = total_slot_ms
                if cache_hit is not None:
                    cost_metadata["cache_hit"] = cache_hit

            events.append(
                ObservedQueryEvent(
                    captured_at=captured_at,
                    sql=sql,
                    source_name=identity["consumer_key"],
                    query_type="query_job",
                    event_id=event_id,
                    database_name=project_id,
                    database_user=str(row.get("user_email", "")).strip() or None,
                    statement_id=job_id or None,
                    duration_ms=_compute_duration_ms(
                        creation_time=captured_at,
                        end_time=normalized_end,
                    ),
                    metadata={
                        "adapter": "bigquery",
                        "bq_project": project_id,
                        "bq_location": config.location,
                        "job_id": job_id,
                        "labels": labels,
                        "consumer_source_type": identity["source_type"],
                        "identity_confidence": identity["confidence"],
                        "dag_id": identity["dag_id"],
                        "task_id": identity["task_id"],
                        "referenced_tables": referenced_tables,
                        **cost_metadata,
                    },
                    raw_payload=row,
                )
            )

        logger.info(
            "BigQueryAdapter.observe_traffic completed adapter=%s scanned=%d events=%d",
            adapter.key,
            len(rows),
            len(events),
        )
        observation_cursor: dict[str, object] | None = None
        if latest_creation_time is not None:
            observation_cursor = {"bq_creation_time": latest_creation_time.isoformat()}
        return TrafficObservationResult(
            scanned_records=len(rows),
            events=tuple(events),
            observation_cursor=observation_cursor,
        )

    async def execute_query(
        self,
        adapter: PersistedSourceAdapter,
        sql: str,
        *,
        max_rows: int | None = None,
        probe_target: str | None = None,
        dry_run: bool = False,
    ) -> QueryResult:
        """Execute a BigQuery SQL query.

        When dry_run=True the query is not executed; BQ returns the estimated bytes
        processed, which is useful for cost estimation before running expensive queries.
        """
        config = self._get_config(adapter)
        self._validate_config(config)
        project_id, sa_json = self._credentials(adapter)
        client = self._get_client(project_id, sa_json, location=config.location)
        bigquery = _get_bigquery_module()
        del probe_target

        row_limit = max_rows if max_rows and max_rows > 0 else 100
        started_at = time.perf_counter()
        try:
            job_config = _make_query_job_config(bigquery, config, dry_run=dry_run)
            query_job = client.query(sql, job_config=job_config)
            bytes_processed = _int_or_none(getattr(query_job, "total_bytes_processed", None))
            bytes_billed = _int_or_none(getattr(query_job, "total_bytes_billed", None))

            if dry_run:
                duration_ms = (time.perf_counter() - started_at) * 1000.0
                content_hash = hashlib.sha256(sql.encode()).hexdigest()[:32]
                return QueryResult(
                    success=True,
                    row_count=0,
                    duration_ms=duration_ms,
                    content_hash=content_hash,
                    bytes_processed=bytes_processed,
                    bytes_billed=bytes_billed,
                )

            rows: list[dict[str, object]] = []
            truncated = False
            for i, row in enumerate(query_job.result()):
                if i >= row_limit:
                    truncated = True
                    break
                rows.append(_row_to_dict(row))

            duration_ms = (time.perf_counter() - started_at) * 1000.0
            return QueryResult(
                success=True,
                row_count=len(rows),
                duration_ms=duration_ms,
                rows=tuple(rows),
                truncated=truncated,
                bytes_processed=bytes_processed,
                bytes_billed=bytes_billed,
            )
        except Exception as exc:
            duration_ms = (time.perf_counter() - started_at) * 1000.0
            return QueryResult(
                success=False,
                row_count=0,
                duration_ms=duration_ms,
                error_message=str(exc),
            )

    # ------------------------------------------------------------------
    # SourceAdapterV2 protocol
    # ------------------------------------------------------------------

    @property
    def declared_capabilities(self) -> frozenset[AdapterCapability]:
        return frozenset({
            AdapterCapability.DISCOVER,
            AdapterCapability.SCHEMA,
            AdapterCapability.DEFINITIONS,
            AdapterCapability.TRAFFIC,
        })

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        """Probe which capabilities are actually available.

        - DISCOVER: lightweight datasets.list call
        - SCHEMA: SELECT 1 from INFORMATION_SCHEMA.COLUMNS
        - TRAFFIC: SELECT 1 from INFORMATION_SCHEMA.JOBS_BY_PROJECT
        """
        caps = capabilities if capabilities is not None else self.declared_capabilities
        config = self._get_config(adapter)
        self._validate_config(config)
        project_id, sa_json = self._credentials(adapter)
        client = self._get_client(project_id, sa_json, location=config.location)
        bigquery = _get_bigquery_module()

        scope_ctx = ScopeContext(
            scope=ExtractionScope.REGION,
            identifiers={"project": project_id, "location": config.location},
        )
        results: list[CapabilityProbeResult] = []

        if AdapterCapability.DISCOVER in caps:
            try:
                list(client.list_datasets())
                results.append(CapabilityProbeResult(
                    capability=AdapterCapability.DISCOVER,
                    available=True,
                    scope=ExtractionScope.REGION,
                    scope_context=scope_ctx,
                ))
            except Exception as exc:
                results.append(CapabilityProbeResult(
                    capability=AdapterCapability.DISCOVER,
                    available=False,
                    scope=ExtractionScope.REGION,
                    scope_context=scope_ctx,
                    message=str(exc),
                    permissions_missing=_missing_permissions_from_exc(exc),
                ))

        if AdapterCapability.SCHEMA in caps:
            probe_sql = (
                f"SELECT 1 FROM {quote_bq_identifier(project_id)}"
                f".{quote_bq_identifier(f'region-{config.location}')}"
                f".INFORMATION_SCHEMA.COLUMNS LIMIT 1"
            )
            try:
                client.query(probe_sql, job_config=_make_query_job_config(bigquery, config)).result()
                results.append(CapabilityProbeResult(
                    capability=AdapterCapability.SCHEMA,
                    available=True,
                    scope=ExtractionScope.REGION,
                    scope_context=scope_ctx,
                ))
            except Exception as exc:
                if _is_403(exc):
                    # Try per-dataset fallback: if any dataset is queryable, SCHEMA works at dataset scope
                    logger.debug(
                        "Region-level INFORMATION_SCHEMA.COLUMNS probe failed (%s). "
                        "Probing per-dataset scope.",
                        exc,
                    )
                    _ds_available = False
                    _ds_exc: Exception | None = exc
                    try:
                        _datasets = list(client.list_datasets(project=project_id))
                        for _ds in _datasets:
                            _ds_id = _get_dataset_id(_ds)
                            if not _ds_id:
                                continue
                            _ds_probe_sql = (
                                f"SELECT 1 FROM {quote_bq_identifier(project_id)}"
                                f".{quote_bq_identifier(_ds_id)}"
                                f".INFORMATION_SCHEMA.COLUMNS LIMIT 1"
                            )
                            try:
                                client.query(
                                    _ds_probe_sql,
                                    job_config=_make_query_job_config(bigquery, config),
                                ).result()
                                _ds_available = True
                                break
                            except Exception:
                                continue
                    except Exception as _list_exc:
                        _ds_exc = _list_exc
                    results.append(CapabilityProbeResult(
                        capability=AdapterCapability.SCHEMA,
                        available=_ds_available,
                        scope=ExtractionScope.REGION,
                        scope_context=scope_ctx,
                        message=None if _ds_available else str(_ds_exc),
                        permissions_missing=() if _ds_available else _missing_permissions_from_exc(exc),
                    ))
                else:
                    results.append(CapabilityProbeResult(
                        capability=AdapterCapability.SCHEMA,
                        available=False,
                        scope=ExtractionScope.REGION,
                        scope_context=scope_ctx,
                        message=str(exc),
                        permissions_missing=_missing_permissions_from_exc(exc),
                    ))

        if AdapterCapability.TRAFFIC in caps:
            jobs_view = (
                f"{quote_bq_identifier(project_id)}"
                f".{quote_bq_identifier(f'region-{config.location}')}"
                f".INFORMATION_SCHEMA.JOBS_BY_PROJECT"
            )
            probe_sql = f"SELECT 1 FROM {jobs_view} LIMIT 1"
            try:
                client.query(probe_sql, job_config=_make_query_job_config(bigquery, config)).result()
                results.append(CapabilityProbeResult(
                    capability=AdapterCapability.TRAFFIC,
                    available=True,
                    scope=ExtractionScope.REGION,
                    scope_context=scope_ctx,
                ))
            except Exception as exc:
                results.append(CapabilityProbeResult(
                    capability=AdapterCapability.TRAFFIC,
                    available=False,
                    scope=ExtractionScope.REGION,
                    scope_context=scope_ctx,
                    message=str(exc),
                    permissions_missing=_missing_permissions_from_exc(exc),
                ))

        if AdapterCapability.DEFINITIONS in caps:
            _region = f"{quote_bq_identifier(project_id)}.{quote_bq_identifier(f'region-{config.location}')}"
            probe_errors: list[str] = []
            missing_perms: list[str] = []
            for probe_sql in (
                f"SELECT 1 FROM {_region}.INFORMATION_SCHEMA.ROUTINES LIMIT 1",
                f"SELECT 1 FROM {_region}.INFORMATION_SCHEMA.VIEWS LIMIT 1",
            ):
                try:
                    client.query(probe_sql, job_config=_make_query_job_config(bigquery, config)).result()
                except Exception as exc:
                    probe_errors.append(str(exc))
                    missing_perms.extend(_missing_permissions_from_exc(exc))
            results.append(CapabilityProbeResult(
                capability=AdapterCapability.DEFINITIONS,
                available=len(probe_errors) == 0,
                scope=ExtractionScope.REGION,
                scope_context=scope_ctx,
                message="; ".join(probe_errors) if probe_errors else None,
                permissions_missing=tuple(dict.fromkeys(missing_perms)),
            ))

        return tuple(results)

    async def discover(self, adapter: PersistedSourceAdapter) -> DiscoverySnapshot:
        """DISCOVER: list all datasets in the project as DiscoveredContainers."""
        config = self._get_config(adapter)
        self._validate_config(config)
        project_id, sa_json = self._credentials(adapter)
        client = self._get_client(project_id, sa_json, location=config.location)
        logger.debug(
            "BigQuery connection established: project=%s location=%s adapter=%s",
            project_id,
            config.location,
            adapter.key,
        )

        started_at = time.perf_counter()
        captured_at = datetime.now(tz=UTC)

        datasets = list(client.list_datasets())
        containers = tuple(
            DiscoveredContainer(
                container_id=f"{project_id}.{_get_dataset_id(ds)}",
                container_type="dataset",
                display_name=_get_dataset_id(ds),
                location=config.location,
                metadata={"project_id": project_id},
            )
            for ds in datasets
            if _get_dataset_id(ds)
        )

        duration_ms = (time.perf_counter() - started_at) * 1000.0
        scope_ctx = ScopeContext(
            scope=ExtractionScope.REGION,
            identifiers={"project": project_id, "location": config.location},
        )
        meta = ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.BIGQUERY,
            capability=AdapterCapability.DISCOVER,
            scope_context=scope_ctx,
            captured_at=captured_at,
            duration_ms=duration_ms,
            row_count=len(containers),
        )
        return DiscoverySnapshot(meta=meta, containers=containers)

    async def extract_schema(self, adapter: PersistedSourceAdapter) -> SchemaSnapshotV2:
        """SCHEMA: tables/views with freshness + descriptions, routines, and ML models.

        Queries (in order, with independent graceful fallback for optional views):
          1. INFORMATION_SCHEMA.COLUMNS      — columns for all tables/views
          2. INFORMATION_SCHEMA.TABLE_STORAGE — row_count, size_bytes, last_modified_time
          3. INFORMATION_SCHEMA.TABLE_OPTIONS — table descriptions
          4. INFORMATION_SCHEMA.PARAMETERS   — routine parameters (best-effort)
          5. INFORMATION_SCHEMA.ROUTINES     — UDFs, procedures, table functions
          6. INFORMATION_SCHEMA.MODELS       — BigQuery ML models
        """
        started_at = time.perf_counter()
        captured_at = datetime.now(tz=UTC)

        config = self._get_config(adapter)
        self._validate_config(config)
        project_id, sa_json = self._credentials(adapter)
        client = self._get_client(project_id, sa_json, location=config.location)
        bigquery = _get_bigquery_module()
        region_prefix = f"{quote_bq_identifier(project_id)}.{quote_bq_identifier(f'region-{config.location}')}"

        # ------------------------------------------------------------------
        # 1. COLUMNS
        # ------------------------------------------------------------------
        columns_sql = f"""
            SELECT
              table_schema,
              table_name,
              column_name,
              data_type,
              is_nullable,
              ordinal_position,
              is_partitioning_column,
              clustering_ordinal_position
            FROM {region_prefix}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema <> 'INFORMATION_SCHEMA'
            ORDER BY table_schema, table_name, ordinal_position
            LIMIT @max_rows
        """
        column_config = _make_query_job_config(
            bigquery,
            config,
            query_parameters=[
                bigquery.ScalarQueryParameter("max_rows", "INT64", config.max_column_rows),
            ],
        )
        logger.debug(
            "BigQuery query started: capability=schema adapter=%s", adapter.key
        )
        _t0_col = time.perf_counter()
        try:
            col_rows = await _retry_with_backoff(
                lambda: [_row_to_dict(r) for r in client.query(columns_sql, job_config=column_config).result()],
                retryable=_is_bq_retryable,
                max_attempts=3,
                base_delay=2.0,
            )
        except Exception as _col_exc:
            if _is_403(_col_exc):
                logger.debug(
                    "Region-level INFORMATION_SCHEMA.COLUMNS failed (%s). "
                    "Falling back to per-dataset queries. adapter=%s",
                    _col_exc,
                    adapter.key,
                )
                col_rows = _fetch_columns_per_dataset(
                    client,
                    project_id,
                    bigquery,
                    config,
                    config.max_column_rows,
                )
            else:
                logger.error(
                    "BigQuery query failed: capability=schema adapter=%s error=%s",
                    adapter.key,
                    _col_exc,
                )
                raise
        logger.debug(
            "BigQuery query finished: capability=schema adapter=%s duration_ms=%.1f rows=%d",
            adapter.key,
            (time.perf_counter() - _t0_col) * 1000.0,
            len(col_rows),
        )

        grouped: dict[tuple[str, str], list[V2ColumnSchema]] = defaultdict(list)
        table_partition: dict[tuple[str, str], str] = {}
        table_clustering: dict[tuple[str, str], dict[int, str]] = defaultdict(dict)

        for row in col_rows:
            schema_name = str(row.get("table_schema", "")).strip()
            table_name = str(row.get("table_name", "")).strip()
            column_name = str(row.get("column_name", "")).strip()
            data_type = str(row.get("data_type", "")).strip()
            if not schema_name or not table_name or not column_name or not data_type:
                continue

            key = (schema_name, table_name)
            grouped[key].append(
                V2ColumnSchema(
                    name=column_name,
                    data_type=data_type,
                    is_nullable=str(row.get("is_nullable", "YES")).upper() == "YES",
                )
            )
            if str(row.get("is_partitioning_column", "")).upper() == "YES":
                table_partition.setdefault(key, column_name)
            clustering_pos = row.get("clustering_ordinal_position")
            if clustering_pos is not None:
                with contextlib.suppress(TypeError, ValueError):
                    table_clustering[key][int(clustering_pos)] = column_name

        # ------------------------------------------------------------------
        # 2. TABLE_STORAGE (row counts, logical bytes, last_modified_time)
        # ------------------------------------------------------------------
        storage_lookup: dict[tuple[str, str], dict[str, Any]] = {}
        try:
            storage_sql = f"""
                SELECT dataset_id, table_id, row_count, total_logical_bytes, last_modified_time
                FROM {region_prefix}.INFORMATION_SCHEMA.TABLE_STORAGE
                WHERE deleted = FALSE
            """
            for srow in client.query(
                storage_sql,
                job_config=_make_query_job_config(bigquery, config),
            ).result():
                sdict = _row_to_dict(srow)
                ds = str(sdict.get("dataset_id", "")).strip()
                tbl = str(sdict.get("table_id", "")).strip()
                if not ds or not tbl:
                    continue
                rc = sdict.get("row_count")
                sb = sdict.get("total_logical_bytes")
                storage_lookup[(ds, tbl)] = {
                    "row_count": int(rc) if rc is not None else 0,
                    "size_bytes": int(sb) if sb is not None else 0,
                    "last_modified": _normalize_timestamp(sdict.get("last_modified_time")),
                }
        except Exception as exc:
            logger.debug(
                "BigQueryAdapter.extract_schema: TABLE_STORAGE query failed, "
                "row_count/size_bytes/last_modified will be omitted. adapter=%s error=%s",
                adapter.key,
                exc,
            )

        # ------------------------------------------------------------------
        # 3. TABLES (object kinds)
        # ------------------------------------------------------------------
        table_kind_lookup: dict[tuple[str, str], V2SchemaObjectKind] = {}
        try:
            tables_sql = f"""
                SELECT table_schema, table_name, table_type
                FROM {region_prefix}.INFORMATION_SCHEMA.TABLES
                WHERE table_schema <> 'INFORMATION_SCHEMA'
            """
            for row in client.query(
                tables_sql,
                job_config=_make_query_job_config(bigquery, config),
            ).result():
                rdict = _row_to_dict(row)
                ds = str(rdict.get("table_schema", "")).strip()
                tbl = str(rdict.get("table_name", "")).strip()
                table_type = str(rdict.get("table_type", "")).strip()
                if ds and tbl and table_type:
                    table_kind_lookup[(ds, tbl)] = _map_bq_table_type(table_type)
        except Exception as exc:
            logger.warning(
                "BigQueryAdapter.extract_schema: TABLES query failed, "
                "table kinds will default to TABLE. adapter=%s error=%s",
                adapter.key,
                exc,
            )

        # ------------------------------------------------------------------
        # 4. TABLE_OPTIONS (descriptions)
        # ------------------------------------------------------------------
        description_lookup: dict[tuple[str, str], str] = {}
        try:
            opts_sql = f"""
                SELECT table_schema, table_name, option_value
                FROM {region_prefix}.INFORMATION_SCHEMA.TABLE_OPTIONS
                WHERE option_name = 'description'
            """
            for row in client.query(
                opts_sql,
                job_config=_make_query_job_config(bigquery, config),
            ).result():
                rdict = _row_to_dict(row)
                ds = str(rdict.get("table_schema", "")).strip()
                tbl = str(rdict.get("table_name", "")).strip()
                val = str(rdict.get("option_value", "")).strip().strip('"')
                if ds and tbl and val:
                    description_lookup[(ds, tbl)] = val
        except Exception as exc:
            logger.warning(
                "BigQueryAdapter.extract_schema: TABLE_OPTIONS query failed, "
                "table descriptions will be omitted. adapter=%s error=%s",
                adapter.key,
                exc,
            )

        # ------------------------------------------------------------------
        # Build table/view SchemaObjects
        # ------------------------------------------------------------------
        objects_list: list[SchemaObject] = []
        for (schema_name, table_name), cols in grouped.items():
            key = (schema_name, table_name)
            st = storage_lookup.get(key, {})
            objects_list.append(
                SchemaObject(
                    schema_name=schema_name,
                    object_name=table_name,
                    kind=table_kind_lookup.get(key, V2SchemaObjectKind.TABLE),
                    columns=tuple(cols),
                    row_count=st.get("row_count"),
                    size_bytes=st.get("size_bytes"),
                    last_modified=st.get("last_modified"),
                    description=description_lookup.get(key),
                    partition_column=table_partition.get(key),
                    clustering_columns=tuple(
                        col for _, col in sorted(table_clustering.get(key, {}).items())
                    ),
                )
            )

        # ------------------------------------------------------------------
        # 4. PARAMETERS (routine parameters — best-effort, non-fatal)
        # ------------------------------------------------------------------
        routine_params: dict[tuple[str, str], list[V2ColumnSchema]] = defaultdict(list)
        try:
            params_sql = f"""
                SELECT specific_schema, specific_name, parameter_name, data_type, ordinal_position
                FROM {region_prefix}.INFORMATION_SCHEMA.PARAMETERS
                ORDER BY specific_schema, specific_name, ordinal_position
            """
            for row in client.query(
                params_sql,
                job_config=_make_query_job_config(bigquery, config),
            ).result():
                rdict = _row_to_dict(row)
                schema = str(rdict.get("specific_schema", "")).strip()
                name = str(rdict.get("specific_name", "")).strip()
                param_name = str(rdict.get("parameter_name", "")).strip()
                data_type = str(rdict.get("data_type", "")).strip() or "ANY TYPE"
                if schema and name and param_name:
                    routine_params[(schema, name)].append(
                        V2ColumnSchema(name=param_name, data_type=data_type)
                    )
        except Exception as exc:
            logger.warning(
                "BigQueryAdapter.extract_schema: PARAMETERS query failed, "
                "routine parameters will be omitted. adapter=%s error=%s",
                adapter.key,
                exc,
            )

        # ------------------------------------------------------------------
        # 5. ROUTINES (UDFs, procedures, table functions)
        # ------------------------------------------------------------------
        try:
            routines_sql = f"""
                SELECT
                  routine_schema,
                  routine_name,
                  routine_type,
                  data_type,
                  routine_definition,
                  routine_body,
                  external_language
                FROM {region_prefix}.INFORMATION_SCHEMA.ROUTINES
            """
            for row in client.query(
                routines_sql,
                job_config=_make_query_job_config(bigquery, config),
            ).result():
                rdict = _row_to_dict(row)
                schema = str(rdict.get("routine_schema", "")).strip()
                name = str(rdict.get("routine_name", "")).strip()
                if not schema or not name:
                    continue
                routine_type = str(rdict.get("routine_type", "")).strip().upper()
                if "PROCEDURE" in routine_type:
                    kind = V2SchemaObjectKind.PROCEDURE
                elif "TABLE" in routine_type:
                    kind = V2SchemaObjectKind.TABLE_FUNCTION
                else:
                    kind = V2SchemaObjectKind.UDF
                body_type = str(rdict.get("routine_body", "")).strip().upper()
                ext_lang = str(rdict.get("external_language", "")).strip()
                language = ext_lang if (body_type == "EXTERNAL" and ext_lang) else (body_type or None)
                return_type = str(rdict.get("data_type", "")).strip() or None
                definition_body = str(rdict.get("routine_definition", "")).strip() or None
                objects_list.append(
                    SchemaObject(
                        schema_name=schema,
                        object_name=name,
                        kind=kind,
                        columns=tuple(routine_params.get((schema, name), [])),
                        language=language,
                        return_type=return_type,
                        definition_body=definition_body,
                    )
                )
        except Exception as exc:
            logger.warning(
                "BigQueryAdapter.extract_schema: ROUTINES query failed, "
                "UDFs/procedures will be omitted. adapter=%s error=%s",
                adapter.key,
                exc,
            )

        # ------------------------------------------------------------------
        # 6. MODELS (BigQuery ML)
        # ------------------------------------------------------------------
        try:
            models_sql = f"""
                SELECT model_schema, model_name, model_type, last_modified_time
                FROM {region_prefix}.INFORMATION_SCHEMA.MODELS
            """
            for row in client.query(
                models_sql,
                job_config=_make_query_job_config(bigquery, config),
            ).result():
                rdict = _row_to_dict(row)
                schema = str(rdict.get("model_schema", "")).strip()
                name = str(rdict.get("model_name", "")).strip()
                if not schema or not name:
                    continue
                model_type = str(rdict.get("model_type", "")).strip() or None
                last_mod = _normalize_timestamp(rdict.get("last_modified_time"))
                objects_list.append(
                    SchemaObject(
                        schema_name=schema,
                        object_name=name,
                        kind=V2SchemaObjectKind.ML_MODEL,
                        model_type=model_type,
                        last_modified=last_mod,
                    )
                )
        except Exception as exc:
            logger.warning(
                "BigQueryAdapter.extract_schema: MODELS query failed, "
                "ML models will be omitted. adapter=%s error=%s",
                adapter.key,
                exc,
            )

        objects = tuple(objects_list)
        duration_ms = (time.perf_counter() - started_at) * 1000.0
        scope_ctx = ScopeContext(
            scope=ExtractionScope.REGION,
            identifiers={"project": project_id, "location": config.location},
        )
        meta = ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.BIGQUERY,
            capability=AdapterCapability.SCHEMA,
            scope_context=scope_ctx,
            captured_at=captured_at,
            duration_ms=duration_ms,
            row_count=len(objects),
        )
        return SchemaSnapshotV2(meta=meta, objects=objects)

    async def extract_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficExtractionResult:
        """TRAFFIC: wrap observe_traffic() output into v2 TrafficExtractionResult."""
        started_at = time.perf_counter()
        captured_at = datetime.now(tz=UTC)

        v1_result = await self._observe_traffic(adapter, since=since)

        config = self._get_config(adapter)
        project_id, _ = self._credentials(adapter)
        duration_ms = (time.perf_counter() - started_at) * 1000.0
        scope_ctx = ScopeContext(
            scope=ExtractionScope.REGION,
            identifiers={"project": project_id, "location": config.location},
        )
        meta = ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.BIGQUERY,
            capability=AdapterCapability.TRAFFIC,
            scope_context=scope_ctx,
            captured_at=captured_at,
            duration_ms=duration_ms,
            row_count=len(v1_result.events),
        )
        return TrafficExtractionResult(
            meta=meta,
            events=v1_result.events,
            observation_cursor=v1_result.observation_cursor,
        )

    async def extract_definitions(self, adapter: PersistedSourceAdapter) -> DefinitionSnapshot:
        """DEFINITIONS: extract view SQL, routine DDL, and table DDL from INFORMATION_SCHEMA."""
        config = self._get_config(adapter)
        self._validate_config(config)
        project_id, sa_json = self._credentials(adapter)
        client = self._get_client(project_id, sa_json, location=config.location)
        bigquery = _get_bigquery_module()

        started_at = time.perf_counter()
        captured_at = datetime.now(tz=UTC)
        region = f"{quote_bq_identifier(project_id)}.{quote_bq_identifier(f'region-{config.location}')}"
        definitions: list[ObjectDefinition] = []

        # --- Views ---
        views_sql = f"""
            SELECT table_schema, table_name, view_definition
            FROM {region}.INFORMATION_SCHEMA.VIEWS
            WHERE table_schema <> 'INFORMATION_SCHEMA'
        """
        try:
            view_job = client.query(
                views_sql,
                job_config=_make_query_job_config(bigquery, config),
            )
            for row in view_job.result():
                d = _row_to_dict(row)
                schema_name = str(d.get("table_schema", "")).strip()
                object_name = str(d.get("table_name", "")).strip()
                view_def = d.get("view_definition")
                if not schema_name or not object_name or view_def is None:
                    continue
                definition_text = str(view_def).strip()
                if not definition_text:
                    continue
                definitions.append(ObjectDefinition(
                    schema_name=schema_name,
                    object_name=object_name,
                    object_kind=V2SchemaObjectKind.VIEW,
                    definition_text=definition_text,
                    definition_language="sql",
                ))
        except Exception as exc:
            logger.warning(
                "BigQueryAdapter.extract_definitions: VIEWS query failed. adapter=%s error=%s",
                adapter.key,
                exc,
            )

        # --- Routines ---
        routines_sql = f"""
            SELECT routine_schema, routine_name, routine_type, routine_definition, external_language
            FROM {region}.INFORMATION_SCHEMA.ROUTINES
            WHERE routine_schema <> 'INFORMATION_SCHEMA'
        """
        try:
            routine_job = client.query(
                routines_sql,
                job_config=_make_query_job_config(bigquery, config),
            )
            for row in routine_job.result():
                d = _row_to_dict(row)
                schema_name = str(d.get("routine_schema", "")).strip()
                object_name = str(d.get("routine_name", "")).strip()
                routine_def = d.get("routine_definition")
                if not schema_name or not object_name or routine_def is None:
                    continue
                definition_text = str(routine_def).strip()
                if not definition_text:
                    continue
                routine_type = str(d.get("routine_type", "")).upper()
                ext_lang = d.get("external_language")
                if routine_type == "PROCEDURE":
                    kind = V2SchemaObjectKind.PROCEDURE
                elif routine_type == "TABLE FUNCTION":
                    kind = V2SchemaObjectKind.TABLE_FUNCTION
                else:
                    kind = V2SchemaObjectKind.UDF
                definition_language = str(ext_lang).lower() if ext_lang else "sql"
                definitions.append(ObjectDefinition(
                    schema_name=schema_name,
                    object_name=object_name,
                    object_kind=kind,
                    definition_text=definition_text,
                    definition_language=definition_language,
                    metadata={"routine_type": routine_type},
                ))
        except Exception as exc:
            logger.warning(
                "BigQueryAdapter.extract_definitions: ROUTINES query failed. adapter=%s error=%s",
                adapter.key,
                exc,
            )

        # --- Tables (DDL) ---
        tables_sql = f"""
            SELECT table_schema, table_name, ddl
            FROM {region}.INFORMATION_SCHEMA.TABLES
            WHERE table_type = 'BASE TABLE'
              AND table_schema <> 'INFORMATION_SCHEMA'
        """
        try:
            table_job = client.query(
                tables_sql,
                job_config=_make_query_job_config(bigquery, config),
            )
            for row in table_job.result():
                d = _row_to_dict(row)
                schema_name = str(d.get("table_schema", "")).strip()
                object_name = str(d.get("table_name", "")).strip()
                ddl = d.get("ddl")
                if not schema_name or not object_name or ddl is None:
                    continue
                definition_text = str(ddl).strip()
                if not definition_text:
                    continue
                definitions.append(ObjectDefinition(
                    schema_name=schema_name,
                    object_name=object_name,
                    object_kind=V2SchemaObjectKind.TABLE,
                    definition_text=definition_text,
                    definition_language="ddl",
                ))
        except Exception as exc:
            logger.warning(
                "BigQueryAdapter.extract_definitions: TABLES DDL query failed. adapter=%s error=%s",
                adapter.key,
                exc,
            )

        scope_ctx = ScopeContext(
            scope=ExtractionScope.REGION,
            identifiers={"project": project_id, "location": config.location},
        )
        duration_ms = (time.perf_counter() - started_at) * 1000.0
        meta = ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.BIGQUERY,
            capability=AdapterCapability.DEFINITIONS,
            scope_context=scope_ctx,
            captured_at=captured_at,
            duration_ms=duration_ms,
            row_count=len(definitions),
        )
        return DefinitionSnapshot(meta=meta, definitions=tuple(definitions))

    async def extract_lineage(self, adapter: PersistedSourceAdapter) -> None:  # type: ignore[override]
        raise NotImplementedError(
            "BigQueryAdapter does not support LINEAGE extraction. "
            "Lineage is inferred from TRAFFIC events by the scanner as a post-processing step."
        )

    async def extract_orchestration(self, adapter: PersistedSourceAdapter) -> None:  # type: ignore[override]
        raise NotImplementedError(
            "BigQueryAdapter does not support ORCHESTRATION extraction. "
            "BigQuery has no native orchestration; use AirflowAdapter for DAG metadata."
        )

    def get_setup_instructions(self) -> SetupInstructions:
        """Return operator guidance for enabling a BigQuery source adapter."""
        return SetupInstructions(
            title="BigQuery Source Adapter",
            summary=(
                "Register a BigQuery project using Application Default Credentials"
                " (recommended) or an explicit service account JSON key."
                " The configured identity requires permissions to list datasets,"
                " list jobs, and read INFORMATION_SCHEMA views."
            ),
            steps=(
                "For local development, run 'gcloud auth application-default login'"
                " to configure Application Default Credentials.",
                "If you need explicit non-user auth, create a Google Cloud service"
                " account in the target GCP project.",
                "Grant the configured identity the 'BigQuery Job User' role"
                " (roles/bigquery.jobUser) for bigquery.jobs.create and"
                " bigquery.jobs.list, and the 'BigQuery Metadata Viewer' role"
                " (roles/bigquery.metadataViewer) for INFORMATION_SCHEMA access.",
                "If using explicit credentials, download the service account JSON key"
                " and store it as a managed secret or an environment-variable"
                " reference (provider='env').",
                "Set project_id to the GCP project ID and location to the BigQuery"
                " region matching your datasets (e.g. 'us', 'eu', 'us-central1')."
                " INFORMATION_SCHEMA is region-scoped; mismatched location returns"
                " no rows.",
                "INFORMATION_SCHEMA.JOBS_BY_PROJECT retains up to 180 days of history."
                " Set lookback_hours to control the observation window (default: 24).",
            ),
            docs_url="https://cloud.google.com/bigquery/docs/information-schema-jobs",
        )
