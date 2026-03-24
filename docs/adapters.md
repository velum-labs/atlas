# Adapters

Atlas adapters connect to data sources to extract assets, schemas, and query traffic. Each adapter implements a standard interface — but the data available and the prerequisites differ.

## Capability summary

| Capability | BigQuery | PostgreSQL | dbt |
|-----------|----------|------------|-----|
| Schema introspection | Yes | Yes | Yes |
| Query traffic | Yes | Yes | No |
| Lineage extraction | Yes | Yes | Yes |
| Query execution | Yes | Yes | No |

---

## BigQuery

### Prerequisites

A GCP service account (or ADC identity) with the following IAM roles on the target project:

- `roles/bigquery.jobUser` — run INFORMATION_SCHEMA queries and traffic analysis
- `roles/bigquery.metadataViewer` — read table and column metadata

Optionally, for query execution via Atlas:

- `roles/bigquery.dataViewer` — read table data

### Connect

```bash
# Application Default Credentials (recommended for local use)
gcloud auth application-default login
alma-atlas connect bigquery --project my-gcp-project

# Explicit service account key
alma-atlas connect bigquery --project my-gcp-project \
  --credentials /path/to/sa-key.json
```

### What is extracted

**Schema introspection** queries `INFORMATION_SCHEMA.COLUMNS` and `INFORMATION_SCHEMA.TABLE_STORAGE` to collect:

- All tables and views in the project
- Column names, types, and nullability
- Table row counts and storage size (from TABLE_STORAGE)

**Query traffic** reads `INFORMATION_SCHEMA.JOBS_BY_PROJECT` (BigQuery retains 180 days of job history). Atlas extracts:

- SQL statements and the tables they reference
- Job execution metadata (user email, creation time)
- Consumer identity — when Airflow labels are present (`dag_id`, `task_id`), the consumer is identified with 0.95 confidence; otherwise Atlas falls back to the user email

Atlas uses a cursor (`bq_creation_time`) so subsequent scans only fetch new jobs.

**Lineage edges** are built by parsing table references from job SQL.

### Config options

Set in `~/.alma/sources.json` under `params`:

| Key | Required | Description |
|-----|----------|-------------|
| `project` | Yes | GCP project ID |
| `credentials` | No | Path to service account JSON key. Omit to use ADC. |

### Known limitations

- Query traffic uses `INFORMATION_SCHEMA.JOBS_BY_PROJECT`, which requires the `bigquery.jobs.listAll` permission. Standard `roles/bigquery.jobUser` is sufficient.
- INFORMATION_SCHEMA queries run against the project's default location. Cross-region datasets may not appear.
- Traffic analysis looks back up to 180 days (BigQuery's maximum job history retention).
- Atlas does not extract BI tool assets (Looker, Data Studio) in this release.

---

## PostgreSQL

### Prerequisites

A database user with read access to the schemas you want to scan:

```sql
-- Create a dedicated read-only user
CREATE USER atlas_user WITH PASSWORD 'yourpassword';
GRANT CONNECT ON DATABASE mydb TO atlas_user;
GRANT USAGE ON SCHEMA public TO atlas_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO atlas_user;
-- Allow access to future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO atlas_user;
```

For query traffic via `pg_stat_statements` (recommended):

```sql
-- In postgresql.conf:
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.track = all

-- After restarting Postgres:
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
GRANT SELECT ON pg_stat_statements TO atlas_user;
```

### Connect

```bash
alma-atlas connect postgres \
  --dsn "postgresql://atlas_user:password@localhost:5432/mydb"

# Scan a non-default schema
alma-atlas connect postgres \
  --dsn "postgresql://atlas_user:password@localhost:5432/mydb" \
  --schema analytics
```

### What is extracted

**Schema introspection** queries `information_schema.columns` and `pg_catalog` views to collect:

- Tables, views, and materialized views in the configured schema
- Column names, types, and nullability
- View definitions and materialized view query text
- Row counts from `pg_stat_user_tables`
- View dependency edges from `pg_depend` + `pg_rewrite`

**Query traffic** is extracted in two modes, in priority order:

1. **`pg_stat_statements`** (preferred) — reads cumulative query statistics if the extension is installed. Extracts normalized query text, call counts, and total execution time.

2. **Log-based** — if `log_capture` is configured, Atlas parses PostgreSQL log files directly for executed statements. This mode supports incremental observation via inode and byte-offset tracking (handles log rotation).

**Lineage edges** are built from view dependency catalog entries and query table references.

### Config options

Set in `~/.alma/sources.json` under `params`:

| Key | Required | Description |
|-----|----------|-------------|
| `dsn` | Yes | PostgreSQL DSN (`postgresql://user:pass@host:port/db`) |
| `schema` | No | Schema to scan. Defaults to `public`. |

### Known limitations

- Only one schema is scanned per registered source. Register multiple sources to cover multiple schemas.
- `pg_stat_statements` provides cumulative statistics, not per-execution detail. Query-to-table attribution depends on Atlas's SQL parser.
- Log-based traffic observation requires filesystem read access to the Postgres log directory, which is typically not available for managed databases (RDS, Cloud SQL, etc.). Use `pg_stat_statements` for managed Postgres.
- Query execution is limited to a 30-second timeout and read-only operations.

---

## dbt

The dbt adapter reads compiled dbt artifacts — no live warehouse connection is required. It extracts models, seeds, snapshots, and their dependency graph from `manifest.json`, and optionally column types from `catalog.json`.

### Prerequisites

- A compiled dbt project (run `dbt compile` or `dbt run` to generate `target/manifest.json`)
- dbt Core 1.8+ (manifest schema v12) or dbt Fusion (schema v20)

### Connect

```bash
# Point at the project directory
alma-atlas connect dbt --project-dir ./my-dbt-project

# Or point directly at the manifest
alma-atlas connect dbt --manifest ./my-dbt-project/target/manifest.json

# Override the displayed project name
alma-atlas connect dbt --project-dir ./my-dbt-project --project analytics
```

### What is extracted

**Schema introspection** parses `manifest.json`:

- Models (`nodes` with resource type `model`) — includes SQL definition and materialization type
- Seeds (`resource_type: seed`) — CSV-based tables
- Snapshots (`resource_type: snapshot`)
- External sources (`sources` in the manifest) — referenced but not defined in dbt

If `target/catalog.json` is present (generated by `dbt docs generate`), Atlas merges column types and descriptions from it.

**Lineage edges** are built from the `depends_on.nodes` field in each node — this is the full compile-time dependency graph dbt produces.

**Materialization mapping:**

| dbt materialization | Atlas kind |
|---------------------|------------|
| `table` | `TABLE` |
| `incremental` | `TABLE` |
| `snapshot` | `TABLE` |
| `seed` | `TABLE` |
| `view` | `VIEW` |
| `ephemeral` | `VIEW` |
| `materialized_view` | `MATERIALIZED_VIEW` |

### Config options

Set in `~/.alma/sources.json` under `params`:

| Key | Required | Description |
|-----|----------|-------------|
| `manifest_path` | Yes | Absolute path to `manifest.json` |
| `project_name` | No | Display name override. Defaults to `name` in the manifest. |

### Known limitations

- The dbt adapter is file-based. Query traffic and live execution are not available.
- Schema types from `catalog.json` are only available if `dbt docs generate` has been run. Without it, column types are omitted.
- Ephemeral models are included in the lineage graph but do not correspond to physical objects in the warehouse.
- Cross-project dbt references (dbt mesh) are not yet resolved across separately registered dbt sources.
- Only manifest schema versions v12 and v20 are supported. Earlier versions may parse with degraded output.
