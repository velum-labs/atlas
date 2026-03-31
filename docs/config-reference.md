# Config Reference

## Configuration Directory

Atlas stores local state in `~/.alma/` by default:

```text
~/.alma/
├── atlas.db
├── config.json
├── sources.json
└── sync_cursor.json
```

Override the root with:

```bash
export ALMA_CONFIG_DIR=/etc/alma
```

## `sources.json`

`sources.json` is the canonical on-disk source registry. Each entry is:

```json
{
  "id": "source-id",
  "kind": "source-kind",
  "params": {}
}
```

Atlas persists incremental traffic cursors back into `params.observation_cursor` for connectors that support resume.

### BigQuery

```json
{
  "id": "bigquery:my-gcp-project",
  "kind": "bigquery",
  "params": {
    "project_id": "my-gcp-project",
    "location": "us"
  }
}
```

Optional auth fields:

- `credentials`: absolute path to a service-account JSON file
- `service_account_env`: env var containing the raw JSON payload

If neither is set, Atlas uses Application Default Credentials.

### PostgreSQL

```json
{
  "id": "postgres:customer:public",
  "kind": "postgres",
  "params": {
    "dsn": "postgresql://atlas_user:password@localhost:5432/customer",
    "include_schemas": ["public"]
  }
}
```

Supported params:

- `dsn` or `dsn_env`
- `include_schemas`
- `exclude_schemas`
- `log_capture`
- `probe_target`
- `read_replica`

### Snowflake

```json
{
  "id": "snowflake:xy12345.us-east-1",
  "kind": "snowflake",
  "params": {
    "account": "xy12345.us-east-1",
    "account_secret_env": "SNOWFLAKE_CONNECTION_JSON",
    "warehouse": "COMPUTE_WH",
    "database": "ANALYTICS",
    "role": "ANALYST",
    "include_schemas": ["ANALYTICS"]
  }
}
```

`account_secret_env` should point to JSON like:

```json
{
  "account": "xy12345.us-east-1",
  "user": "ATLAS_USER",
  "password": "secret",
  "warehouse": "COMPUTE_WH",
  "database": "ANALYTICS",
  "role": "ANALYST"
}
```

### dbt

```json
{
  "id": "dbt:analytics",
  "kind": "dbt",
  "params": {
    "manifest_path": "/repo/target/manifest.json",
    "catalog_path": "/repo/target/catalog.json",
    "run_results_path": "/repo/target/run_results.json",
    "project_name": "analytics"
  }
}
```

### Airflow

```json
{
  "id": "airflow:airflow-example-com",
  "kind": "airflow",
  "params": {
    "base_url": "https://airflow.example.com",
    "auth_token_env": "AIRFLOW_AUTH_TOKEN"
  }
}
```

### Looker

```json
{
  "id": "looker:looker-example-com",
  "kind": "looker",
  "params": {
    "instance_url": "https://looker.example.com",
    "client_id_env": "LOOKER_CLIENT_ID",
    "client_secret_env": "LOOKER_CLIENT_SECRET",
    "port": 19999
  }
}
```

### Fivetran

```json
{
  "id": "fivetran:default",
  "kind": "fivetran",
  "params": {
    "api_key_env": "FIVETRAN_API_KEY",
    "api_secret_env": "FIVETRAN_API_SECRET"
  }
}
```

### Metabase

```json
{
  "id": "metabase:metabase-example-com",
  "kind": "metabase",
  "params": {
    "instance_url": "https://metabase.example.com",
    "api_key_env": "METABASE_API_KEY"
  }
}
```

## `atlas.yml`

`atlas.yml` is the runtime config file used by `--config-file` and `get_config()` autodiscovery.

Supported top-level keys:

- `version`
- `sources`
- `team`
- `hooks`
- `learning`
- `enrichment` (legacy alias for `learning`)

### Learning

ACP is the only supported non-mock learning provider.

Example:

```yaml
version: 1
learning:
  explorer:
    provider: acp
    agent:
      command: claude-agent-acp
  pipeline_analyzer:
    provider: acp
    agent:
      command: claude-agent-acp
  annotator:
    provider: acp
    agent:
      command: claude-agent-acp
```

`mock` remains available for tests and no-op local flows.

## Asset IDs

Atlas uses:

```text
{source_id}::{object_ref}
```

Examples:

- `bigquery:my-project::analytics.orders`
- `postgres:customer:public::public.users`
- `dbt:analytics::marts.fct_orders`

## CLI Source Management

```bash
alma-atlas connect list
alma-atlas connect remove bigquery:my-gcp-project
```

## SQLite Database

`atlas.db` stores the canonical local graph:

- assets
- edges
- schema snapshots
- query fingerprints
- contracts
- violations
- learned annotations

The database can be deleted and rebuilt with `alma-atlas scan`.
