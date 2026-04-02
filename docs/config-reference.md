# Config Reference

## Configuration Directory

Atlas stores local state in `~/.alma/` by default:

```mermaid
flowchart TD
  R["~/.alma/"]
  R --> DB[atlas.db]
  R --> CJ[config.json]
  R --> SJ[sources.json]
  R --> SC[sync_cursor.json]
```

Override the root with:

```bash
export ALMA_CONFIG_DIR=/etc/alma
```

## `sources.json`

`sources.json` is the canonical persisted source registry for `alma-atlas connect`.
Runtime scans can also supply sources through `atlas.yml` or `--connections`
without mutating `sources.json`. Each persisted entry is:

```json
{
  "id": "source-id",
  "kind": "source-kind",
  "params": {}
}
```

Atlas may also persist runtime source state alongside the definition, for example an `observation_cursor` used by connectors that support resume. Treat the local file format as Atlas-owned implementation detail; code-owned source-kind semantics live in `alma_atlas.source_registry` and the connector registry.

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

- `credentials`: optional absolute path to a service-account JSON file
- `service_account_env`: optional env var containing the raw JSON payload

If neither is set, Atlas uses Application Default Credentials. This is the preferred local-development path.

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

Common params:

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

`atlas.yml` is the runtime config file used by `--config-file` and `get_config()`
autodiscovery. It can define runtime-only sources, hooks, and learning settings.

Supported top-level keys:

- `version`
- `sources`
- `team`
- `hooks`
- `learning`
- `enrichment` (legacy alias for `learning`)

Source definitions in `atlas.yml` use the same raw wire shape as `sources.json`, but the authoritative source-kind contract is code-owned rather than documented here.

### Learning

ACP is the only supported non-mock learning provider.

`learning.agent.command` is the authoritative execution setting for real
learning runs. The nested `explorer`, `pipeline_analyzer`, and `annotator`
blocks describe workflow-role overrides, not necessarily separate long-lived
agent runtimes.

When those workflow-role blocks resolve to the same ACP subprocess settings,
Atlas reuses one ACP session per learning invocation. If Atlas exposes direct
repo access over ACP (for example terminal support and/or injected MCP tools),
the analyzer or annotator may inspect the repo directly from the session `cwd`
and Atlas skips the custom `codebase_explorer` fallback. If direct repo access
is unavailable, Atlas falls back to local `repo_scanner` plus
`codebase_explorer`.

`model`, `api_key_env`, `timeout`, and `max_tokens` are retained only for
backward-compatible config parsing, provenance, and tests; the ACP runtime does
not use them to choose or authenticate the underlying model process unless the
spawned ACP agent exposes its own config options and Atlas chooses to map them.

Preferred shape:

```yaml
version: 1
learning:
  provider: acp
  agent:
    command: claude-agent-acp
```

`mock` remains available for tests and no-op local flows.

Advanced per-role override:

```yaml
version: 1
learning:
  agent:
    command: claude-agent-acp
  explorer:
    provider: acp
  pipeline_analyzer:
    provider: acp
  annotator:
    provider: acp
```

Legacy flat format is still accepted and expands to all workflow roles:

```yaml
version: 1
learning:
  provider: acp
  agent:
    command: claude-agent-acp
```

## Asset IDs

Atlas uses the canonical form `{source_id}::{object_ref}`.

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
