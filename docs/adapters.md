# Adapters

Atlas scans all supported source kinds through the same canonical runtime, but this page is intentionally descriptive rather than authoritative. The canonical source-kind contract lives in `alma_atlas.source_registry`; runtime connector behavior lives in the connector registry and adapter code.

## BigQuery

Use either:

- Application Default Credentials
- `credentials` path in `sources.json`
- `service_account_env` pointing at raw JSON in an env var

Common params:

- `project_id`
- `location`
- `credentials`
- `service_account_env`
- `lookback_hours`

## PostgreSQL

Common params:

- `dsn` or `dsn_env`
- `include_schemas`
- `exclude_schemas`
- `log_capture`
- `probe_target`
- `read_replica`

Traffic comes from `pg_stat_statements` when available, with optional log-based fallback.

## Snowflake

Snowflake registration is env-backed. Atlas expects `account_secret_env` to point at a JSON blob containing at least:

- `account`
- `user`
- `password`

Common params:

- `warehouse`
- `database`
- `role`
- `include_schemas`
- `exclude_schemas`
- `lookback_hours`

## dbt

Atlas reads dbt artifacts directly from disk.

Common params:

- `manifest_path`
- `catalog_path`
- `run_results_path`
- `project_name`

Atlas auto-discovers `catalog.json` and `run_results.json` when you register via `connect dbt --project-dir ...` and those files exist under `target/`.

dbt source nodes are projected into the canonical graph as `external_table` assets.

## Airflow

Common params:

- `base_url`
- `auth_token` or `auth_token_env`
- `username` / `password`

Atlas projects DAG discovery, task-execution traffic, Airflow-derived lineage, and orchestration metadata.

## Looker

Common params:

- `instance_url`
- `client_id` / `client_secret`
- `client_id_env` / `client_secret_env`
- `port`

Atlas projects explores as semantic-model assets and declared lineage from `sql_table_name` / join metadata.

## Fivetran

Common params:

- `api_key` / `api_secret`
- `api_key_env` / `api_secret_env`

Atlas projects connector discovery, connector API lineage, and orchestration schedule metadata.

## Metabase

Common params:

- `instance_url`
- `api_key` / `api_key_env`
- or `username` / `password`

Atlas projects connected databases/tables plus query activity. Lineage is currently limited compared to warehouse/native SQL connectors.

## Notes

- Asset IDs always use `source_id::{object_ref}`.
- All connectors now flow through the same canonical persisted adapter model and runtime adapter factory as the core warehouse adapters.
- Community connectors no longer rely on a separate v1 scan path; they are scanned through the same capability-probed runtime as the core warehouse adapters.
- If a source param is not documented here, Atlas rejects it instead of silently ignoring it.
- `sources.json` is the persisted registry for `alma-atlas connect`; `atlas.yml` and `--connections` can override runtime sources without changing persisted state.
