# Adapters

Atlas scans all supported source kinds through the same canonical runtime, but each connector exposes a different capability mix.

## Capability Summary

| Kind | Register via CLI | Assets | Traffic | Lineage | Orchestration |
|------|------------------|--------|---------|---------|---------------|
| BigQuery | Yes | Yes | Yes | Yes | No |
| PostgreSQL | Yes | Yes | Yes | Yes | No |
| Snowflake | Yes | Yes | Yes | Yes | No |
| dbt | Yes | Yes | No | Yes | No |
| Airflow | Yes | Yes | Yes | Yes | Yes |
| Looker | Yes | Yes | No | Yes | No |
| Fivetran | Yes | Yes | No | Yes | Yes |
| Metabase | Yes | Yes | Yes | Limited | No |

## BigQuery

Use either:

- Application Default Credentials
- `credentials` path in `sources.json`
- `service_account_env` pointing at raw JSON in an env var

Important params:

- `project_id`
- `location`
- `credentials`
- `service_account_env`
- `lookback_hours`

## PostgreSQL

Important params:

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

Optional params:

- `warehouse`
- `database`
- `role`
- `include_schemas`
- `exclude_schemas`
- `lookback_hours`

## dbt

Atlas reads dbt artifacts directly from disk.

Important params:

- `manifest_path`
- `catalog_path`
- `run_results_path`
- `project_name`

Atlas auto-discovers `catalog.json` and `run_results.json` when you register via `connect dbt --project-dir ...` and those files exist under `target/`.

dbt source nodes are projected into the canonical graph as `external_table` assets.

## Airflow

Important params:

- `base_url`
- `auth_token` or `auth_token_env`
- `username` / `password`

Atlas projects DAG discovery, task-execution traffic, Airflow-derived lineage, and orchestration metadata.

## Looker

Important params:

- `instance_url`
- `client_id` / `client_secret`
- `client_id_env` / `client_secret_env`
- `port`

Atlas projects explores as semantic-model assets and declared lineage from `sql_table_name` / join metadata.

## Fivetran

Important params:

- `api_key` / `api_secret`
- `api_key_env` / `api_secret_env`

Atlas projects connector discovery, connector API lineage, and orchestration schedule metadata.

## Metabase

Important params:

- `instance_url`
- `api_key` / `api_key_env`
- or `username` / `password`

Atlas projects connected databases/tables plus query activity. Lineage is currently limited compared to warehouse/native SQL connectors.

## Notes

- Asset IDs always use `source_id::{object_ref}`.
- Community connectors no longer rely on a separate v1 scan path; they are scanned through the same capability-probed runtime as the core warehouse adapters.
- If a source param is not documented here, Atlas now rejects it instead of silently ignoring it.
