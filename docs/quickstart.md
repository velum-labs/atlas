# Quickstart

This guide walks from install to a working local Atlas graph and MCP server.

## Install

```bash
uv add alma-atlas
```

Verify:

```bash
alma-atlas --help
```

## Register Sources

### BigQuery

Atlas prefers Application Default Credentials (ADC) for local development. Use an explicit
service account key only when you need non-user auth or cannot rely on ADC.

```bash
# Preferred: ADC
gcloud auth application-default login
alma-atlas connect bigquery --project my-gcp-project

# Optional fallback: explicit key file
alma-atlas connect bigquery \
  --project my-gcp-project \
  --credentials ~/.config/gcloud/atlas-sa.json

# Optional fallback: raw JSON from an env var
export BQ_SERVICE_ACCOUNT_JSON='{"type":"service_account","project_id":"my-gcp-project",...}'
alma-atlas connect bigquery \
  --project my-gcp-project \
  --service-account-env BQ_SERVICE_ACCOUNT_JSON
```

### PostgreSQL

```bash
alma-atlas connect postgres \
  --dsn "postgresql://atlas_user:password@localhost:5432/mydb" \
  --schema public
```

### Snowflake

Atlas expects a Snowflake connection JSON blob in an env var such as:

```bash
export SNOWFLAKE_CONNECTION_JSON='{"account":"xy12345.us-east-1","user":"ATLAS_USER","password":"...","warehouse":"COMPUTE_WH","database":"ANALYTICS","role":"ANALYST"}'
```

Then register the source:

```bash
alma-atlas connect snowflake \
  --account xy12345.us-east-1 \
  --account-secret-env SNOWFLAKE_CONNECTION_JSON \
  --role ANALYST \
  --schema ANALYTICS
```

### dbt

```bash
# Atlas auto-discovers target/manifest.json and, when present,
# target/catalog.json and target/run_results.json.
alma-atlas connect dbt --project-dir ./my-dbt-project
```

### Optional Community Sources

```bash
alma-atlas connect airflow --base-url https://airflow.example.com --auth-token-env AIRFLOW_AUTH_TOKEN
alma-atlas connect looker --instance-url https://looker.example.com --client-id-env LOOKER_CLIENT_ID --client-secret-env LOOKER_CLIENT_SECRET
alma-atlas connect fivetran --api-key-env FIVETRAN_API_KEY --api-secret-env FIVETRAN_API_SECRET
alma-atlas connect metabase --instance-url https://metabase.example.com --api-key-env METABASE_API_KEY
```

## Scan

```bash
alma-atlas scan
```

Atlas writes a local SQLite graph at `~/.alma/atlas.db` by default.

## Verify

```bash
alma-atlas status
alma-atlas search orders
```

Use MCP or CLI search/lineage commands to discover real asset IDs before requesting a specific asset.

## Asset IDs

Atlas uses canonical IDs in the form `{source_id}::{object_ref}`.

Examples:

- `bigquery:my-project::analytics.orders`
- `postgres:customer:public::public.users`
- `dbt:analytics::marts.fct_orders`

## Serve MCP

```bash
# stdio (for local IDE integration)
alma-atlas serve

# SSE
alma-atlas serve --transport sse --host 127.0.0.1 --port 8080
```

For SSE clients, the endpoint is `http://127.0.0.1:8080/sse`.

## Re-scan

Run `alma-atlas scan` whenever you want to refresh the graph. Traffic-backed connectors persist observation cursors in the saved source config so subsequent scans can resume from the last observed point.

## Next Steps

- [MCP Tools Reference](mcp-tools.md)
- [Config Reference](config-reference.md)
- [Adapters](adapters.md)
