# Quickstart

This guide walks you from a fresh install to a working MCP server in under 5 minutes. Pick the path that matches your data stack.

## Prerequisites

- Python 3.12+
- Valid credentials for at least one source (BigQuery, PostgreSQL, or dbt)

## Install

```bash
pip install alma-atlas
```

Verify:

```bash
alma-atlas --help
```

## Path A: BigQuery

### 1. Authenticate

Atlas reads BigQuery using Application Default Credentials or an explicit service account key.

**Option 1 — ADC (recommended for local use):**

```bash
gcloud auth application-default login
```

**Option 2 — Service account key:**

Download a JSON key for a service account with the following roles:

- `roles/bigquery.jobUser` — run INFORMATION_SCHEMA queries
- `roles/bigquery.metadataViewer` — read table and column metadata

Save the key to a local path, e.g. `~/.config/gcloud/atlas-sa.json`.

### 2. Connect

```bash
alma-atlas connect bigquery --project my-gcp-project
# with an explicit key file:
alma-atlas connect bigquery --project my-gcp-project --credentials ~/.config/gcloud/atlas-sa.json
```

Expected output:

```
Connected: BigQuery project my-gcp-project
```

### 3. Scan

```bash
alma-atlas scan
```

Expected output:

```
Scanning bigquery:my-gcp-project...
  assets: 142  edges: 89
Scan complete.
```

### 4. Verify

```bash
alma-atlas status
```

Expected output:

```
Atlas graph: 142 assets, 89 edges, 34 query fingerprints

Assets by kind:
  TABLE: 98
  VIEW: 44

Assets by source:
  bigquery:my-gcp-project: 142
```

### 5. Start the MCP server

```bash
alma-atlas serve
```

Expected output:

```
Alma Atlas MCP Server — transport: stdio
```

The server is now listening on stdin/stdout. Connect your IDE — see [IDE configuration](../README.md#ide-configuration).

---

## Path B: PostgreSQL

### 1. Prepare credentials

Atlas connects via a standard DSN. The database user needs:

```sql
GRANT CONNECT ON DATABASE mydb TO atlas_user;
GRANT USAGE ON SCHEMA public TO atlas_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO atlas_user;
```

For query traffic via `pg_stat_statements`:

```sql
-- In postgresql.conf:
shared_preload_libraries = 'pg_stat_statements'

-- Then:
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
GRANT SELECT ON pg_stat_statements TO atlas_user;
```

### 2. Connect

```bash
alma-atlas connect postgres --dsn "postgresql://atlas_user:password@localhost:5432/mydb"
# scan a non-default schema:
alma-atlas connect postgres --dsn "postgresql://..." --schema analytics
```

Expected output:

```
Connected: Postgres database mydb
```

### 3. Scan

```bash
alma-atlas scan
```

Expected output:

```
Scanning postgres:mydb...
  assets: 67  edges: 41
Scan complete.
```

### 4. Verify and serve

```bash
alma-atlas status
alma-atlas serve
```

---

## Path C: dbt

Atlas reads dbt's compiled artifacts — no warehouse connection required for schema and lineage data.

### 1. Compile your dbt project

```bash
cd my-dbt-project
dbt compile   # or: dbt run
```

This produces `target/manifest.json` (and optionally `target/catalog.json`).

### 2. Connect

```bash
# Point at the project directory (Atlas finds target/manifest.json automatically):
alma-atlas connect dbt --project-dir ./my-dbt-project

# Or point directly at the manifest:
alma-atlas connect dbt --manifest ./my-dbt-project/target/manifest.json

# Override the project display name:
alma-atlas connect dbt --project-dir ./my-dbt-project --project my-project
```

Expected output:

```
Connected: dbt project from /Users/you/my-dbt-project/target/manifest.json
```

### 3. Scan

```bash
alma-atlas scan
```

Expected output:

```
Scanning dbt:my-project...
  assets: 54  edges: 78
Scan complete.
```

### 4. Verify and serve

```bash
alma-atlas status
alma-atlas serve
```

---

## Combining multiple sources

Atlas merges assets and edges from all registered sources into a single graph. Register as many sources as you need:

```bash
alma-atlas connect bigquery --project my-gcp-project
alma-atlas connect dbt --project-dir ./my-dbt-project
alma-atlas scan
alma-atlas status
```

List registered sources:

```bash
alma-atlas connect list
```

Remove a source:

```bash
alma-atlas connect remove bigquery:my-gcp-project
```

---

## Re-scanning

Run `alma-atlas scan` at any time to refresh the graph. Atlas uses cursors for incremental traffic observation — only new query events are fetched on subsequent scans.

## SSE transport

For remote or multi-client setups, use the SSE transport instead of stdio:

```bash
alma-atlas serve --transport sse --host 127.0.0.1 --port 8080
```

Then configure your MCP client with `http://127.0.0.1:8080/sse` as the endpoint.

## Next steps

- [MCP Tools Reference](mcp-tools.md) — what tools are available to your AI agent
- [Config Reference](config-reference.md) — full `sources.json` format
- [Adapters](adapters.md) — adapter-specific options and limitations
