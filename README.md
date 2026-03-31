# Alma Atlas

[![PyPI version](https://img.shields.io/pypi/v/alma-atlas)](https://pypi.org/project/alma-atlas/)
[![CI](https://github.com/almaos/atlas/actions/workflows/ci.yml/badge.svg)](https://github.com/almaos/atlas/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

**Open-source data stack discovery CLI + MCP server**

Alma Atlas scans warehouses, dbt projects, orchestration systems, and BI/semantic layers into one local graph, then exposes that graph over [Model Context Protocol](https://modelcontextprotocol.io) so agents can answer questions with live schema, lineage, and query-context instead of guessing from code alone.

## Why Atlas?

AI coding assistants can read your repo, but they do not know:

- which columns exist right now
- how data actually flows between systems
- what breaks downstream if a table changes
- which queries or dashboards depend on a dataset

Atlas gives them that context through a local graph and MCP tools.

## Quickstart

```bash
uv add alma-atlas

# Register one or more sources
alma-atlas connect bigquery --project my-gcp-project
alma-atlas connect postgres --dsn "postgresql://user:pass@host/db" --schema public
alma-atlas connect snowflake \
  --account xy12345.us-east-1 \
  --account-secret-env SNOWFLAKE_CONNECTION_JSON \
  --role ANALYST \
  --schema ANALYTICS
alma-atlas connect dbt --project-dir ./my-dbt-project

# Scan all registered sources
alma-atlas scan

# Start the MCP server
alma-atlas serve
```

See [docs/quickstart.md](docs/quickstart.md) for connector-specific setup and examples.

## Supported Source Kinds

Atlas currently supports:

- `bigquery`
- `postgres`
- `snowflake`
- `dbt`
- `airflow`
- `looker`
- `fivetran`
- `metabase`

Use [docs/adapters.md](docs/adapters.md) for examples and setup notes. The canonical source-kind contract lives in `alma_atlas.source_registry` and the connector runtime registry.

## MCP Tools

`alma-atlas serve` registers the Atlas MCP tool set for search, schema lookup, lineage, contracts, violations, and team sync.

See [docs/mcp-tools.md](docs/mcp-tools.md) for usage examples. The canonical tool catalog lives in `alma_atlas.mcp.tools`.

## Asset IDs

Atlas uses canonical asset IDs in the form:

```text
{source_id}::{object_ref}
```

Examples:

- `bigquery:my-project::analytics.orders`
- `postgres:customer:public::public.users`
- `dbt:analytics::marts.fct_orders`
- `looker:bi-example::ecommerce.orders`

If you do not know an ID, use `atlas_search` or `alma-atlas search` first.

## Learning

Learning is ACP-only. Configure learning in `atlas.yml` with `learning.agent.command`
or `provider: acp`; ACP is the runtime boundary, while `explorer`,
`pipeline_analyzer`, and `annotator` are workflow roles on top. When those
roles resolve to the same ACP subprocess settings, Atlas reuses one ACP session
per learning invocation. `mock` is still available for tests and local no-op
flows.

## IDE Configuration

### Claude Desktop

```json
{
  "mcpServers": {
    "atlas": {
      "command": "alma-atlas",
      "args": ["serve"]
    }
  }
}
```

### Cursor

```json
{
  "mcpServers": {
    "atlas": {
      "command": "alma-atlas",
      "args": ["serve"]
    }
  }
}
```

Restart your IDE after saving.

## Architecture

```mermaid
flowchart TD
    sources[Sources] --> adapters[SourceAdapterV2 adapters]
    adapters --> scanner[Canonical scan orchestrator]
    scanner --> graph["SQLite graph: assets / edges / schema / queries / contracts / violations / annotations"]
    graph --> cli[CLI]
    graph --> mcp[MCP server]
    graph --> sync[Team sync]
```

## Package Layout

| Package | Purpose |
|---------|---------|
| `alma-atlas` | CLI, MCP server, scan orchestration, learning, sync |
| `alma-atlas-store` | SQLite repositories and migrations |
| `alma-connectors` | Source adapters |
| `alma-analysis` | Graph and lineage analysis |
| `alma-sqlkit` | SQL parsing and normalization |
| `alma-algebrakit` | SQL algebra and fingerprinting |
| `alma-ports` | Shared protocols and safety helpers |

## Documentation

- [Quickstart](docs/quickstart.md)
- [MCP Tools Reference](docs/mcp-tools.md)
- [Config Reference](docs/config-reference.md)
- [Adapters](docs/adapters.md)
- [Architecture](docs/architecture.md)

## Contributing

```bash
git clone https://github.com/almaos/atlas.git
cd atlas
uv sync --all-packages
uv run alma-atlas --help
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
