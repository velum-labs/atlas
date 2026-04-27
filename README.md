# Alma Atlas

[![PyPI version](https://img.shields.io/pypi/v/alma-atlas)](https://pypi.org/project/alma-atlas/)
[![CI](https://github.com/almaos/atlas/actions/workflows/ci.yml/badge.svg)](https://github.com/almaos/atlas/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

Open-source data stack discovery CLI + MCP server

Alma Atlas scans warehouses, dbt projects, orchestration systems, and BI/semantic layers into one local graph, then exposes that graph over [Model Context Protocol](https://modelcontextprotocol.io) so agents can answer questions with live schema, lineage, and query-context instead of guessing from code alone.

## Why Atlas?

AI coding assistants can read your repo, but they do not know:

- which columns exist right now
- how data actually flows between systems
- what breaks downstream if a table changes
- which queries or dashboards depend on a dataset

Atlas gives them that context through a local graph and MCP tools.

## Try it in 60 seconds

No warehouse credentials needed — Atlas ships a bundled sample data stack
(mock Snowflake + dbt + Looker) so you can see what an agent does with cross-system
context before connecting anything real.

```bash
uv add alma-atlas

# Install bundled sample data into ~/.alma-atlas/atlas.db
alma-atlas sample install

# Register Atlas as an MCP server in your AI client
alma-atlas install cursor          # or: alma-atlas install claude
```

Restart Cursor / Claude Desktop, then ask the agent something like
*"what depends on `snowflake:demo::analytics.orders`?"* — Atlas walks the
sample lineage chain across all three mock sources.

`alma-atlas sample preview` lists what's in the bundled snapshot.

## Quickstart (your own data)

```bash
uv add alma-atlas

# Authenticate BigQuery for local development (ADC)
gcloud auth application-default login

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

# Register Atlas as an MCP server in Cursor / Claude Desktop
alma-atlas install cursor          # or: alma-atlas install claude
```

Don't want Atlas anymore? `alma-atlas uninstall` removes the local data
directory entirely (graph, encrypted credentials, telemetry id).


## Docker (optional)

If you prefer running Atlas in a container:

```bash
# build image
docker build -t atlas .

# run CLI commands
docker run --rm -v $HOME/.alma-atlas:/root/.alma-atlas atlas status

# or use docker compose
docker compose run --rm atlas scan

# start MCP server (SSE) on http://localhost:8080
docker compose up atlas-mcp
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

Atlas uses canonical asset IDs in the form `{source_id}::{object_ref}`.

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

The recommended path is `alma-atlas install cursor` (or `install claude`),
which writes the MCP config and merges with any other MCP servers you
already have registered. The CLI handles project-vs-global scope on Cursor
(`--scope project|global`, default global) and resolves the right config
path on Mac, Linux, and Windows.

If you'd rather edit the JSON yourself, the entry looks like:

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

## Atlas Companion (concierge mode)

Atlas Companion is a curated 3-tool MCP surface for technical leads at
companies running [Alma](https://velum.com). Instead of the 20-tool atlas_*
surface, an invite token gates access and exposes only:

- `companion_search_assets`
- `companion_get_schema_and_owner`
- `companion_explain_lineage_and_contract`

Each tool returns a curated `CompanionBundle` — short, prompt-ready context
blocks instead of raw metadata dumps. Every MCP call validates the invite
token against the Alma deployment endpoint (instant revocation, no caching).

```bash
# Install with an invite token (writes the config and the token together)
alma-atlas install cursor --token <invite>

# Or run Companion mode directly
alma-atlas serve --alma-token <invite>
ALMA_INVITE_TOKEN=<invite> alma-atlas serve

# Override the Alma endpoint (defaults to https://app.alma.dev)
alma-atlas serve --alma-token <invite> --alma-endpoint https://staging.alma.dev
```

Invite tokens are issued by Velum to named users. If you don't have one,
the default `alma-atlas serve` (full 20-tool surface) is what you want.

## Telemetry

Atlas emits anonymous behavioral telemetry to PostHog Cloud so we can
understand which tools get used and where installs come from. Two buckets:

- **Mandatory (always on, anonymous)** — counts of tool calls, install
  events, and source kinds (`tool_name`, `mcp_session_duration_seconds`,
  `connector_kind`, `install_source`, `atlas_version`, `platform`,
  `python_version`). No file paths, no user identifiers, no warehouse
  data.

- **Opt-in (account-correlated)** — only enabled in Atlas Companion mode,
  where the invite token implies consent. Sends a SHA-256 truncation of
  the token as a stable correlator (the raw token never reaches PostHog)
  so Velum can attribute installs to specific accounts in funnel analysis.

To disable telemetry entirely:

```bash
ATLAS_TELEMETRY_OFF=1 alma-atlas serve
```

PostHog API errors are silent — telemetry never crashes the host process.

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
| ------- | ------- |
| `alma-atlas` | CLI, MCP server, scan orchestration, learning, sync |
| `alma-atlas-store` | SQLite repositories and migrations |
| `alma-connectors` | Source adapters |
| `alma-analysis` | Graph and lineage analysis |
| `alma-sqlkit` | SQL parsing and normalization |
| `alma-algebrakit` | SQL algebra and fingerprinting |
| `alma-ports` | Shared protocols and safety helpers |

## Documentation

- [Quickstart](docs/quickstart.md)
- [Reference Overview](docs/reference/index.md)
- [API Reference](docs/reference/api/index.md)
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
