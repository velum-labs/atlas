# Alma Atlas

[![PyPI version](https://img.shields.io/pypi/v/alma-atlas)](https://pypi.org/project/alma-atlas/)
[![CI](https://github.com/almaos/atlas/actions/workflows/ci.yml/badge.svg)](https://github.com/almaos/atlas/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

**Open-source data stack discovery CLI + MCP server**

Alma Atlas scans your data warehouse, dbt project, and BI tools to build a live dependency graph of your entire data stack — tables, schemas, query traffic, and lineage — then exposes that graph as [Model Context Protocol](https://modelcontextprotocol.io) tools so AI agents can answer questions about your data infrastructure in real time.

## Why Atlas?

AI coding assistants (Cursor, Claude Code, Copilot) write syntactically valid but semantically wrong data code. They can see your files but know nothing about:

- The live schema of your warehouse — so they reference columns that don't exist
- How data flows through your stack — so they pick the wrong table for a join
- Who consumes what downstream — so a refactor silently breaks a dashboard

Atlas gives your AI the context a senior data engineer carries in their head: live schemas, lineage, query traffic, and blast-radius analysis — all queryable through MCP tools.

## Quickstart

```bash
uv add alma-atlas

# Or install from ghcr.io (pre-release / latest tagged builds):
# uv add alma-atlas --index velum=https://pyoci.com/ghcr.io/velum-labs/

# Connect a source (pick one or more)
alma-atlas connect bigquery --project my-gcp-project
alma-atlas connect snowflake --account xy12345.us-east-1 --role ANALYST
alma-atlas connect postgres --dsn postgresql://user:pass@host/db
alma-atlas connect dbt --project-dir ./my-dbt-project

# Scan all connected sources
alma-atlas scan

# Start the MCP server
alma-atlas serve
```

Then add Atlas to your IDE — see [IDE configuration](#ide-configuration) below.

See [docs/quickstart.md](docs/quickstart.md) for a full walkthrough including expected output and credential setup.

## MCP Tools

Six tools are registered when you run `alma-atlas serve`:

| Tool | Description |
|------|-------------|
| `atlas_search` | Full-text search across all assets by name, ID, or keyword |
| `atlas_get_asset` | Full metadata for an asset: kind, tags, row count, first/last seen |
| `atlas_get_schema` | Column names, types, and nullability for a table or view |
| `atlas_lineage` | Upstream or downstream traversal with configurable depth |
| `atlas_impact` | All assets transitively downstream of a given asset |
| `atlas_status` | Graph summary: asset counts by kind and source |

See [docs/mcp-tools.md](docs/mcp-tools.md) for input schemas and example output.

## Adapters

| Adapter | Schema | Query Traffic | Lineage | Execute |
|---------|--------|---------------|---------|---------|
| BigQuery | Yes | Yes (INFORMATION_SCHEMA.JOBS) | Yes | Yes |
| Snowflake | Yes | Yes (ACCOUNT_USAGE.QUERY_HISTORY) | Yes | Yes |
| PostgreSQL | Yes | Yes (pg_stat_statements / logs) | Yes | Yes |
| dbt | Yes (manifest + catalog) | No | Yes (depends_on) | No |

See [docs/adapters.md](docs/adapters.md) for prerequisites, config options, and limitations per adapter.

## IDE Configuration

### Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

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

Create or edit `.cursor/mcp.json` in your project root:

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

Restart the IDE after saving. The MCP tools (`atlas_search`, `atlas_lineage`, etc.) will appear automatically.

## Architecture

```mermaid
graph TD
    subgraph Sources
        BQ[BigQuery]
        SF[Snowflake]
        PG[PostgreSQL]
        DBT[dbt]
    end

    subgraph alma-connectors
        BQA[BigQuery Adapter]
        SFA[Snowflake Adapter]
        PGA[Postgres Adapter]
        DBTA[dbt Adapter]
    end

    subgraph alma-atlas
        PIPE[Scan Pipeline]
        CLI[CLI]
        MCP[MCP Server]
    end

    subgraph Storage
        DB[(atlas.db\nSQLite)]
    end

    subgraph Consumers
        CLAUDE[Claude Desktop]
        CURSOR[Cursor]
        CLINE[Cline / Continue]
    end

    BQ --> BQA
    SF --> SFA
    PG --> PGA
    DBT --> DBTA

    BQA --> PIPE
    SFA --> PIPE
    PGA --> PIPE
    DBTA --> PIPE

    PIPE --> DB
    DB --> CLI
    DB --> MCP

    MCP -->|MCP stdio / SSE| CLAUDE
    MCP -->|MCP stdio / SSE| CURSOR
    MCP -->|MCP stdio / SSE| CLINE
```

## Package Structure

Alma Atlas is a Python monorepo. Each package has a single responsibility:

| Package | Purpose |
|---------|---------|
| `alma-atlas` | CLI, MCP server, scan pipeline orchestration |
| `alma-atlas-store` | SQLite persistence (assets, edges, schemas, queries) |
| `alma-ports` | Protocol interfaces — zero runtime dependencies |
| `alma-connectors` | Source adapters (BigQuery, Snowflake, Postgres, dbt) |
| `alma-analysis` | Pure analysis functions (lineage, consumer identity) |
| `alma-sqlkit` | SQL parsing and normalization utilities |
| `alma-algebrakit` | SQL algebraic fingerprinting for query deduplication |

## Documentation

- [Quickstart](docs/quickstart.md) — step-by-step setup for each adapter
- [MCP Tools Reference](docs/mcp-tools.md) — tool names, input schemas, example output
- [Config Reference](docs/config-reference.md) — `sources.json` format and environment variables
- [Adapters](docs/adapters.md) — prerequisites, config options, and limitations per adapter

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, code style, and how to add a new connector.

Requirements: Python 3.12+, [uv](https://docs.astral.sh/uv/)

```bash
git clone https://github.com/almaos/atlas.git
cd atlas
uv sync
uv run alma-atlas --help
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
