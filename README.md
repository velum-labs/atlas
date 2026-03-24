# Alma Atlas

**Open-source data stack discovery CLI + MCP server**

Alma Atlas scans your data warehouse, dbt project, and BI tools to build a live dependency graph of your entire data stack — tables, schemas, query traffic, and lineage — then exposes that graph as [Model Context Protocol](https://modelcontextprotocol.io) tools so AI agents can answer questions about your data infrastructure in real time.

## Features

- **Connect** — register BigQuery, Postgres, and dbt sources in seconds
- **Scan** — automatically discover assets, schemas, query traffic, and upstream/downstream lineage
- **Graph** — stitch everything into a typed dependency graph (tables → views → queries → consumers)
- **Search** — find any asset by name, ID, or keyword across all connected sources
- **Lineage** — trace upstream and downstream dependencies across source boundaries
- **Impact analysis** — see every downstream asset affected before making a change
- **MCP server** — expose the full graph as MCP tools for Claude, Cursor, Cline, and any MCP-compatible agent

## Quickstart

```bash
# 1. Install
pip install alma-atlas

# 2. Connect a source
alma-atlas connect bigquery --project my-gcp-project
# or: alma-atlas connect postgres --dsn postgresql://user:pass@host/db
# or: alma-atlas connect dbt --project-dir ./my-dbt-project

# 3. Scan
alma-atlas scan

# 4. Verify
alma-atlas status

# 5. Start the MCP server
alma-atlas serve
```

Then add Atlas to your IDE — see [IDE configuration](#ide-configuration) below.

See [docs/quickstart.md](docs/quickstart.md) for a full walkthrough including expected output and credential setup.

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
        PG[PostgreSQL]
        DBT[dbt]
    end

    subgraph alma-connectors
        BQA[BigQuery Adapter]
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
    PG --> PGA
    DBT --> DBTA

    BQA --> PIPE
    PGA --> PIPE
    DBTA --> PIPE

    PIPE --> DB
    DB --> CLI
    DB --> MCP

    MCP -->|MCP stdio / SSE| CLAUDE
    MCP -->|MCP stdio / SSE| CURSOR
    MCP -->|MCP stdio / SSE| CLINE
```

## Supported Adapters

| Adapter | Schema | Query Traffic | Lineage | Execute |
|---------|--------|---------------|---------|---------|
| BigQuery | Yes | Yes (INFORMATION_SCHEMA.JOBS) | Yes | Yes |
| PostgreSQL | Yes | Yes (logs / pg_stat_statements) | Yes | Yes |
| dbt | Yes (manifest + catalog) | No | Yes (depends_on) | No |

## Package Structure

Alma Atlas is a Python monorepo. Each package has a single responsibility:

| Package | Purpose |
|---------|---------|
| `alma-atlas` | CLI, MCP server, scan pipeline orchestration |
| `alma-atlas-store` | SQLite persistence (assets, edges, schemas, queries) |
| `alma-ports` | Protocol interfaces — zero runtime dependencies |
| `alma-connectors` | Source adapters (BigQuery, Postgres, dbt) |
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
