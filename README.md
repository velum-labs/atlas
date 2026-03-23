# Alma Atlas

**Open-source data stack discovery CLI + MCP server**

Alma Atlas scans your data warehouse, dbt project, and BI tools to build a live dependency graph of your entire data stack — tables, queries, consumers, contracts — then exposes that graph over an MCP interface for AI-assisted data engineering.

## Features

- **Connect** — register BigQuery, Snowflake, Postgres, or dbt sources
- **Scan** — discover assets, schemas, query traffic, and lineage automatically
- **Graph** — stitch assets into a typed dependency graph (tables → queries → consumers)
- **Search** — find any asset by name, tag, or SQL fingerprint
- **Lineage** — trace upstream/downstream dependencies across sources
- **MCP server** — expose the entire graph as Model Context Protocol tools for AI agents

## Quickstart

```bash
# Install
pip install alma-atlas

# Connect a source
alma-atlas connect bigquery --project my-project

# Scan
alma-atlas scan

# Inspect status
alma-atlas status

# Start MCP server
alma-atlas serve
```

## Architecture

Alma Atlas is structured as a Python monorepo with focused packages:

| Package | Purpose |
|---|---|
| `alma-atlas` | CLI, MCP server, scan pipeline orchestration |
| `alma-atlas-store` | SQLite persistence layer |
| `alma-ports` | Protocol interfaces (zero deps) |
| `alma-connectors` | Source adapters (BigQuery, Snowflake, Postgres, dbt) |
| `alma-analysis` | Pure analysis functions (lineage, edges, consumers) |
| `alma-sqlkit` | SQL parsing and normalization utilities |
| `alma-algebrakit` | SQL algebraic fingerprinting |

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup and contribution guidelines.

## License

Apache 2.0 — see [LICENSE](LICENSE).
