# Show HN: Alma Atlas — open-source data intelligence for AI coding tools

---

**Title:** Show HN: Alma Atlas – open-source MCP server that gives AI agents live knowledge of your data stack

---

AI coding tools have a data problem. Cursor, Claude Code, and Copilot can read your SQL files, but they have no idea what your warehouse actually looks like right now. They don't know which columns exist, how data flows from source to model to dashboard, or what breaks downstream if you rename a field. So they write plausible-looking SQL that references the wrong table, joins on a deprecated column, or silently invalidates a dashboard three hops away.

Atlas fixes this. It's a CLI + MCP server that:

1. Connects to your data sources (BigQuery, Snowflake, Postgres, dbt)
2. Scans schemas, query traffic, and lineage
3. Stores everything in a local SQLite graph
4. Exposes that graph as MCP tools your AI agent calls in real time

When your agent is writing a SQL migration, it can call `atlas_get_schema` to see live column types, `atlas_lineage` to trace what feeds the table, and `atlas_impact` to see every downstream model that would break. All from your machine, no signup, no SaaS.

**Install and try it:**

```bash
pip install alma-atlas
alma-atlas connect bigquery --project my-project  # or snowflake, postgres, dbt
alma-atlas scan
alma-atlas serve
```

Then add this to your IDE's MCP config:

```json
{ "mcpServers": { "atlas": { "command": "alma-atlas", "args": ["serve"] } } }
```

**What it exposes (6 MCP tools):**

- `atlas_search` — find any asset by name or keyword
- `atlas_get_asset` — full metadata: kind, row count, tags, first/last seen
- `atlas_get_schema` — live column names and types
- `atlas_lineage` — upstream/downstream graph traversal
- `atlas_impact` — blast radius before a change
- `atlas_status` — graph summary

**What it doesn't do (yet):** Airflow/Looker adapters are on the roadmap but not shipped. The local store is SQLite — it's fast enough for single-engineer use but we haven't tested it at 10k+ asset scale. Column-level lineage is available for BigQuery and Snowflake via query log parsing; Postgres lineage comes from view dependencies and `pg_stat_statements`.

**Tech notes:** It's a Python monorepo (uv, Python 3.12+). The MCP server runs stdio transport for IDE integration or HTTP for remote/shared access. The lineage inference engine parses query logs with a dialect-agnostic SQL parser and scores inferred connections by confidence (0.3–1.0). The graph schema is the same one running in Alma's production observability platform, just swapped onto SQLite for local use.

Apache 2.0. GitHub: https://github.com/almaos/atlas

Feedback welcome, especially from people who've built similar things or tried to wrangle lineage across multi-system stacks.
