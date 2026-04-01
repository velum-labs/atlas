# MCP Tools Reference

Atlas registers the MCP tool set defined in `alma_atlas.mcp.tools` when you run `alma-atlas serve`.

All tools require a populated local Atlas database. Run `alma-atlas scan` first.

## Tool Catalog

This page is a usage guide. The authoritative tool catalog and input schemas are code-owned in `alma_atlas.mcp.tools`.

| Tool | Input | Description |
|------|-------|-------------|
| `atlas_search` | `query`, optional `limit` | Search assets by ID, name, or keyword |
| `atlas_get_asset` | `asset_id` | Return one asset as JSON |
| `atlas_get_annotations` | optional `asset_id`, optional `limit` | Return learned business annotations |
| `atlas_lineage` | `asset_id`, `direction`, optional `depth` | Traverse upstream or downstream lineage |
| `atlas_status` | none | Summarize assets, edges, and query fingerprints |
| `atlas_get_schema` | `asset_id` | Return the latest schema snapshot |
| `atlas_impact` | `asset_id`, optional `depth` | Show downstream blast radius |
| `atlas_get_query_patterns` | optional `top_n` | Show top stored query fingerprints |
| `atlas_suggest_tables` | `query`, optional `limit` | Rank likely tables for a search intent |
| `atlas_check_contract` | `asset_id` | Validate one asset against stored contracts |
| `atlas_list_violations` | optional `asset_id`, optional `limit` | Show recent unresolved violations |
| `atlas_team_sync` | none | Push local graph state and pull team contracts |

## Asset IDs

Atlas asset IDs use the canonical form `{source_id}::{object_ref}`.

Examples:

| Source | Asset ID |
|--------|----------|
| BigQuery | `bigquery:my-project::analytics.orders` |
| PostgreSQL | `postgres:customer:public::public.users` |
| dbt | `dbt:analytics::marts.fct_orders` |
| Looker | `looker:bi-example::ecommerce.orders` |

Use `atlas_search` if you do not know the exact ID.

## Common Examples

### Search

```json
{
  "query": "orders",
  "limit": 5
}
```

### Get One Asset

```json
{
  "asset_id": "bigquery:my-project::analytics.orders"
}
```

### Lineage

```json
{
  "asset_id": "dbt:analytics::marts.fct_orders",
  "direction": "upstream",
  "depth": 3
}
```

### Schema

```json
{
  "asset_id": "postgres:customer:public::public.users"
}
```

### Query Patterns

```json
{
  "top_n": 20
}
```

### Contract Check

```json
{
  "asset_id": "dbt:analytics::marts.fct_orders"
}
```

### Violations

```json
{
  "limit": 50
}
```

## Notes

- `atlas_status` is the MCP graph summary. The CLI `alma-atlas status` is a separate command with its own display format.
- `atlas_team_sync` reports sync results for assets, edges, contracts, and violations; it does not make schema snapshots, query fingerprints, or annotations globally shared.
- `atlas_get_asset` returns the asset record as stored in SQLite. That means the exact metadata keys vary by connector and by what the current scan path projected.
