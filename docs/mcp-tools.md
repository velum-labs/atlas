# MCP Tools Reference

Alma Atlas registers six MCP tools on the server started by `alma-atlas serve`. These tools expose the Atlas graph to any MCP-compatible AI agent.

All tools require a populated database. Run `alma-atlas scan` before connecting an agent.

---

## atlas_search

Search for data assets by name, ID, or keyword.

**Input schema**

```json
{
  "type": "object",
  "properties": {
    "query": {
      "type": "string",
      "description": "Search term"
    },
    "limit": {
      "type": "integer",
      "description": "Maximum number of results",
      "default": 20
    }
  },
  "required": ["query"]
}
```

**Example input**

```json
{
  "query": "orders",
  "limit": 5
}
```

**Example output**

```
Found 3 asset(s) matching 'orders':

  bigquery:my-project.analytics.orders  [TABLE]  source=bigquery:my-project
  bigquery:my-project.analytics.orders_daily  [VIEW]  source=bigquery:my-project  Daily order aggregation
  dbt:my-project.fct_orders  [TABLE]  source=dbt:my-project  Fact table for order events
```

---

## atlas_get_asset

Retrieve full details for a specific data asset by its ID.

**Input schema**

```json
{
  "type": "object",
  "properties": {
    "asset_id": {
      "type": "string",
      "description": "Fully-qualified asset ID"
    }
  },
  "required": ["asset_id"]
}
```

**Example input**

```json
{
  "asset_id": "bigquery:my-project.analytics.orders"
}
```

**Example output**

```json
{
  "id": "bigquery:my-project.analytics.orders",
  "source": "bigquery:my-project",
  "kind": "TABLE",
  "name": "orders",
  "description": null,
  "tags": [],
  "metadata": {
    "project": "my-project",
    "dataset": "analytics",
    "table": "orders",
    "row_count": 4821043
  },
  "first_seen": "2024-11-01T09:00:00",
  "last_seen": "2025-03-20T14:32:00"
}
```

---

## atlas_lineage

Trace upstream or downstream lineage for a data asset.

**Input schema**

```json
{
  "type": "object",
  "properties": {
    "asset_id": {
      "type": "string",
      "description": "Asset ID to trace from"
    },
    "direction": {
      "type": "string",
      "enum": ["upstream", "downstream"],
      "description": "Direction of traversal"
    },
    "depth": {
      "type": "integer",
      "description": "Maximum traversal depth (omit for unlimited)"
    }
  },
  "required": ["asset_id", "direction"]
}
```

**Example input — upstream**

```json
{
  "asset_id": "dbt:my-project.fct_orders",
  "direction": "upstream",
  "depth": 3
}
```

**Example output**

```
Upstream lineage for dbt:my-project.fct_orders (4 nodes):
  bigquery:my-project.raw.orders
  bigquery:my-project.raw.customers
  dbt:my-project.stg_orders
  dbt:my-project.stg_customers
```

**Example input — downstream**

```json
{
  "asset_id": "bigquery:my-project.analytics.orders",
  "direction": "downstream"
}
```

**Example output**

```
Downstream lineage for bigquery:my-project.analytics.orders (2 nodes):
  bigquery:my-project.analytics.orders_daily
  dbt:my-project.fct_orders
```

---

## atlas_status

Return a summary of the Atlas graph: total assets, edges, query fingerprints, and counts by kind and source.

**Input schema**

```json
{
  "type": "object",
  "properties": {}
}
```

No inputs required.

**Example output**

```
Atlas graph: 196 assets, 130 edges, 34 query fingerprints

Assets by kind:
  TABLE: 98
  VIEW: 44
  MATERIALIZED_VIEW: 8
  SEED: 4
  SNAPSHOT: 2

Assets by source:
  bigquery:my-project: 142
  dbt:my-project: 54
```

---

## atlas_get_schema

Get the latest schema snapshot for a data asset — column names, types, and nullability.

**Input schema**

```json
{
  "type": "object",
  "properties": {
    "asset_id": {
      "type": "string",
      "description": "Asset ID to get schema for"
    }
  },
  "required": ["asset_id"]
}
```

**Example input**

```json
{
  "asset_id": "bigquery:my-project.analytics.orders"
}
```

**Example output**

```
Schema for bigquery:my-project.analytics.orders (captured 2025-03-20T14:32:00):

  order_id          STRING    NOT NULL
  customer_id       STRING    NOT NULL
  order_date        DATE      NOT NULL
  status            STRING    NULL
  total_amount      NUMERIC   NULL
  created_at        TIMESTAMP NOT NULL
  updated_at        TIMESTAMP NULL
```

---

## atlas_impact

Analyse the downstream impact of changes to an asset — returns all assets that depend on it, directly or transitively.

**Input schema**

```json
{
  "type": "object",
  "properties": {
    "asset_id": {
      "type": "string",
      "description": "Asset ID to analyse impact for"
    },
    "depth": {
      "type": "integer",
      "description": "Maximum depth of impact analysis"
    }
  },
  "required": ["asset_id"]
}
```

**Example input**

```json
{
  "asset_id": "bigquery:my-project.raw.orders"
}
```

**Example output**

```
Impact analysis for bigquery:my-project.raw.orders:
  5 downstream asset(s) would be affected by changes:

  bigquery:my-project.analytics.orders
  bigquery:my-project.analytics.orders_daily
  dbt:my-project.stg_orders
  dbt:my-project.fct_orders
  dbt:my-project.rpt_revenue

Recommendation: Review these 5 downstream assets before making changes to bigquery:my-project.raw.orders.
```

---

## Asset ID format

Asset IDs follow the pattern `{source_id}.{schema}.{object}`, where `source_id` is the identifier assigned during `alma-atlas connect`. Use `atlas_search` to discover IDs if you don't know them.

Examples:

| Source | Asset ID |
|--------|----------|
| BigQuery | `bigquery:my-project.analytics.orders` |
| PostgreSQL | `postgres:mydb.public.users` |
| dbt | `dbt:my-project.fct_orders` |
