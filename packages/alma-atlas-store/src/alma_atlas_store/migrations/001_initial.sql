-- 001_initial.sql — Initial schema for Alma Atlas store
--
-- Creates the core tables for assets, edges, schemas, queries,
-- consumers, and contracts.

CREATE TABLE IF NOT EXISTS assets (
    id          TEXT PRIMARY KEY,          -- Fully-qualified asset ID (e.g. project.dataset.table)
    source      TEXT NOT NULL,             -- Source connector type (bigquery, snowflake, postgres, dbt)
    kind        TEXT NOT NULL,             -- Asset kind (table, view, model, dashboard, etc.)
    name        TEXT NOT NULL,             -- Short display name
    description TEXT,
    tags        TEXT,                      -- JSON array of tag strings
    metadata    TEXT,                      -- JSON blob for connector-specific metadata
    first_seen  TEXT DEFAULT CURRENT_TIMESTAMP,
    last_seen   TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS edges (
    id          TEXT PRIMARY KEY,          -- upstream_id:downstream_id:kind
    upstream_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    downstream_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    kind        TEXT NOT NULL,             -- Edge kind: reads, writes, depends_on, etc.
    metadata    TEXT,                      -- JSON blob
    first_seen  TEXT DEFAULT CURRENT_TIMESTAMP,
    last_seen   TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(upstream_id, downstream_id, kind)
);

CREATE TABLE IF NOT EXISTS schema_snapshots (
    id          TEXT PRIMARY KEY,          -- asset_id:snapshot_hash
    asset_id    TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    columns     TEXT NOT NULL,             -- JSON array of {name, type, nullable, description}
    fingerprint TEXT NOT NULL,             -- Hash of column names+types for drift detection
    captured_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS queries (
    fingerprint TEXT PRIMARY KEY,          -- SQL fingerprint from alma-algebrakit
    sql_text    TEXT NOT NULL,             -- Representative (normalized) SQL
    tables      TEXT NOT NULL,             -- JSON array of referenced asset IDs
    source      TEXT NOT NULL,             -- Source connector type
    first_seen  TEXT DEFAULT CURRENT_TIMESTAMP,
    last_seen   TEXT DEFAULT CURRENT_TIMESTAMP,
    execution_count INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS consumers (
    id          TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,             -- Consumer kind: user, service, dashboard, notebook
    name        TEXT NOT NULL,
    source      TEXT NOT NULL,
    metadata    TEXT,
    first_seen  TEXT DEFAULT CURRENT_TIMESTAMP,
    last_seen   TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS consumer_assets (
    consumer_id TEXT NOT NULL REFERENCES consumers(id) ON DELETE CASCADE,
    asset_id    TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    PRIMARY KEY (consumer_id, asset_id)
);

CREATE TABLE IF NOT EXISTS contracts (
    id          TEXT PRIMARY KEY,
    asset_id    TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    version     TEXT NOT NULL,
    spec        TEXT NOT NULL,             -- JSON blob: columns, sla, owner, etc.
    status      TEXT NOT NULL DEFAULT 'draft',  -- draft, active, deprecated
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at  TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_edges_upstream   ON edges(upstream_id);
CREATE INDEX IF NOT EXISTS idx_edges_downstream ON edges(downstream_id);
CREATE INDEX IF NOT EXISTS idx_schema_asset     ON schema_snapshots(asset_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_contract_asset   ON contracts(asset_id);
