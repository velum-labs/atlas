-- 002_v2_extractions.sql — Schema for v2 capability extraction results
--
-- Stores serialised payloads from each SourceAdapterV2 extraction run so that
-- the full provenance of every capability result is available for replay,
-- auditing, and incremental cursors.

CREATE TABLE IF NOT EXISTS v2_extraction_results (
    id           TEXT PRIMARY KEY,    -- {adapter_key}:{capability}:{captured_at}
    adapter_key  TEXT NOT NULL,
    adapter_kind TEXT NOT NULL,
    capability   TEXT NOT NULL,       -- AdapterCapability value
    scope        TEXT NOT NULL,       -- ExtractionScope value
    captured_at  TEXT NOT NULL,
    duration_ms  REAL NOT NULL,
    row_count    INTEGER NOT NULL,
    payload      TEXT NOT NULL,       -- JSON-serialised result (dataclasses.asdict)
    stored_at    TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Lineage edges are stored separately from the FK-constrained `edges` table
-- because their endpoints are fully-qualified object names, not asset IDs.
CREATE TABLE IF NOT EXISTS v2_lineage_edges (
    id              TEXT PRIMARY KEY,  -- {adapter_key}:{source_object}:{target_object}:{edge_kind}
    adapter_key     TEXT NOT NULL,
    source_object   TEXT NOT NULL,
    target_object   TEXT NOT NULL,
    edge_kind       TEXT NOT NULL,     -- LineageEdgeKind value
    confidence      REAL NOT NULL,
    metadata        TEXT,              -- JSON blob
    captured_at     TEXT NOT NULL,
    stored_at       TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_v2_extractions_adapter_cap
    ON v2_extraction_results(adapter_key, capability, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_v2_lineage_source
    ON v2_lineage_edges(adapter_key, source_object);
CREATE INDEX IF NOT EXISTS idx_v2_lineage_target
    ON v2_lineage_edges(adapter_key, target_object);
