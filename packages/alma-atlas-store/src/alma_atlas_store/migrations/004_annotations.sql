-- 004_annotations.sql — Asset annotation schema
--
-- Stores agent-generated business metadata annotations for data assets.
-- Annotations are supplementary: they never overwrite schema or edge data.

CREATE TABLE IF NOT EXISTS asset_annotations (
    asset_id               TEXT PRIMARY KEY,
    ownership              TEXT,            -- team or person responsible
    granularity            TEXT,            -- 'one row per user per day'
    join_keys              TEXT,            -- JSON array of column names
    freshness_guarantee    TEXT,            -- 'updated hourly' / 'SLA: 6h'
    business_logic_summary TEXT,            -- 1-2 sentence plain-English description
    sensitivity            TEXT,            -- 'PII', 'financial', 'public'
    annotated_at           TEXT DEFAULT CURRENT_TIMESTAMP,
    annotated_by           TEXT             -- provenance, e.g. 'agent:claude-sonnet-4-20250514'
);

CREATE INDEX IF NOT EXISTS idx_asset_annotations_annotated_at
    ON asset_annotations(annotated_at);
