-- 003_enforcement.sql — Enforcement engine schema
--
-- Adds enforcement mode to contracts and creates the violations table
-- for storing drift detection results.

ALTER TABLE contracts ADD COLUMN mode TEXT NOT NULL DEFAULT 'shadow';

CREATE TABLE IF NOT EXISTS violations (
    id             TEXT PRIMARY KEY,    -- uuid
    asset_id       TEXT NOT NULL,
    violation_type TEXT NOT NULL,       -- added_column, removed_column, type_changed, table_dropped, row_count_anomaly
    severity       TEXT NOT NULL,       -- info, warning, error
    details        TEXT NOT NULL,       -- JSON blob with human-readable description and specifics
    detected_at    TEXT DEFAULT CURRENT_TIMESTAMP,
    resolved_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_violations_asset ON violations(asset_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_violations_unresolved ON violations(asset_id) WHERE resolved_at IS NULL;
