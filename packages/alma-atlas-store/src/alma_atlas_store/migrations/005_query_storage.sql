-- 005_query_storage.sql — Query storage normalization and read-path indexes
--
-- Adds a normalized query-to-asset mapping table for indexed lookups and
-- supporting indexes for common query/violation access patterns.

CREATE TABLE IF NOT EXISTS query_tables (
    fingerprint TEXT NOT NULL REFERENCES queries(fingerprint) ON DELETE CASCADE,
    asset_id    TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    PRIMARY KEY (fingerprint, asset_id)
);

INSERT OR IGNORE INTO query_tables (fingerprint, asset_id)
SELECT q.fingerprint, jt.value
FROM queries AS q
JOIN json_each(q.tables) AS jt
JOIN assets AS a ON a.id = jt.value
WHERE json_valid(q.tables);

CREATE INDEX IF NOT EXISTS idx_query_tables_asset_id
    ON query_tables(asset_id);

CREATE INDEX IF NOT EXISTS idx_queries_execution_count
    ON queries(execution_count DESC);

CREATE INDEX IF NOT EXISTS idx_queries_last_seen
    ON queries(last_seen);

CREATE INDEX IF NOT EXISTS idx_violations_recent_unresolved
    ON violations(detected_at DESC)
    WHERE resolved_at IS NULL;
