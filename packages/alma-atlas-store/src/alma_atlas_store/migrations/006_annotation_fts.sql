-- 006_annotation_fts.sql — FTS5 full-text search index over annotations

CREATE VIRTUAL TABLE IF NOT EXISTS annotation_fts USING fts5(
    asset_id UNINDEXED,
    content,
    tokenize='porter unicode61'
);
