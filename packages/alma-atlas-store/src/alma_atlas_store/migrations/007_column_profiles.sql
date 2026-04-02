-- 007_column_profiles.sql -- Column profiling statistics per asset column
--
-- Stores per-column distribution stats gathered during scan: distinct counts,
-- null rates, min/max, top values (low-cardinality), and sample values.

CREATE TABLE IF NOT EXISTS column_profiles (
    asset_id      TEXT    NOT NULL,
    column_name   TEXT    NOT NULL,
    distinct_count INTEGER,
    null_count    INTEGER,
    null_fraction REAL,
    min_value     TEXT,
    max_value     TEXT,
    top_values    TEXT    NOT NULL DEFAULT '[]',
    sample_values TEXT    NOT NULL DEFAULT '[]',
    profiled_at   TEXT,
    PRIMARY KEY (asset_id, column_name)
);
