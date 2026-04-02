-- 009_business_terms.sql -- Business glossary term storage
--
-- Stores named business terms with optional definition, formula,
-- and referenced column IDs. Terms can be entered manually or learned
-- by the annotator agent.

CREATE TABLE IF NOT EXISTS business_terms (
    name                TEXT PRIMARY KEY,
    definition          TEXT,
    formula             TEXT,
    referenced_columns  TEXT DEFAULT '[]',
    source              TEXT DEFAULT 'manual',
    created_at          TEXT
);
