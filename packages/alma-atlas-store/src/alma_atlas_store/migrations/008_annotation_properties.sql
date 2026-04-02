-- 006_annotation_properties.sql -- Extensible properties column for asset annotations
--
-- Adds a JSON TEXT column to store open-ended annotation properties such as
-- column_notes and notes produced by the annotator agent.

ALTER TABLE asset_annotations ADD COLUMN properties TEXT DEFAULT '{}';
