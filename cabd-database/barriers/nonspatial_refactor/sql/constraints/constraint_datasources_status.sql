ALTER TABLE IF EXISTS cabd.data_source_updates
    ADD COLUMN IF NOT EXISTS sharepoint_status text;

ALTER TABLE IF EXISTS cabd.data_source_updates
    ADD COLUMN IF NOT EXISTS processing_status text;

ALTER TABLE IF EXISTS cabd.data_source_updates
    DROP CONSTRAINT IF EXISTS status_check;

ALTER TABLE IF EXISTS cabd.data_source_updates
    DROP CONSTRAINT IF EXISTS data_source_updates_processing_status_check;

ALTER TABLE IF EXISTS cabd.data_source_updates
    ADD CONSTRAINT data_source_updates_processing_status_check
    CHECK (processing_status IN ('needs review', 'ready', 'wait', 'blocked', 'done'));