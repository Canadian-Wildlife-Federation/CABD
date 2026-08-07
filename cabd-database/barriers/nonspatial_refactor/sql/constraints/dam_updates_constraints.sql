-- =========================
-- 1) Constraints
-- =========================

-- entry_classification domain
ALTER TABLE cabd.dam_updates
  DROP CONSTRAINT IF EXISTS dam_updates_entry_classification_check;

ALTER TABLE cabd.dam_updates
  ADD CONSTRAINT dam_updates_entry_classification_check
  CHECK (entry_classification IN ('new feature', 'modify feature', 'delete feature'));

-- update_status domain (your new desired set)
ALTER TABLE cabd.dam_updates
  DROP CONSTRAINT IF EXISTS status_check;

ALTER TABLE cabd.dam_updates
  ADD CONSTRAINT status_check
  CHECK (update_status IN ('needs review', 'ready', 'wait', 'blocked'));

-- update_type domain (keep if you still use it)
ALTER TABLE cabd.dam_updates
  DROP CONSTRAINT IF EXISTS update_type_check;

ALTER TABLE cabd.dam_updates
  ADD CONSTRAINT update_type_check
  CHECK (update_type IN ('cwf', 'user'));

-- uniqueness (matches original loader intent)
ALTER TABLE cabd.dam_updates
  DROP CONSTRAINT IF EXISTS dam_record_unique;

ALTER TABLE cabd.dam_updates
  ADD CONSTRAINT dam_updates_unique_cabd_source
  UNIQUE (cabd_id, data_source_short_name);

-- require lat/long for new features
ALTER TABLE cabd.dam_updates
  DROP CONSTRAINT IF EXISTS new_requires_latlon;

ALTER TABLE cabd.dam_updates
  ADD CONSTRAINT new_requires_latlon
  CHECK (
    entry_classification <> 'new feature'
    OR (latitude IS NOT NULL AND longitude IS NOT NULL)
  );

-- lat/long range sanity (applies when provided)
ALTER TABLE cabd.dam_updates
  DROP CONSTRAINT IF EXISTS updates_lat_range;

ALTER TABLE cabd.dam_updates
  ADD CONSTRAINT updates_lat_range
  CHECK (latitude IS NULL OR (latitude BETWEEN -90 AND 90));

ALTER TABLE cabd.dam_updates
  DROP CONSTRAINT IF EXISTS updates_lon_range;

ALTER TABLE cabd.dam_updates
  ADD CONSTRAINT updates_lon_range
  CHECK (longitude IS NULL OR (longitude BETWEEN -180 AND 180));

ALTER TABLE cabd.dam_updates rename column update_status to status;