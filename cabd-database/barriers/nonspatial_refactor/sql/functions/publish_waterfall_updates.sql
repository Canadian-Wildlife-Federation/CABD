-- Publish waterfalls from a staging table into live tables.
-- Pattern:
--   1) snapshot the "ready" rows to process (by staging PK id)
--   2) apply all changes using only that snapshot
--   3) delete successfully processed rows from staging
--
-- Assumptions about staging table:
--   - Has integer PK column named "id" (as per your current cabd.waterfall_updates loader)
--   - Has cabd_id (uuid), entry_classification, update_status, latitude/longitude, data_source_short_name, etc.
--
-- NOTE: This is a "minimal mapping" publish (geometry + source tables + deletes).
--       Extend it with attribute/provenance mapping as you port over logic from map_waterfall_updates.py.

CREATE OR REPLACE FUNCTION cabd.publish_waterfall_updates(staging_table regclass)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  deleted_from_staging integer := 0;
BEGIN
  -- Ensure required columns exist (safe no-ops if they already do)
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS data_source uuid', staging_table);
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS blocked_reason text', staging_table);
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS blocked_at timestamptz', staging_table);

  -- Snapshot "ready" rows at start of run
  CREATE TEMP TABLE _wf_ready (id integer PRIMARY KEY) ON COMMIT DROP;
  EXECUTE format($q$
    INSERT INTO _wf_ready (id)
    SELECT id
    FROM %s
    WHERE update_status = 'ready'
  $q$, staging_table);

  IF NOT EXISTS (SELECT 1 FROM _wf_ready) THEN
    RETURN 0;
  END IF;

  -- Resolve data sources for just these ready rows
  EXECUTE format($q$
    UPDATE %s u
    SET data_source = ds.id
    FROM cabd.data_source ds
    WHERE u.id IN (SELECT id FROM _wf_ready)
      AND ds.name = u.data_source_short_name
  $q$, staging_table);

  -- Best-effort normalize submitted_on (ignore failures)
  BEGIN
    EXECUTE format('ALTER TABLE %s ALTER COLUMN submitted_on TYPE timestamptz USING submitted_on::timestamptz', staging_table);
  EXCEPTION WHEN others THEN
    NULL;
  END;

  -- BLOCK rows that have a missing referenced data source
  BEGIN
  EXECUTE format($q$
    UPDATE %s
    SET
      update_status = 'blocked',
      blocked_reason = 'missing data source in cabd.data_source',
      blocked_at = now()
    WHERE data_source is null
  $q$, staging_table);
  END;

  -- Identify publishable rows: still ready AND data_source resolved
  CREATE TEMP TABLE _wf_publishable (id integer PRIMARY KEY) ON COMMIT DROP;
  EXECUTE format($q$
    INSERT INTO _wf_publishable (id)
    SELECT u.id
    FROM %s u
    JOIN _wf_ready r ON r.id = u.id
    WHERE u.update_status = 'ready'
      AND u.data_source IS NOT NULL
  $q$, staging_table);

  -- If nothing publishable, do nothing (keep unresolved rows in staging)
  IF NOT EXISTS (SELECT 1 FROM _wf_publishable) THEN
    RETURN 0;
  END IF;

  ---------------------------------------------------------------------------
  -- DELETE FEATURES (publishable only)
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    DELETE FROM waterfalls.waterfalls_attribute_source
    WHERE cabd_id IN (
      SELECT u.cabd_id
      FROM %s u
      JOIN _wf_publishable p ON p.id = u.id
      WHERE u.entry_classification = 'delete feature'
    )
  $q$, staging_table);

  EXECUTE format($q$
    DELETE FROM waterfalls.waterfalls_feature_source
    WHERE cabd_id IN (
      SELECT u.cabd_id
      FROM %s u
      JOIN _wf_publishable p ON p.id = u.id
      WHERE u.entry_classification = 'delete feature'
    )
  $q$, staging_table);

  EXECUTE format($q$
    DELETE FROM waterfalls.waterfalls
    WHERE cabd_id IN (
      SELECT u.cabd_id
      FROM %s u
      JOIN _wf_publishable p ON p.id = u.id
      WHERE u.entry_classification = 'delete feature'
    )
  $q$, staging_table);

  ---------------------------------------------------------------------------
  -- INSERT NEW FEATURES (publishable only)
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    INSERT INTO waterfalls.waterfalls (cabd_id, province_territory_code, original_point, snapped_point)
    SELECT
      u.cabd_id,
      u.province_territory_code,
      ST_SetSRID(ST_MakePoint(u.longitude::float, u.latitude::float), 4617),
      ST_SetSRID(ST_MakePoint(u.longitude::float, u.latitude::float), 4617)
    FROM %s u
    JOIN _wf_publishable p ON p.id = u.id
    WHERE u.entry_classification = 'new feature'
      AND u.latitude IS NOT NULL
      AND u.longitude IS NOT NULL
    ON CONFLICT (cabd_id) DO NOTHING
  $q$, staging_table);

  ---------------------------------------------------------------------------
  -- ENSURE SOURCE TRACKING ROWS EXIST (publishable only)
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    INSERT INTO waterfalls.waterfalls_attribute_source (cabd_id)
    SELECT DISTINCT u.cabd_id
    FROM %s u
    JOIN _wf_publishable p ON p.id = u.id
    WHERE u.entry_classification IN ('new feature', 'modify feature')
    ON CONFLICT (cabd_id) DO NOTHING
  $q$, staging_table);

  EXECUTE format($q$
    INSERT INTO waterfalls.waterfalls_feature_source (cabd_id, datasource_id)
    SELECT DISTINCT u.cabd_id, u.data_source
    FROM %s u
    JOIN _wf_publishable p ON p.id = u.id
    WHERE u.entry_classification IN ('new feature', 'modify feature')
      AND u.data_source IS NOT NULL
    ON CONFLICT DO NOTHING
  $q$, staging_table);

  ---------------------------------------------------------------------------
  -- MODIFY FEATURES: MOVE GEOMETRY IF PROVIDED (publishable only)
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    UPDATE waterfalls.waterfalls w
    SET
      original_point = ST_SetSRID(ST_MakePoint(u.longitude::float, u.latitude::float), 4617),
      snapped_point  = ST_SetSRID(ST_MakePoint(u.longitude::float, u.latitude::float), 4617)
    FROM %s u
    JOIN _wf_publishable p ON p.id = u.id
    WHERE u.entry_classification = 'modify feature'
      AND u.latitude IS NOT NULL
      AND u.longitude IS NOT NULL
      AND w.cabd_id = u.cabd_id
  $q$, staging_table);

  ---------------------------------------------------------------------------
  -- Delete ONLY successfully published rows from staging (publishable only)
  -- Leave rows missing data_source in staging for later reprocessing.
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    DELETE FROM %s u
    USING _wf_publishable p
    WHERE u.id = p.id
  $q$, staging_table);

  GET DIAGNOSTICS deleted_from_staging = ROW_COUNT;
  RETURN deleted_from_staging;
END;
$$;