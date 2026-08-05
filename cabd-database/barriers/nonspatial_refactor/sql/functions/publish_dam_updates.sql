-- Minimal v1 publish: resolves datasource IDs, applies delete/new/modify geometry basics,
-- and marks rows done. Extend column-by-column mapping incrementally.


-- Publish dams from a staging table into live tables.
-- Deletes ONLY successfully published rows from staging.
-- Keeps unresolved rows (e.g., missing data_source) in staging for later reprocessing.

CREATE OR REPLACE FUNCTION cabd.publish_dam_updates(staging_table regclass)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  deleted_from_staging integer := 0;
BEGIN
  -- Ensure required columns exist
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS data_source uuid', staging_table);
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS blocked_reason text', staging_table);
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS blocked_at timestamptz', staging_table);

  -- Snapshot ready rows at start of run
  CREATE TEMP TABLE _dam_ready (id integer PRIMARY KEY) ON COMMIT DROP;
  EXECUTE format($q$
    INSERT INTO _dam_ready (id)
    SELECT id
    FROM %s
    WHERE update_status = 'ready'
  $q$, staging_table);

  IF NOT EXISTS (SELECT 1 FROM _dam_ready) THEN
    RETURN 0;
  END IF;

  -- Resolve data sources for this batch only
  EXECUTE format($q$
    UPDATE %s u
    SET data_source = ds.id
    FROM cabd.data_source ds
    WHERE u.id IN (SELECT id FROM _dam_ready)
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
  
  -- Publishable = ready AND data_source resolved
  CREATE TEMP TABLE _dam_publishable (id integer PRIMARY KEY) ON COMMIT DROP;
  EXECUTE format($q$
    INSERT INTO _dam_publishable (id)
    SELECT u.id
    FROM %s u
    JOIN _dam_ready r ON r.id = u.id
    WHERE u.update_status = 'ready'
      AND u.data_source IS NOT NULL
  $q$, staging_table);

  IF NOT EXISTS (SELECT 1 FROM _dam_publishable) THEN
    RETURN 0;
  END IF;

  ---------------------------------------------------------------------------
  -- DELETE FEATURES (publishable only)
  -- Also unlink fishways that reference a dam being deleted.
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    UPDATE fishways.fishways
    SET dam_id = NULL
    WHERE dam_id IN (
      SELECT u.cabd_id
      FROM %s u
      JOIN _dam_publishable p ON p.id = u.id
      WHERE u.entry_classification = 'delete feature'
    )
  $q$, staging_table);

  EXECUTE format($q$
    DELETE FROM dams.dams_attribute_source
    WHERE cabd_id IN (
      SELECT u.cabd_id
      FROM %s u
      JOIN _dam_publishable p ON p.id = u.id
      WHERE u.entry_classification = 'delete feature'
    )
  $q$, staging_table);

  EXECUTE format($q$
    DELETE FROM dams.dams_feature_source
    WHERE cabd_id IN (
      SELECT u.cabd_id
      FROM %s u
      JOIN _dam_publishable p ON p.id = u.id
      WHERE u.entry_classification = 'delete feature'
    )
  $q$, staging_table);

  EXECUTE format($q$
    DELETE FROM dams.dams
    WHERE cabd_id IN (
      SELECT u.cabd_id
      FROM %s u
      JOIN _dam_publishable p ON p.id = u.id
      WHERE u.entry_classification = 'delete feature'
    )
  $q$, staging_table);

  ---------------------------------------------------------------------------
  -- INSERT NEW FEATURES (geometry only)
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    INSERT INTO dams.dams (cabd_id, province_territory_code, original_point, snapped_point)
    SELECT
      u.cabd_id,
      u.province_territory_code,
      ST_SetSRID(ST_MakePoint(u.longitude::float, u.latitude::float), 4617),
      ST_SetSRID(ST_MakePoint(u.longitude::float, u.latitude::float), 4617)
    FROM %s u
    JOIN _dam_publishable p ON p.id = u.id
    WHERE u.entry_classification = 'new feature'
      AND u.latitude IS NOT NULL
      AND u.longitude IS NOT NULL
    ON CONFLICT (cabd_id) DO NOTHING
  $q$, staging_table);

  ---------------------------------------------------------------------------
  -- ENSURE SOURCE TRACKING ROWS EXIST
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    INSERT INTO dams.dams_attribute_source (cabd_id)
    SELECT DISTINCT u.cabd_id
    FROM %s u
    JOIN _dam_publishable p ON p.id = u.id
    WHERE u.entry_classification IN ('new feature', 'modify feature')
    ON CONFLICT (cabd_id) DO NOTHING
  $q$, staging_table);

  EXECUTE format($q$
    INSERT INTO dams.dams_feature_source (cabd_id, datasource_id)
    SELECT DISTINCT u.cabd_id, u.data_source
    FROM %s u
    JOIN _dam_publishable p ON p.id = u.id
    WHERE u.entry_classification IN ('new feature', 'modify feature')
      AND u.data_source IS NOT NULL
    ON CONFLICT DO NOTHING
  $q$, staging_table);

  ---------------------------------------------------------------------------
  -- MODIFY FEATURES: MOVE GEOMETRY IF PROVIDED
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    UPDATE dams.dams d
    SET
      original_point = ST_SetSRID(ST_MakePoint(u.longitude::float, u.latitude::float), 4617),
      snapped_point  = ST_SetSRID(ST_MakePoint(u.longitude::float, u.latitude::float), 4617)
    FROM %s u
    JOIN _dam_publishable p ON p.id = u.id
    WHERE u.entry_classification = 'modify feature'
      AND u.latitude IS NOT NULL
      AND u.longitude IS NOT NULL
      AND d.cabd_id = u.cabd_id
  $q$, staging_table);

  ---------------------------------------------------------------------------
  -- DELETE successfully published rows from staging (publishable only)
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    DELETE FROM %s u
    USING _dam_publishable p
    WHERE u.id = p.id
  $q$, staging_table);

  GET DIAGNOSTICS deleted_from_staging = ROW_COUNT;
  RETURN deleted_from_staging;
END;
$$;