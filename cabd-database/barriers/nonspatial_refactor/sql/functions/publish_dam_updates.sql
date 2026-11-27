-- Publish dams from a staging table into live tables.
--
-- To be called using the publish.py script rather than directly from SQL.
--
-- Column-driven publish: the mapped attribute list (attr_columns) and the
-- provenance list (ds_columns) are derived from the dams YAML column_types in
-- Python and passed in as text[]. The function assembles the dynamic
-- attribute UPDATE and the dams_attribute_source (_dsid) provenance UPDATE from
-- those arrays.
--
--   attr_columns : all publishable attribute columns (INCLUDES use_analysis)
--   ds_columns   : attr_columns minus use_analysis (team-determined, no _dsid)
--
-- Blocking rules live in cabd.check_dam_updates (shared with the
-- `cabd-updates check` command). Apply that function before this one.
--
-- Deletes ONLY successfully published rows from staging.
-- Keeps blocked rows in staging; their blocks are recomputed on every run.

DROP FUNCTION IF EXISTS cabd.publish_dam_updates(regclass, text[], text[]);

CREATE OR REPLACE FUNCTION cabd.publish_dam_updates(
  staging_table  regclass,
  attr_columns   text[],
  ds_columns     text[]
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  deleted_from_staging integer := 0;
  attr_set_clause      text;
  ds_set_clause        text;
BEGIN
  -- Ensure required columns exist
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS data_source uuid', staging_table);
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS blocked_reason text', staging_table);
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS blocked_at timestamptz', staging_table);
  -- Issue codes from cabd.check_dam_updates; marks blocks set by this function.
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS blocked_check text', staging_table);

  -- Normalize submission timestamps before checking for ordered conflicts.
  BEGIN
    EXECUTE format('ALTER TABLE %s ALTER COLUMN submitted_on TYPE timestamptz USING submitted_on::timestamptz', staging_table);
  EXCEPTION WHEN others THEN
    NULL;
  END;

  ---------------------------------------------------------------------------
  -- RETRY: clear blocks set by earlier publish runs so every row is checked
  -- again. Rows that still have a problem are re-blocked below. Only ready
  -- rows are ever blocked, so resetting to 'ready' restores the prior status.
  -- The blocked_reason match handles rows blocked before blocked_check existed.
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    UPDATE %s
    SET processing_status = 'ready',
        blocked_reason = NULL,
        blocked_at = NULL,
        blocked_check = NULL
    WHERE processing_status = 'blocked'
      AND (
        blocked_check IS NOT NULL
        OR blocked_reason = 'missing data source in cabd.data_source'
      )
  $q$, staging_table);

  ---------------------------------------------------------------------------
  -- BLOCK ready rows that fail any check in cabd.check_dam_updates.
  -- Data sources were upserted before this function runs, so only
  -- cabd.data_source counts (include_staged_sources = false).
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    UPDATE %1$s u
    SET processing_status = 'blocked',
        blocked_reason    = c.reasons,
        blocked_check     = c.codes,
        blocked_at        = now()
    FROM (
      SELECT
        id,
        string_agg(issue, '; ' ORDER BY issue_code)      AS reasons,
        string_agg(issue_code, ',' ORDER BY issue_code) AS codes
      FROM cabd.check_dam_updates(%1$L::regclass, false)
      WHERE severity = 'block'
      GROUP BY id
    ) c
    WHERE u.id = c.id
      AND u.processing_status = 'ready'
  $q$, staging_table);

  -- Snapshot ready rows at start of run
  CREATE TEMP TABLE _dam_ready (id integer PRIMARY KEY) ON COMMIT DROP;
  EXECUTE format($q$
    INSERT INTO _dam_ready (id)
    SELECT id
    FROM %s
    WHERE processing_status = 'ready'
  $q$, staging_table);

  IF NOT EXISTS (SELECT 1 FROM _dam_ready) THEN
    RETURN 0;
  END IF;

  -- Resolve data sources for this batch only (case-insensitive name match)
  EXECUTE format($q$
    UPDATE %s u
    SET data_source = ds.id
    FROM cabd.data_source ds
    WHERE u.id IN (SELECT id FROM _dam_ready)
      AND lower(ds.name) = lower(btrim(u.data_source_short_name))
  $q$, staging_table);

  ---------------------------------------------------------------------------
  -- PROCESS 1: one modify/delete update at a time per feature.
  -- Keep the earliest (by submitted_on); defer the rest to 'wait' so later
  -- updates run after the current change. Deferred rows are excluded from
  -- _dam_publishable and restored to 'ready' at the end of the run.
  -- (cabd.check_dam_updates reports these as 'deferred' info rows.)
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    WITH cte AS (
      SELECT id,
             row_number() OVER (PARTITION BY cabd_id ORDER BY submitted_on ASC) AS rn
      FROM %s
      WHERE processing_status = 'ready'
        AND entry_classification IN ('modify feature', 'delete feature')
    )
    UPDATE %s
    SET processing_status = 'wait'
    WHERE id IN (SELECT id FROM cte WHERE rn > 1)
  $q$, staging_table, staging_table);

  -- Publishable = ready, plus a resolved data source for new/modify rows.
  -- Deletes write no source records, so they don't need one. The NOT NULL
  -- guard is a safety net; the checks above already block these rows.
  CREATE TEMP TABLE _dam_publishable (id integer PRIMARY KEY) ON COMMIT DROP;
  EXECUTE format($q$
    INSERT INTO _dam_publishable (id)
    SELECT u.id
    FROM %s u
    JOIN _dam_ready r ON r.id = u.id
    WHERE u.processing_status = 'ready'
      AND (
        u.entry_classification = 'delete feature'
        OR u.data_source IS NOT NULL
      )
  $q$, staging_table);

  IF NOT EXISTS (SELECT 1 FROM _dam_publishable) THEN
    -- Nothing publishable; still restore any rows deferred above.
    EXECUTE format($q$
      UPDATE %s SET processing_status = 'ready' WHERE processing_status = 'wait'
    $q$, staging_table);
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
  -- PROCESS 2: attribute-source provenance (_dsid columns).
  -- MUST run BEFORE the live attribute overwrite (Process 3): each <col>_dsid is
  -- decided by comparing the incoming value to the CURRENT (old) live value.
  -- Iterates ds_columns (excludes use_analysis). Populates the bare
  -- dams_attribute_source rows created above.
  ---------------------------------------------------------------------------
  SELECT string_agg(
           format(
             '%I = CASE WHEN (u.%I IS NOT NULL AND u.%I IS DISTINCT FROM d.%I) THEN u.data_source ELSE s.%I END',
             col || '_dsid', col, col, col, col || '_dsid'
           ),
           ', '
         )
  INTO ds_set_clause
  FROM unnest(ds_columns) AS col;

  IF ds_set_clause IS NOT NULL THEN
    EXECUTE format($q$
      UPDATE dams.dams_attribute_source AS s
      SET %s
      FROM dams.dams AS d, %s AS u
      WHERE s.cabd_id = u.cabd_id
        AND d.cabd_id = s.cabd_id
        AND u.entry_classification IN ('new feature', 'modify feature')
        AND u.processing_status = 'ready'
        AND u.data_source IS NOT NULL
        AND u.id IN (SELECT id FROM _dam_publishable)
    $q$, ds_set_clause, staging_table);
  END IF;

  ---------------------------------------------------------------------------
  -- PROCESS 3: live attribute mapping.
  -- Iterates attr_columns (INCLUDES use_analysis). For each column, write the
  -- incoming value only when it is non-null AND differs from the live value;
  -- otherwise keep the live value. Applies to new + modify (new-feature rows
  -- were inserted geometry-only above and get their attributes here).
  ---------------------------------------------------------------------------
  SELECT string_agg(
           format(
             '%I = CASE WHEN (u.%I IS NOT NULL AND u.%I IS DISTINCT FROM d.%I) THEN u.%I ELSE d.%I END',
             col, col, col, col, col, col
           ),
           ', '
         )
  INTO attr_set_clause
  FROM unnest(attr_columns) AS col;

  IF attr_set_clause IS NOT NULL THEN
    EXECUTE format($q$
      UPDATE dams.dams AS d
      SET %s
      FROM %s AS u
      WHERE d.cabd_id = u.cabd_id
        AND u.entry_classification IN ('new feature', 'modify feature')
        AND u.processing_status = 'ready'
        AND u.data_source IS NOT NULL
        AND u.id IN (SELECT id FROM _dam_publishable)
    $q$, attr_set_clause, staging_table);
  END IF;

  ---------------------------------------------------------------------------
  -- DELETE successfully published rows from staging (publishable only)
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    DELETE FROM %s u
    USING _dam_publishable p
    WHERE u.id = p.id
  $q$, staging_table);
  GET DIAGNOSTICS deleted_from_staging = ROW_COUNT;

  ---------------------------------------------------------------------------
  -- PROCESS 4: reset deferred rows so the next update per feature is picked up.
  ---------------------------------------------------------------------------
  EXECUTE format($q$
    UPDATE %s SET processing_status = 'ready' WHERE processing_status = 'wait'
  $q$, staging_table);

  RETURN deleted_from_staging;
END;
$$;
