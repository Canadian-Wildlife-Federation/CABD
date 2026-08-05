-- Ensure ON CONFLICT(name) works
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='cabd' AND tablename='data_source' AND indexname='data_source_name_uq'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX data_source_name_uq ON cabd.data_source(name)';
  END IF;
END$$;

-- Insert-only upsert from cabd.data_source_updates into cabd.data_source,
-- ALWAYS restricted to only data sources referenced by staged feature updates.
CREATE OR REPLACE FUNCTION cabd.upsert_data_sources(
  staging_table regclass DEFAULT 'cabd.data_source_updates'::regclass,
  dam_updates_table regclass DEFAULT 'cabd.dam_updates'::regclass,
  fishway_updates_table regclass DEFAULT 'cabd.fishway_updates'::regclass,
  waterfall_updates_table regclass DEFAULT 'cabd.waterfall_updates'::regclass
)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  inserted_count integer := 0;
BEGIN
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS blocked_reason text', staging_table);
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS blocked_at timestamptz', staging_table);

  EXECUTE format($q$
    WITH needed AS (
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE data_source_short_name IS NOT NULL
      UNION
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE data_source_short_name IS NOT NULL
      UNION
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE data_source_short_name IS NOT NULL
    )
    INSERT INTO cabd.data_source (
      id,
      name,
      version_date,
      source,
      source_type,
      full_name,
      organization_name,
      data_source_category
    )
    SELECT
      s.id,
      s.name,
      s.version_date,
      s.source,
      s.source_type,
      s.full_name,
      s.organization_name,
      s.data_source_category
    FROM %s s
    WHERE s.name IN (SELECT n FROM needed)
    ON CONFLICT (name) DO NOTHING
  $q$, dam_updates_table, fishway_updates_table, waterfall_updates_table, staging_table);

  EXECUTE format($q$
    WITH needed AS (
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE data_source_short_name IS NOT NULL
      UNION
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE data_source_short_name IS NOT NULL
      UNION
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE data_source_short_name IS NOT NULL
    )
    UPDATE %s
    SET
      update_status = 'blocked',
      blocked_reason = 'data source not used for publishable features',
      blocked_at = now()
    WHERE name NOT IN (SELECT n FROM needed) 
  $q$, dam_updates_table, fishway_updates_table, waterfall_updates_table, staging_table);

  EXECUTE format('DELETE FROM %s WHERE name IN (select name from cabd.data_source)', staging_table);

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$$;