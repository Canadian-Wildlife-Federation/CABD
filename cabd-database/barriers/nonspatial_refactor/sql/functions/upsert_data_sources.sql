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

  -- Remove staged data sources that are associated ONLY with delete-feature
  -- actions. A source also referenced by a new/modify action is kept, because
  -- that action still needs the source published.
  EXECUTE format($q$
    WITH delete_refs AS (
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE entry_classification = 'delete feature' AND data_source_short_name IS NOT NULL
      UNION
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE entry_classification = 'delete feature' AND data_source_short_name IS NOT NULL
      UNION
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE entry_classification = 'delete feature' AND data_source_short_name IS NOT NULL
    ),
    keep_refs AS (
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE entry_classification IN ('new feature','modify feature') AND data_source_short_name IS NOT NULL
      UNION
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE entry_classification IN ('new feature','modify feature') AND data_source_short_name IS NOT NULL
      UNION
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE entry_classification IN ('new feature','modify feature') AND data_source_short_name IS NOT NULL
    )
    DELETE FROM %s
    WHERE lower(name) IN (SELECT lower(n) FROM delete_refs)
      AND lower(name) NOT IN (SELECT lower(n) FROM keep_refs)
  $q$, dam_updates_table, fishway_updates_table, waterfall_updates_table,
       dam_updates_table, fishway_updates_table, waterfall_updates_table,
       staging_table);

  -- needed = sources referenced by NON-delete (publishable) actions only
  EXECUTE format($q$
    WITH needed AS (
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE entry_classification IN ('new feature','modify feature') AND data_source_short_name IS NOT NULL
      UNION
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE entry_classification IN ('new feature','modify feature') AND data_source_short_name IS NOT NULL
      UNION
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE entry_classification IN ('new feature','modify feature') AND data_source_short_name IS NOT NULL
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
    WHERE lower(s.name) IN (SELECT lower(n) FROM needed)
    ON CONFLICT (name) DO NOTHING
  $q$, dam_updates_table, fishway_updates_table, waterfall_updates_table, staging_table);

  EXECUTE format($q$
    WITH needed AS (
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE entry_classification IN ('new feature','modify feature') AND data_source_short_name IS NOT NULL
      UNION
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE entry_classification IN ('new feature','modify feature') AND data_source_short_name IS NOT NULL
      UNION
      SELECT DISTINCT data_source_short_name AS n FROM %s WHERE entry_classification IN ('new feature','modify feature') AND data_source_short_name IS NOT NULL
    )
    UPDATE %s
    SET
      update_status = 'blocked',
      blocked_reason = 'data source not used for publishable features',
      blocked_at = now()
    WHERE lower(name) NOT IN (SELECT lower(n) FROM needed)
  $q$, dam_updates_table, fishway_updates_table, waterfall_updates_table, staging_table);

  EXECUTE format('DELETE FROM %s WHERE name IN (select name from cabd.data_source)', staging_table);

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$$;