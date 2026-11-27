DROP FUNCTION IF EXISTS cabd.upsert_data_sources(regclass, regclass, regclass, regclass);

CREATE OR REPLACE FUNCTION cabd.upsert_data_sources()
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  inserted_count integer := 0;
BEGIN
  ALTER TABLE cabd.data_source_updates ADD COLUMN IF NOT EXISTS blocked_reason text;
  ALTER TABLE cabd.data_source_updates ADD COLUMN IF NOT EXISTS blocked_at timestamptz;

  -- Remove staged data sources that are associated ONLY with delete-feature
  -- actions. A source also referenced by a new/modify action is kept, because
  -- that action still needs the source published.
  WITH delete_refs AS (
    SELECT data_source_short_name AS n FROM cabd.dam_updates
    WHERE entry_classification = 'delete feature' AND data_source_short_name IS NOT NULL
    UNION
    SELECT data_source_short_name FROM cabd.fishway_updates
    WHERE entry_classification = 'delete feature' AND data_source_short_name IS NOT NULL
    UNION
    SELECT data_source_short_name FROM cabd.waterfall_updates
    WHERE entry_classification = 'delete feature' AND data_source_short_name IS NOT NULL
  ),
  keep_refs AS (
    SELECT data_source_short_name AS n FROM cabd.dam_updates
    WHERE entry_classification IN ('new feature', 'modify feature') AND data_source_short_name IS NOT NULL
    UNION
    SELECT data_source_short_name FROM cabd.fishway_updates
    WHERE entry_classification IN ('new feature', 'modify feature') AND data_source_short_name IS NOT NULL
    UNION
    SELECT data_source_short_name FROM cabd.waterfall_updates
    WHERE entry_classification IN ('new feature', 'modify feature') AND data_source_short_name IS NOT NULL
  )
  DELETE FROM cabd.data_source_updates
  WHERE lower(name) IN (SELECT lower(n) FROM delete_refs)
    AND lower(name) NOT IN (SELECT lower(n) FROM keep_refs);

  -- needed = sources referenced by NON-delete (publishable) actions only
  WITH needed AS (
    SELECT data_source_short_name AS n FROM cabd.dam_updates
    WHERE entry_classification IN ('new feature', 'modify feature') AND data_source_short_name IS NOT NULL
    UNION
    SELECT data_source_short_name FROM cabd.fishway_updates
    WHERE entry_classification IN ('new feature', 'modify feature') AND data_source_short_name IS NOT NULL
    UNION
    SELECT data_source_short_name FROM cabd.waterfall_updates
    WHERE entry_classification IN ('new feature', 'modify feature') AND data_source_short_name IS NOT NULL
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
  FROM cabd.data_source_updates s
  WHERE lower(s.name) IN (SELECT lower(n) FROM needed)
  ON CONFLICT (name) DO NOTHING;

  WITH needed AS (
    SELECT data_source_short_name AS n FROM cabd.dam_updates
    WHERE entry_classification IN ('new feature', 'modify feature') AND data_source_short_name IS NOT NULL
    UNION
    SELECT data_source_short_name FROM cabd.fishway_updates
    WHERE entry_classification IN ('new feature', 'modify feature') AND data_source_short_name IS NOT NULL
    UNION
    SELECT data_source_short_name FROM cabd.waterfall_updates
    WHERE entry_classification IN ('new feature', 'modify feature') AND data_source_short_name IS NOT NULL
  )
  UPDATE cabd.data_source_updates
  SET
    processing_status = 'blocked',
    blocked_reason = 'data source not used for publishable features',
    blocked_at = now()
  WHERE lower(name) NOT IN (SELECT lower(n) FROM needed);

  DELETE FROM cabd.data_source_updates
  WHERE name IN (SELECT name FROM cabd.data_source);

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$$;