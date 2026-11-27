-- Read-only checks for staged dam updates.
--
-- Returns one row per problem found. Used in two places so the rules only
-- exist once:
--   * `cabd-updates check --feature dams` (preview before publishing)
--   * cabd.publish_dam_updates (decides which ready rows to block)
--
-- severity:
--   block : the row will be blocked by publish until the problem is fixed
--   info  : not a problem; e.g. the row is deferred to a later publish run
--
-- include_staged_sources:
--   true  (preview) - a data source that exists only in
--                     cabd.data_source_updates counts as available, because
--                     publish inserts it before publishing features.
--   false (publish) - only cabd.data_source counts.

CREATE OR REPLACE FUNCTION cabd.check_dam_updates(
  staging_table          regclass DEFAULT 'cabd.dam_updates'::regclass,
  include_staged_sources boolean  DEFAULT true
)
RETURNS TABLE (
  id                   integer,
  cabd_id              uuid,
  entry_classification text,
  processing_status    text,
  severity             text,
  issue_code           text,
  issue                text
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN QUERY EXECUTE format($q$
    WITH u AS (
      SELECT
        s.id,
        s.cabd_id,
        s.entry_classification::text AS entry_classification,
        s.processing_status::text    AS processing_status,
        s.submitted_on,
        s.data_source_short_name,
        s.latitude,
        s.longitude
      FROM %1$s s
    ),
    issues (id, severity, issue_code, issue) AS (

      -- The entry classification must be present and match the staging domain.
      SELECT u.id, 'block', 'invalid_entry_classification',
             'entry_classification must be one of: new feature, modify feature, delete feature'
      FROM u
      WHERE u.entry_classification IS NULL
         OR u.entry_classification NOT IN (
           'new feature',
           'modify feature',
           'delete feature'
         )

      UNION ALL

      -- Update order can't be determined without a timestamp.
      SELECT u.id, 'block', 'missing_submitted_on',
             'submitted_on is required to determine update ordering.'
      FROM u
      WHERE u.submitted_on IS NULL

      UNION ALL

      -- A paired delete/modify for the same feature has no timestamp, so this
      -- row's order relative to it can't be determined either.
      SELECT u.id, 'block', 'paired_missing_submitted_on',
             'A paired delete/modify update for this feature has no submitted_on, so update ordering cannot be determined.'
      FROM u
      WHERE u.submitted_on IS NOT NULL
        AND u.entry_classification IN ('modify feature', 'delete feature')
        AND EXISTS (
          SELECT 1 FROM u o
          WHERE o.cabd_id = u.cabd_id
            AND o.id <> u.id
            AND o.submitted_on IS NULL
            AND o.entry_classification IN ('modify feature', 'delete feature')
            AND o.entry_classification <> u.entry_classification
        )

      UNION ALL

      -- A delete followed by a later modify: both rows are unsafe.
      SELECT u.id, 'block', 'delete_then_modify',
             'A feature is marked for deletion but has been modified by a subsequent update.'
      FROM u
      WHERE (
              u.entry_classification = 'delete feature'
              AND EXISTS (
                SELECT 1 FROM u m
                WHERE m.cabd_id = u.cabd_id
                  AND m.entry_classification = 'modify feature'
                  AND m.submitted_on > u.submitted_on
              )
            )
         OR (
              u.entry_classification = 'modify feature'
              AND EXISTS (
                SELECT 1 FROM u d
                WHERE d.cabd_id = u.cabd_id
                  AND d.entry_classification = 'delete feature'
                  AND u.submitted_on > d.submitted_on
              )
            )

      UNION ALL

      -- New and modified features need a data source for provenance.
      -- Deletes don't, because they write no source records.
      SELECT u.id, 'block', 'missing_data_source',
             'data source ' || coalesce(quote_literal(u.data_source_short_name), '(blank)')
               || ' not found in cabd.data_source'
      FROM u
      WHERE u.entry_classification IN ('new feature', 'modify feature')
        AND NOT EXISTS (
          SELECT 1 FROM cabd.data_source ds
          WHERE lower(ds.name) = lower(btrim(u.data_source_short_name))
        )
        AND NOT (
          $1
          AND EXISTS (
            SELECT 1 FROM cabd.data_source_updates dsu
            WHERE lower(dsu.name) = lower(btrim(u.data_source_short_name))
          )
        )

      UNION ALL

      -- Modify/delete must target a dam that exists. Otherwise the update does
      -- nothing but would still be removed from staging as if it published.
      -- A modify for a dam that's still a staged new feature is blocked here
      -- and picked up automatically on the run after the new dam publishes.
      SELECT u.id, 'block', 'feature_not_found',
             'cabd_id ' || coalesce(u.cabd_id::text, '(blank)') || ' not found in dams.dams'
      FROM u
      WHERE u.entry_classification IN ('modify feature', 'delete feature')
        AND (
          u.cabd_id IS NULL
          OR NOT EXISTS (SELECT 1 FROM dams.dams d WHERE d.cabd_id = u.cabd_id)
        )

      UNION ALL

      -- A new feature whose cabd_id already exists would overwrite that dam's
      -- attributes.
      SELECT u.id, 'block', 'new_feature_exists',
             'cabd_id ' || u.cabd_id::text
               || ' already exists in dams.dams; publishing would overwrite it'
      FROM u
      WHERE u.entry_classification = 'new feature'
        AND EXISTS (SELECT 1 FROM dams.dams d WHERE d.cabd_id = u.cabd_id)

      UNION ALL

      -- A new feature can't be inserted without a location.
      SELECT u.id, 'block', 'new_feature_missing_coordinates',
             'new feature requires latitude and longitude'
      FROM u
      WHERE u.entry_classification = 'new feature'
        AND (u.latitude IS NULL OR u.longitude IS NULL)

      UNION ALL

      -- Informational: only the earliest ready modify/delete per feature
      -- publishes each run; the rest wait for a later run.
      SELECT r.id, 'info', 'deferred',
             'An earlier modify/delete update for this feature publishes first; this one will publish in a later run.'
      FROM (
        SELECT
          u.id,
          row_number() OVER (PARTITION BY u.cabd_id ORDER BY u.submitted_on ASC) AS rn
        FROM u
        WHERE u.processing_status = 'ready'
          AND u.entry_classification IN ('modify feature', 'delete feature')
      ) r
      WHERE r.rn > 1
    )
    SELECT
      u.id,
      u.cabd_id,
      u.entry_classification,
      u.processing_status,
      i.severity,
      i.issue_code,
      i.issue
    FROM issues i
    JOIN u ON u.id = i.id
    ORDER BY u.id, i.severity, i.issue_code
  $q$, staging_table)
  USING include_staged_sources;
END;
$$;
