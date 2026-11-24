-- Recognized feature workflow values move to processing_status; other legacy
-- values are retained as sharepoint_status. Data-source values are treated as
-- source statuses, with blocked workflow state recovered from blocked_reason.
-- Rows with any nonblank SharePoint status are excluded; blank-status rows are
-- left available for processing.
BEGIN;

DO $migration$
DECLARE
  v_table text;
  v_relation regclass;
  v_legacy_column text;
  v_has_column boolean;
  v_has_reviewer_comments boolean;
  v_has_blocked_reason boolean;
  v_is_data_source boolean;
  v_is_raw boolean;
  v_workflow_values text[];
  v_constraint_name text;
BEGIN
  FOREACH v_table IN ARRAY ARRAY[
    'cabd.dam_updates',
    'cabd.fishway_updates',
    'cabd.waterfall_updates',
    'cabd.data_source_updates',
    'cabd.dam_updates_raw',
    'cabd.fishway_updates_raw',
    'cabd.waterfall_updates_raw'
  ]
  LOOP
    v_relation := to_regclass(v_table);
    IF v_relation IS NULL THEN
      RAISE NOTICE 'Skipping %, table does not exist', v_table;
      CONTINUE;
    END IF;

    v_is_data_source := v_table = 'cabd.data_source_updates';
    v_is_raw := v_table IN (
      'cabd.dam_updates_raw',
      'cabd.fishway_updates_raw',
      'cabd.waterfall_updates_raw'
    );
    IF v_is_data_source THEN
      v_workflow_values := ARRAY[
        'needs review', 'ready', 'wait', 'blocked', 'done'
      ];
    ELSE
      v_workflow_values := ARRAY[
        'needs review', 'ready', 'wait', 'blocked'
      ];
    END IF;

    EXECUTE format(
      'ALTER TABLE %s ADD COLUMN IF NOT EXISTS sharepoint_status text',
      v_relation
    );
    EXECUTE format(
      'ALTER TABLE %s ADD COLUMN IF NOT EXISTS processing_status text',
      v_relation
    );

    FOREACH v_legacy_column IN ARRAY ARRAY['update_status', 'status']
    LOOP
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = split_part(v_table, '.', 1)
          AND table_name = split_part(v_table, '.', 2)
          AND column_name = v_legacy_column
      )
      INTO v_has_column;

      IF v_has_column THEN
        IF v_is_data_source THEN
          SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = split_part(v_table, '.', 1)
              AND table_name = split_part(v_table, '.', 2)
              AND column_name = 'blocked_reason'
          )
          INTO v_has_blocked_reason;

          IF v_has_blocked_reason THEN
            EXECUTE format(
              'UPDATE %1$s
               SET sharepoint_status = COALESCE(
                     sharepoint_status,
                     CASE
                       WHEN lower(btrim(%2$I::text)) = ''blocked''
                         AND NULLIF(btrim(blocked_reason), '''') IS NOT NULL
                       THEN NULL
                       ELSE NULLIF(btrim(%2$I::text), '''')
                     END
                   ),
                   processing_status = COALESCE(
                     processing_status,
                     CASE
                       WHEN lower(btrim(%2$I::text)) = ''blocked''
                         AND NULLIF(btrim(blocked_reason), '''') IS NOT NULL
                       THEN ''blocked''
                     END
                   )
               WHERE %2$I IS NOT NULL',
              v_relation,
              v_legacy_column
            );
          ELSE
            EXECUTE format(
              'UPDATE %1$s
               SET sharepoint_status = COALESCE(
                 sharepoint_status,
                 NULLIF(btrim(%2$I::text), '''')
               )
               WHERE %2$I IS NOT NULL',
              v_relation,
              v_legacy_column
            );
          END IF;
        ELSE
          EXECUTE format(
            'UPDATE %1$s
             SET sharepoint_status = COALESCE(
                   sharepoint_status,
                   CASE
                     WHEN lower(btrim(%2$I::text)) <> ALL(%3$L::text[])
                     THEN NULLIF(btrim(%2$I::text), '''')
                   END
                 ),
                 processing_status = COALESCE(
                   processing_status,
                   CASE
                     WHEN lower(btrim(%2$I::text)) = ANY(%3$L::text[])
                     THEN lower(btrim(%2$I::text))
                   END
                 )
             WHERE %2$I IS NOT NULL',
            v_relation,
            v_legacy_column,
            v_workflow_values
          );
        END IF;
      END IF;
    END LOOP;

    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = split_part(v_table, '.', 1)
        AND table_name = split_part(v_table, '.', 2)
        AND column_name = 'reviewer_comments'
    )
    INTO v_has_reviewer_comments;

    IF v_is_data_source THEN
      EXECUTE format(
        'UPDATE %s SET processing_status = COALESCE(processing_status, ''ready'')',
        v_relation
      );
    ELSIF v_has_reviewer_comments THEN
      EXECUTE format(
        'UPDATE %s
         SET processing_status = CASE
           WHEN NULLIF(btrim(reviewer_comments::text), '''') IS NULL
             THEN ''ready''
           ELSE ''needs review''
         END
         WHERE processing_status IS NULL',
        v_relation
      );
    ELSE
      EXECUTE format(
        'UPDATE %s SET processing_status = COALESCE(processing_status, ''ready'')',
        v_relation
      );
    END IF;

    EXECUTE format(
      'UPDATE %s SET processing_status = lower(btrim(processing_status))
       WHERE processing_status IS NOT NULL',
      v_relation
    );

    FOREACH v_legacy_column IN ARRAY ARRAY['update_status', 'status']
    LOOP
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = split_part(v_table, '.', 1)
          AND table_name = split_part(v_table, '.', 2)
          AND column_name = v_legacy_column
      )
      INTO v_has_column;

      IF v_has_column THEN
        EXECUTE format(
          'ALTER TABLE %s DROP COLUMN %I',
          v_relation,
          v_legacy_column
        );
      END IF;
    END LOOP;

    EXECUTE format(
      'UPDATE %s
       SET sharepoint_status = NULLIF(btrim(sharepoint_status::text), '''')',
      v_relation
    );
    EXECUTE format(
      'DELETE FROM %s WHERE sharepoint_status IS NOT NULL',
      v_relation
    );

    IF EXISTS (
      SELECT 1
      FROM pg_attribute
      WHERE attrelid = v_relation
        AND attname = 'processing_status'
        AND NOT attisdropped
    ) THEN
      EXECUTE format(
        'SELECT EXISTS (
           SELECT 1 FROM %s
           WHERE processing_status IS NOT NULL
             AND processing_status <> ALL(%L::text[])
         )',
        v_relation,
        v_workflow_values
      )
      INTO v_has_column;
      IF v_has_column THEN
        RAISE EXCEPTION
          'Unexpected processing_status values remain in %. Review the table before retrying.',
          v_table;
      END IF;
    END IF;

    EXECUTE format(
      'COMMENT ON COLUMN %s.sharepoint_status IS %L',
      v_relation,
      'Original status value supplied by SharePoint; nonblank values are excluded from staging.'
    );
    EXECUTE format(
      'COMMENT ON COLUMN %s.processing_status IS %L',
      v_relation,
      'CABD workflow state used by staging, review, blocking, and publishing.'
    );

    IF NOT v_is_raw THEN
      v_constraint_name := split_part(v_table, '.', 2)
        || '_processing_status_check';
      EXECUTE format(
        'ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I',
        v_relation,
        v_constraint_name
      );
      EXECUTE format(
        'ALTER TABLE %s ADD CONSTRAINT %I CHECK (processing_status = ANY(%L::text[]))',
        v_relation,
        v_constraint_name,
        v_workflow_values
      );
    END IF;
  END LOOP;
END;
$migration$;

COMMIT;
