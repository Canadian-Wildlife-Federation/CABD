from __future__ import annotations

from pathlib import Path
import csv

from cabd_updates.db.call import call_scalar


def stage_data_sources_csv_to_table(
    conn,
    csv_path: Path,
    staging_table: str = "cabd.data_source_updates",
    dedupe_on: str = "name",
) -> dict:
    """
    Stage data sources into cabd.data_source_updates using COPY, then perform a
    small amount of normalization/deduplication in SQL.

    Expected CSV columns (recommended):
        - name
        - version_date
        - source
        - source_type
        - full_name
        - organization_name
        - data_source_category
        - id (optional; if absent, IDs will be generated)

    Notes:
        - This function intentionally does NOT restrict to sources referenced by
          staged updates.
        - That restriction is enforced inside cabd.upsert_data_sources().
        - This function does NOT publish; it only stages.
    """

    # Read header
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        header = [h.strip() for h in header]

    # Create staging table if it doesn't exist
    # (based on cabd.data_source structure)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {staging_table}
            (LIKE cabd.data_source INCLUDING ALL);

            ALTER TABLE {staging_table}
            ALTER COLUMN "id" DROP NOT NULL;
            """
        )

        # Ensure uniqueness for ON CONFLICT(name) style behavior
        # and dedupe hygiene
        cur.execute(
            f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_indexes
                    WHERE schemaname = split_part('{staging_table}', '.', 1)
                      AND tablename = split_part('{staging_table}', '.', 2)
                      AND indexname = 'data_source_updates_name_uq'
                ) THEN
                    EXECUTE
                        'CREATE UNIQUE INDEX data_source_updates_name_uq
                         ON {staging_table}(name)';
                END IF;
            END
            $$;
            """
        )

    # COPY into staging table
    copy_sql = (
        f"COPY {staging_table} "
        f"({', '.join([quote_ident(h) for h in header])}) "
        f"FROM STDIN WITH (FORMAT csv, HEADER true)"
    )

    with conn.cursor() as cur, csv_path.open("r", encoding="utf-8-sig") as f:
        cur.copy_expert(copy_sql, f)

    # Normalize + dedupe inside the staging table
    with conn.cursor() as cur:
        # Ensure id column exists (it does in cabd.data_source LIKE),
        # but keep it safe
        cur.execute(
            f'ALTER TABLE {staging_table} '
            f'ADD COLUMN IF NOT EXISTS "id" uuid'
        )

        # Normalize
        cur.execute(
            f"""
            UPDATE {staging_table}
            SET
                name = NULLIF(btrim(name), ''),
                source_type = COALESCE(
                    NULLIF(btrim(source_type), ''),
                    'non-spatial'
                )
            """
        )

        # Generate ids where missing
        cur.execute(
            f"""
            UPDATE {staging_table}
            SET id = gen_random_uuid()
            WHERE id IS NULL
            """
        )

        # Drop rows without a name
        cur.execute(
            f"DELETE FROM {staging_table} WHERE name IS NULL"
        )

        # Dedupe: keep one row per name
        # (prefer earliest version_date)
        cur.execute(
            f"""
            WITH ranked AS (
                SELECT
                    ctid,
                    row_number() OVER (
                        PARTITION BY {quote_ident(dedupe_on)}
                        ORDER BY version_date ASC NULLS LAST
                    ) AS rn
                FROM {staging_table}
            )
            DELETE FROM {staging_table} s
            USING ranked r
            WHERE s.ctid = r.ctid
              AND r.rn > 1
            """
        )

    row_count = call_scalar(
        conn,
        "SELECT COUNT(*) FROM " + staging_table,
    )

    return {
        "staging_table": staging_table,
        "row_count": int(row_count or 0),
    }


def quote_ident(ident: str) -> str:
    ident = ident.replace('"', '""')
    return f'"{ident}"'