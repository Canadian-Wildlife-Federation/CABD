from __future__ import annotations

from pathlib import Path
import csv
from typing import Iterable


def stage_updates_csv_to_table(
    conn,
    csv_path: Path,
    staging_table: str,
    *,
    # Generic normalization knobs (feature-agnostic)
    trim_columns: Iterable[str] = (
        "data_source_short_name",
        "reviewer_comments",
        "province_territory_code",
        "entry_classification",
        "update_status",
        "update_type",
    ),
    lower_columns: Iterable[str] = ("province_territory_code",),
    set_status_from_reviewer_comments: bool = True,
    default_update_type: str | None = "user",
    generate_cabd_id_for_new_feature: bool = True,
    ) -> None:
    """
    Load a CSV into a staging table using COPY, then apply a generic
    normalization pass.

    This function is intentionally feature-agnostic:
      - It only normalizes columns if they exist in the staging table.
      - It does not do coded-value lookups.
      - It does not assume a specific primary key column.

    Normalization performed (if the relevant columns exist):
      - Trim whitespace and convert '' -> NULL for configured trim_columns
      - Lowercase configured lower_columns
      - If update_status is NULL/blank: set to 'ready' if reviewer_comments
        is NULL else 'needs review'
      - If update_type is NULL/blank: set to default_update_type
        (default 'user')
      - If cabd_id is NULL and entry_classification='new feature':
        set cabd_id = gen_random_uuid()

    Assumptions:
      - CSV header matches column names in staging_table
        (at least a compatible subset).
      - The staging table already exists with appropriate column types
        (uuid, numeric, etc.).
      - If cabd_id is uuid-typed, invalid UUID text in the CSV will fail COPY.
    """

    # ------------------------------------------------------------------
    # 1) COPY from CSV
    # ------------------------------------------------------------------
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        header = [h.strip() for h in header]

    copy_sql = (
        f"COPY {staging_table} ({', '.join([quote_ident(h) for h in header])}) "
        f"FROM STDIN WITH (FORMAT csv, HEADER true)"
    )

    with conn.cursor() as cur, csv_path.open("r", encoding="utf-8-sig") as f:
        cur.copy_expert(copy_sql, f)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            DELETE FROM {staging_table}
            WHERE "status" IN ('complete', 'do not process', 'on hold')
            """
        )
    # ------------------------------------------------------------------
    # 2) Normalization pass
    # ------------------------------------------------------------------
    cols = _get_table_columns(conn, staging_table)

    with conn.cursor() as cur:

        # 2a) trim + empty-string -> NULL
        for c in trim_columns:
            if c in cols:
                cur.execute(
                    f"""
                    UPDATE {staging_table}
                    SET {quote_ident(c)} = NULLIF(btrim({quote_ident(c)}), '')
                    WHERE {quote_ident(c)} IS NOT NULL
                    """
                )

        # 2b) lowercase selected columns
        for c in lower_columns:
            if c in cols:
                cur.execute(
                    f"""
                    UPDATE {staging_table}
                    SET {quote_ident(c)} = lower({quote_ident(c)})
                    WHERE {quote_ident(c)} IS NOT NULL
                    """
                )

        # 2c) set update_status based on reviewer_comments
        #     (only if update_status is missing)
        if (
            set_status_from_reviewer_comments
            and {"update_status", "reviewer_comments"}.issubset(cols)
        ):
            cur.execute(
                f"""
                UPDATE {staging_table}
                SET update_status = CASE
                    WHEN reviewer_comments IS NULL THEN 'ready'
                    ELSE 'needs review'
                END
                WHERE update_status IS NULL
                """
            )

        # 2d) default update_type
        if default_update_type is not None and "update_type" in cols:
            cur.execute(
                f"""
                UPDATE {staging_table}
                SET update_type = %s
                WHERE update_type IS NULL
                """,
                (default_update_type,),
            )

        # 2e) generate cabd_id for new features missing it
        if (
            generate_cabd_id_for_new_feature
            and {"cabd_id", "entry_classification"}.issubset(cols)
        ):
            cur.execute(
                f"""
                UPDATE {staging_table}
                SET cabd_id = gen_random_uuid()
                WHERE entry_classification = 'new feature'
                  AND cabd_id IS NULL
                """
            )


def _get_table_columns(conn, table_name: str) -> set[str]:
    """
    Returns a lowercase set of column names for a schema-qualified table.
    table_name must be 'schema.table' (or just 'table' for search_path resolution).
    """

    if "." in table_name:
        schema, table = table_name.split(".", 1)
    else:
        schema, table = None, table_name

    with conn.cursor() as cur:
        if schema:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                """,
                (schema, table),
            )
        else:
            # Falls back to current search_path; this is less deterministic.
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                """,
                (table,),
            )

        return {r[0].lower() for r in cur.fetchall()}


def quote_ident(ident: str) -> str:
    ident = ident.replace('"', '""')
    return f'"{ident}"'
