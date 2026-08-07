from __future__ import annotations

from pathlib import Path
import csv
import uuid
from typing import Iterable, Sequence


def stage_updates_csv_to_table(
    conn,
    csv_path: Path,
    staging_table: str,
    *,
    required_columns: Sequence[str],
    raw_schema: str | None = None,
    raw_table_prefix: str = "updates_raw_",
    normalize: bool = True,
    trim_columns: Iterable[str] = (
        "data_source_short_name",
        "reviewer_comments",
        "province_territory_code",
        "entry_classification",
        "status",
        "update_type",
    ),
    lower_columns: Iterable[str] = ("province_territory_code",),
    set_status_from_reviewer_comments: bool = True,
    default_update_type: str | None = "cwf",
    generate_cabd_id_for_new_feature: bool = True,
) -> dict:
    """
    Generic, feature-agnostic staging loader:

    1) COPY the entire CSV into a raw table (all columns as TEXT)
    2) INSERT only required_columns into the staging table (legacy moveQuery pattern)
    3) (optional) apply generic normalization rules on the staging table

    IMPORTANT:
    - stage_updates.py stays feature-agnostic by taking required_columns as a list.
    - Coded-value transforms are intentionally NOT performed here.
    """
    if not required_columns:
        raise ValueError("required_columns must be a non-empty list")

    # -----------------------
    # 1) Read CSV header
    # -----------------------
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        header = [h.strip() for h in header]
        header_lc = {h.lower(): h for h in header}  # lowercase->original

    missing = [c for c in required_columns if c.lower() not in header_lc]
    if missing:
        raise ValueError(
            f"{csv_path}: missing required columns for staging: {missing}"
        )

    # -----------------------
    # 2) Create raw table + COPY everything
    # -----------------------
    raw_table = f"temp_updates_{uuid.uuid4().hex[:12]}"
    raw_table_qualified = raw_table
    create_prefix = "CREATE TEMP TABLE"
    drop_stmt = f"DROP TABLE IF EXISTS {raw_table};"

    cols_sql = ", ".join([f"{quote_ident(h)} text" for h in header])

    with conn.cursor() as cur:
        cur.execute(drop_stmt)
        cur.execute(f"{create_prefix} {raw_table_qualified} ({cols_sql});")

        copy_sql = (
            f"COPY {raw_table_qualified} "
            f"({', '.join([quote_ident(h) for h in header])}) "
            f"FROM STDIN WITH (FORMAT csv, HEADER true)"
        )

        with csv_path.open("r", encoding="utf-8-sig") as f2:
            cur.copy_expert(copy_sql, f2)

    # -----------------------
    # 3) INSERT subset into staging table (moveQuery pattern)
    # -----------------------
    # NOTE: coded-value transforms intentionally not done here.
    src_cols = [header_lc[c.lower()] for c in required_columns]

    insert_cols_sql = ", ".join([quote_ident(c) for c in required_columns])
    select_cols_sql = ", ".join([quote_ident(c) for c in src_cols])

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {staging_table} ({insert_cols_sql})
            SELECT {select_cols_sql}
            FROM {raw_table_qualified};
            """
        )

        inserted_rows = cur.rowcount

    # -----------------------
    # 4) Optional normalization on staging table (generic)
    # -----------------------
    if normalize:
        _normalize_staging_table(
            conn,
            staging_table,
            trim_columns=trim_columns,
            lower_columns=lower_columns,
            set_status_from_reviewer_comments=set_status_from_reviewer_comments,
            default_update_type=default_update_type,
            generate_cabd_id_for_new_feature=generate_cabd_id_for_new_feature,
        )

    return {
        "raw_table": raw_table_qualified,
        "inserted_rows": int(inserted_rows or 0),
    }


def _normalize_staging_table(
    conn,
    staging_table: str,
    *,
    trim_columns: Iterable[str],
    lower_columns: Iterable[str],
    set_status_from_reviewer_comments: bool,
    default_update_type: str | None,
    generate_cabd_id_for_new_feature: bool,
) -> None:
    cols = _get_table_columns(conn, staging_table)

    with conn.cursor() as cur:
        # trim + empty-string -> NULL
        for c in trim_columns:
            if c.lower() in cols:
                cur.execute(
                    f"""
                    UPDATE {staging_table}
                    SET {quote_ident(c)} = NULLIF(btrim({quote_ident(c)}::text), '')
                    WHERE {quote_ident(c)} IS NOT NULL
                    """
                )

        # lowercase
        for c in lower_columns:
            if c.lower() in cols:
                cur.execute(
                    f"""
                    UPDATE {staging_table}
                    SET {quote_ident(c)} = lower({quote_ident(c)}::text)
                    WHERE {quote_ident(c)} IS NOT NULL
                    """
                )

        # clean up status column
        cur.execute(
            f"""
            DELETE FROM {staging_table}
            WHERE status IN ('complete', 'do not process', 'on hold')
            """
        )
        if (
            set_status_from_reviewer_comments
            and {"status", "reviewer_comments"}.issubset(cols)
        ):
            cur.execute(
                f"""
                UPDATE {staging_table}
                SET status = CASE
                    WHEN reviewer_comments IS NULL THEN 'ready'
                    ELSE 'needs review'
                END
                WHERE status IS NULL
                """
            )

        # default update_type
        if default_update_type is not None and "update_type" in cols:
            cur.execute(
                f"""
                UPDATE {staging_table}
                SET update_type = %s
                WHERE update_type IS NULL
                """,
                (default_update_type,),
            )

        # generate cabd_id for new features missing it
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
    if "." in table_name:
        schema, table = table_name.split(".", 1)
        schema = schema.strip('"')
        table = table.strip('"')
    else:
        schema, table = None, table_name
    with conn.cursor() as cur:
        if schema:
            cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
            )
        else:
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
