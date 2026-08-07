from __future__ import annotations

from datetime import date
from pathlib import Path
import csv
import json
import re
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
        "cabd_id",
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
    cast_map: dict | None = None,
    province_boundary_table: str | None = "cabd.province_territory_codes",
    province_code_column: str | None = "code",
    province_srid: int = 4617,
    feature_name: str | None = None,
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

    # If cast_map is provided we take the tolerant path: create the raw temp
    # table with all columns as text, then INSERT into the staging table using
    # explicit casts for the mapped columns. Otherwise try to infer column
    # types from the staging table and CREATE the temp table with matching types
    # so COPY will do casting implicitly.
    if cast_map:
        cols_sql = ", ".join([f"{quote_ident(h)} text" for h in header])
    else:
        staging_col_types = _get_table_column_types(conn, staging_table) if staging_table else {}
        cols_sql = ", ".join(
            [f"{quote_ident(h)} {staging_col_types.get(h.lower(), 'text')}" for h in header]
        )

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
    # 3) Optional normalization on the temp raw table (generic)
    # -----------------------
    if normalize:
        _normalize_temp_table(
            conn,
            raw_table_qualified,
            trim_columns=trim_columns,
            lower_columns=lower_columns,
            set_status_from_reviewer_comments=set_status_from_reviewer_comments,
            default_update_type=default_update_type,
            generate_cabd_id_for_new_feature=generate_cabd_id_for_new_feature,
            province_boundary_table=province_boundary_table,
            province_code_column=province_code_column,
            province_srid=province_srid,
            cast_map=cast_map,
        )

    result = _copy_temp_rows_to_staging_with_error_log(
        conn=conn,
        temp_table=raw_table_qualified,
        staging_table=staging_table,
        required_columns=required_columns,
        header=header,
        header_lc=header_lc,
        cast_map=cast_map,
        feature_name=feature_name,
    )

    result["raw_table"] = raw_table_qualified
    return result


def _normalize_temp_table(
    conn,
    temp_table: str,
    *,
    trim_columns: Iterable[str],
    lower_columns: Iterable[str],
    set_status_from_reviewer_comments: bool,
    default_update_type: str | None,
    generate_cabd_id_for_new_feature: bool,
    province_boundary_table: str | None = "cabd.province_territory_codes",
    province_code_column: str | None = "code",
    province_srid: int = 4617,
    cast_map: dict | None = None,
) -> None:
    cols = _get_table_columns(conn, temp_table)

    with conn.cursor() as cur:
        # trim + empty-string -> NULL
        for c in trim_columns:
            if c.lower() in cols:
                cur.execute(
                    f"""
                    UPDATE {temp_table}
                    SET {quote_ident(c)} = NULLIF(btrim({quote_ident(c)}::text), '')
                    WHERE {quote_ident(c)} IS NOT NULL
                    """
                )

        # lowercase
        for c in lower_columns:
            if c.lower() in cols:
                cur.execute(
                    f"""
                    UPDATE {temp_table}
                    SET {quote_ident(c)} = lower({quote_ident(c)}::text)
                    WHERE {quote_ident(c)} IS NOT NULL
                    """
                )

        # default update_type
        if default_update_type is not None and "update_type" in cols:
            cur.execute(
                f"""
                UPDATE {temp_table}
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
                UPDATE {temp_table}
                SET cabd_id = gen_random_uuid()
                WHERE entry_classification = 'new feature'
                AND cabd_id IS NULL
                """
            )

        # clean up status column
        cur.execute(
            f"""
            DELETE FROM {temp_table}
            WHERE status IN ('complete', 'do not process', 'on hold')
            """
        )
        if (
            set_status_from_reviewer_comments
            and {"status", "reviewer_comments"}.issubset(cols)
        ):
            cur.execute(
                f"""
                UPDATE {temp_table}
                SET status = CASE
                    WHEN reviewer_comments IS NULL THEN 'ready'
                    ELSE 'needs review'
                END
                WHERE status IS NULL
                """
            )

        # Ensure a province_territory_code column exists on the raw table
        # and, if possible, populate it from the authoritative province
        # boundary table using PostGIS (transforming from WGS84->province SRID).
        try:
            cur.execute(f"ALTER TABLE {temp_table} ADD COLUMN IF NOT EXISTS province_territory_code text;")

            if province_boundary_table:
                schema_table = province_boundary_table
                if '.' in schema_table:
                    s, t = schema_table.split('.', 1)
                else:
                    s, t = None, schema_table

                # discover geometry column
                geom_col = 'geometry'
                code_col = province_code_column or 'code'

                if geom_col and code_col:
                    update_sql = f'''
                    UPDATE {temp_table} rt
                    SET province_territory_code = b.{quote_ident(code_col)}
                    FROM {schema_table} b
                    WHERE rt.province_territory_code IS NULL
                    AND b.{quote_ident(geom_col)} IS NOT NULL
                    AND ST_Intersects(
                        b.{quote_ident(geom_col)},
                        ST_Transform(
                            ST_SetSRID(ST_Point(rt.longitude::double precision, rt.latitude::double precision), 4326),
                            {province_srid}
                        )
                    )
                    '''
                    cur.execute(update_sql)
        except Exception:
            # best-effort; do not block staging on spatial lookup failures
            pass

def _feature_name_from_staging_table(staging_table: str) -> str:
    if "." in staging_table:
        _, table = staging_table.split(".", 1)
    else:
        table = staging_table
    table = table.strip('"')
    if table.lower().endswith("_updates"):
        table = table[: -len("_updates")]
    return _sanitize_identifier(table.lower() or "feature")


def _error_log_table_name(conn, feature_name: str, staging_table: str) -> str:
    schema = None
    if "." in staging_table:
        schema, _ = staging_table.split(".", 1)
        schema = schema.strip('"')
    date_suffix = date.today().strftime("%Y%m%d")
    error_table = f"error_log_{_sanitize_identifier(feature_name)}_updates_{date_suffix}"
    return f"{quote_ident(schema)}.{quote_ident(error_table)}" if schema else quote_ident(error_table)


def _sanitize_identifier(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9_]+", "_", value)
    value = re.sub(r"__+", "_", value)
    return value.strip("_").lower() or "unknown"


def _ensure_error_log_table(conn, error_log_table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {error_log_table} ("
            "failed_row jsonb, "
            "error_message text, "
            "failed_at timestamptz DEFAULT now()"
            ");"
        )


def _copy_temp_rows_to_staging_with_error_log(
    conn,
    temp_table: str,
    staging_table: str,
    *,
    required_columns: Sequence[str],
    header: list[str],
    header_lc: dict[str, str],
    cast_map: dict | None,
    feature_name: str | None,
) -> dict[str, object]:
    insert_cols_sql = ", ".join([quote_ident(c) for c in required_columns])
    error_log_table = _error_log_table_name(conn, feature_name or _feature_name_from_staging_table(staging_table), staging_table)
    _ensure_error_log_table(conn, error_log_table)

    safe_aliases = [f"c{i}" for i in range(len(header))]
    alias_list_sql = ", ".join(
        f"{quote_ident(col)} AS {alias}"
        for col, alias in zip(header, safe_aliases)
    )

    def _value_expression(alias: str, source_col: str) -> str:
        rc_lc = source_col.lower()
        target = f"t.{alias}"

        if cast_map and rc_lc in cast_map:
            target_type = str(cast_map[rc_lc])
            target_type_lc = target_type.lower()

            # Explicitly parse M/D/YYYY H:MI AM/PM values like:
            # 4/16/2026 3:22 PM
            if "timestamp" in target_type_lc:
                return (
                    f"CASE "
                    f"WHEN NULLIF(btrim({target}::text), '') IS NULL THEN NULL "
                    f"ELSE ("
                    f"to_timestamp("
                    f"NULLIF(btrim({target}::text), ''), "
                    f"'MM/DD/YYYY HH12:MI AM'"
                    f")::timestamp AT TIME ZONE 'America/Toronto'"
                    f") "
                    f"END"
                )

            return f"NULLIF(btrim({target}::text), '')::{target_type}"

        return target

    header_index_by_lc = {
        column_name.lower(): i
        for i, column_name in enumerate(header)
    }

    insert_value_sql = ", ".join(
        _value_expression(
            safe_aliases[header_index_by_lc[required_col.lower()]],
            header_lc[required_col.lower()],
        )
        for required_col in required_columns
    )

    insert_sql = (
        f"INSERT INTO {staging_table} ({insert_cols_sql}) "
        f"SELECT {insert_value_sql} FROM (VALUES ({', '.join(['%s'] * len(header))})) "
        f"AS t({', '.join([quote_ident(alias) for alias in safe_aliases])});"
    )

    error_insert_sql = (
        f"INSERT INTO {error_log_table} (failed_row, error_message) VALUES (%s, %s);"
    )

    inserted_rows = 0
    with conn.cursor() as cur:
        cur.execute(f"SELECT {alias_list_sql} FROM {temp_table};")
        for row in cur.fetchall():
            cur.execute("SAVEPOINT row_insert")
            try:
                cur.execute(insert_sql, row)
                cur.execute("RELEASE SAVEPOINT row_insert")
                inserted_rows += 1
            except Exception as exc:
                cur.execute("ROLLBACK TO SAVEPOINT row_insert")
                cur.execute("RELEASE SAVEPOINT row_insert")
                failed_row = dict(zip(header, row))
                cur.execute(
                    error_insert_sql,
                    (json.dumps(failed_row, default=str), str(exc)),
                )

    return {
        "inserted_rows": int(inserted_rows),
        "error_log_table": error_log_table,
    }


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


def _get_table_column_types(conn, table_name: str) -> dict[str, str]:
    """Return a mapping of lowercase column_name -> SQL type (suitable for CREATE TABLE).

    Uses pg_catalog.format_type for accurate type text (handles varchar(n), numeric, etc.).
    If the table_name contains a schema (schema.table) that schema is used; otherwise
    searches current search_path for the table.
    """
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
                SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) as type
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relname = %s AND n.nspname = %s
                AND a.attnum > 0 AND NOT a.attisdropped
                """,
                (table, schema),
            )
        else:
            cur.execute(
                """
                SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) as type
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relname = %s
                AND a.attnum > 0 AND NOT a.attisdropped
                """,
                (table,),
            )
        return {r[0].lower(): r[1] for r in cur.fetchall()}


def quote_ident(ident: str) -> str:
    ident = ident.replace('"', '""')
    return f'"{ident}"'
