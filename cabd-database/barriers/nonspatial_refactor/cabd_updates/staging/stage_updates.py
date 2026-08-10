from __future__ import annotations

from pathlib import Path
import csv
import re
from io import StringIO
from typing import Iterable, Sequence

_NUMERIC_TYPE_HINTS = ("double precision", "numeric", "real", "int", "smallint", "bigint")
_THOUSANDS_RE = re.compile(r"^-?\d{1,3}(,\d{3})+$")


def _thousands_clean_indices(header: list[str], column_types: dict | None) -> set[int]:
    """Return CSV column indices whose target type is numeric."""
    if not column_types:
        return set()
    types_lc = {
        str(k).lower(): str(v).lower()
        for k, v in column_types.items()
        if v is not None and str(v).strip() != ""
    }
    indices = set()
    for i, h in enumerate(header):
        target = types_lc.get(h.lower(), "")
        if any(hint in target for hint in _NUMERIC_TYPE_HINTS):
            indices.add(i)
    return indices


def _strip_thousands(value: str) -> str:
    """Strip commas only from values that are exactly comma-grouped integers."""
    if value is None:
        return value
    v = value.strip()
    if _THOUSANDS_RE.match(v):
        return v.replace(",", "")
    return value


def stage_updates_csv_to_table(
    conn,
    csv_path: Path,
    staging_table: str,
    *,
    column_types: dict | None = None,
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
    province_boundary_table: str | None = "cabd.province_territory_codes",
    province_code_column: str | None = "code",
    province_srid: int = 4617,
    feature_name: str | None = None,
) -> dict:
    """Stage CSV updates into a persistent feature raw table and promote rows into staging."""
    if not column_types:
        raise ValueError("column_types must be a non-empty mapping")

    required_columns = list(column_types.keys())
    cast_map = {k.lower(): v for k, v in column_types.items() if v is not None and str(v).strip() != ""}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        header = [h.strip() for h in header]
        header_lc = {h.lower(): h for h in header}

    missing = [c for c in required_columns if c.lower() not in header_lc]
    if missing:
        raise ValueError(f"{csv_path}: missing required columns for staging: {missing}")

    raw_table = _raw_table_name(staging_table)
    header_lower = {h.lower() for h in header}

    with conn.cursor() as cur:
        _ensure_raw_table(conn, raw_table, header)
        cur.execute(f"DELETE FROM {raw_table} WHERE load_status = 'loaded';")

        clean_indices = _thousands_clean_indices(header, column_types)
        copy_header = [*header, "source_file", "csv_line_no"]
        with StringIO() as buffer:
            writer = csv.writer(buffer, lineterminator="\n")
            writer.writerow(copy_header)
            with csv_path.open("r", encoding="utf-8-sig", newline="") as f2:
                reader = csv.reader(f2)
                next(reader)
                for line_no, row in enumerate(reader, start=2):
                    cleaned = [
                        _strip_thousands(val) if i in clean_indices else val
                        for i, val in enumerate(row)
                    ]
                    writer.writerow([*cleaned, str(csv_path), line_no])
            buffer.seek(0)
            copy_sql = (
                f"COPY {raw_table} "
                f"({', '.join([quote_ident(h) for h in copy_header])}) "
                f"FROM STDIN WITH (FORMAT csv, HEADER true)"
            )
            cur.copy_expert(copy_sql, buffer)

        if "status" in header_lower:
            cur.execute(
                f"""
                DELETE FROM {raw_table}
                WHERE status IN ('complete', 'do not process', 'on hold')
                """
            )

        if set_status_from_reviewer_comments and {"status", "reviewer_comments"}.issubset(header_lower):
            cur.execute(
                f"""
                UPDATE {raw_table}
                SET status = CASE
                    WHEN reviewer_comments IS NULL THEN 'ready'
                    ELSE 'needs review'
                END
                WHERE status IS NULL
                """
            )

        try:
            cur.execute(f"ALTER TABLE {raw_table} ADD COLUMN IF NOT EXISTS province_territory_code text;")
            if province_boundary_table:
                schema_table = province_boundary_table
                if "." in schema_table:
                    _, table_name = schema_table.split(".", 1)
                else:
                    table_name = schema_table

                geom_col = "geometry"
                code_col = province_code_column or "code"
                if geom_col and code_col:
                    update_sql = f"""
                    UPDATE {raw_table} rt
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
                    """
                    cur.execute(update_sql)
        except Exception:
            pass

    if normalize:
        _normalize_raw_table(
            conn,
            raw_table,
            trim_columns=trim_columns,
            lower_columns=lower_columns,
            set_status_from_reviewer_comments=set_status_from_reviewer_comments,
            default_update_type=default_update_type,
            generate_cabd_id_for_new_feature=generate_cabd_id_for_new_feature,
            province_boundary_table=province_boundary_table,
            province_code_column=province_code_column,
            province_srid=province_srid,
        )

    result = _promote_raw_rows_to_staging(
        conn=conn,
        raw_table=raw_table,
        staging_table=staging_table,
        required_columns=required_columns,
        header_lc=header_lc,
        cast_map=cast_map,
    )
    result["raw_table"] = raw_table
    return result


def _normalize_raw_table(
    conn,
    raw_table: str,
    *,
    trim_columns: Iterable[str],
    lower_columns: Iterable[str],
    set_status_from_reviewer_comments: bool,
    default_update_type: str | None,
    generate_cabd_id_for_new_feature: bool,
    province_boundary_table: str | None = "cabd.province_territory_codes",
    province_code_column: str | None = "code",
    province_srid: int = 4617,
) -> None:
    cols = _get_table_columns(conn, raw_table)

    with conn.cursor() as cur:
        for c in trim_columns:
            if c.lower() in cols:
                cur.execute(
                    f"""
                    UPDATE {raw_table}
                    SET {quote_ident(c)} = NULLIF(btrim({quote_ident(c)}::text), '')
                    WHERE {quote_ident(c)} IS NOT NULL
                    """
                )

        for c in lower_columns:
            if c.lower() in cols:
                cur.execute(
                    f"""
                    UPDATE {raw_table}
                    SET {quote_ident(c)} = lower({quote_ident(c)}::text)
                    WHERE {quote_ident(c)} IS NOT NULL
                    """
                )

        if default_update_type is not None and "update_type" in cols:
            cur.execute(
                f"""
                UPDATE {raw_table}
                SET update_type = %s
                WHERE update_type IS NULL
                """,
                (default_update_type,),
            )

        if generate_cabd_id_for_new_feature and {"cabd_id", "entry_classification"}.issubset(cols):
            cur.execute(
                f"""
                UPDATE {raw_table}
                SET cabd_id = gen_random_uuid()
                WHERE entry_classification = 'new feature'
                AND cabd_id IS NULL
                """
            )

        cur.execute(
            f"""
            DELETE FROM {raw_table}
            WHERE status IN ('complete', 'do not process', 'on hold')
            """
        )
        if set_status_from_reviewer_comments and {"status", "reviewer_comments"}.issubset(cols):
            cur.execute(
                f"""
                UPDATE {raw_table}
                SET status = CASE
                    WHEN reviewer_comments IS NULL THEN 'ready'
                    ELSE 'needs review'
                END
                WHERE status IS NULL
                """
            )

        try:
            cur.execute(f"ALTER TABLE {raw_table} ADD COLUMN IF NOT EXISTS province_territory_code text;")
            if province_boundary_table:
                schema_table = province_boundary_table
                if "." in schema_table:
                    _, _ = schema_table.split(".", 1)
                else:
                    _ = schema_table

                geom_col = "geometry"
                code_col = province_code_column or "code"
                if geom_col and code_col:
                    update_sql = f"""
                    UPDATE {raw_table} rt
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
                    """
                    cur.execute(update_sql)
        except Exception:
            pass


def _sanitize_identifier(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9_]+", "_", value)
    value = re.sub(r"__+", "_", value)
    return value.strip("_").lower() or "unknown"


def _raw_table_name(staging_table: str) -> str:
    if "." in staging_table:
        schema, table = staging_table.split(".", 1)
        schema = schema.strip('"')
        table = table.strip('"')
        return f"{quote_ident(schema)}.{quote_ident(f'{table}_raw')}"
    return quote_ident(f"{staging_table}_raw")


def _ensure_raw_table(conn, raw_table: str, header: list[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {raw_table} (
                raw_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                source_file text,
                csv_line_no integer,
                load_status text NOT NULL DEFAULT 'pending',
                error_message text,
                error_column text,
                loaded_at timestamptz,
                updated_at timestamptz DEFAULT now(),
                CHECK (load_status IN ('pending', 'error', 'loaded'))
            );
            """
        )

        control_columns = {
            "source_file": "text",
            "csv_line_no": "integer",
            "load_status": "text",
            "error_message": "text",
            "error_column": "text",
            "loaded_at": "timestamptz",
            "updated_at": "timestamptz",
        }
        for col_name, col_type in control_columns.items():
            cur.execute(f"ALTER TABLE {raw_table} ADD COLUMN IF NOT EXISTS {quote_ident(col_name)} {col_type};")

        for col_name in header:
            cur.execute(f"ALTER TABLE {raw_table} ADD COLUMN IF NOT EXISTS {quote_ident(col_name)} text;")


def _value_expression(source_col: str, cast_map: dict | None, target_type: str | None = None) -> str:
    raw_col = quote_ident(source_col)
    if cast_map and source_col.lower() in cast_map:
        target_type = str(cast_map[source_col.lower()]).lower()
    if target_type and "timestamp" in target_type:
        return (
            f"CASE "
            f"WHEN NULLIF(btrim({raw_col}::text), '') IS NULL THEN NULL "
            f"ELSE to_timestamp(NULLIF(btrim({raw_col}::text), ''), 'MM/DD/YYYY HH12:MI AM')::timestamp "
            f"AT TIME ZONE 'America/Toronto' "
            f"END"
        )
    if target_type and "date" in target_type:
        return (
            f"CASE "
            f"WHEN NULLIF(btrim({raw_col}::text), '') IS NULL THEN NULL "
            f"ELSE to_date(NULLIF(btrim({raw_col}::text), ''), 'MM/DD/YYYY') "
            f"END"
        )
    if target_type:
        return f"NULLIF(btrim({raw_col}::text), '')::{target_type}"
    return f"NULLIF(btrim({raw_col}::text), '')"


def _promote_raw_rows_to_staging(
    conn,
    raw_table: str,
    staging_table: str,
    *,
    required_columns: Sequence[str],
    header_lc: dict[str, str],
    cast_map: dict | None,
) -> dict[str, object]:
    insert_cols_sql = ", ".join([quote_ident(c) for c in required_columns])
    insert_value_sql = ", ".join(
        _value_expression(
            header_lc[required_col.lower()],
            cast_map,
            cast_map.get(required_col.lower()) if cast_map else None,
        )
        for required_col in required_columns
    )

    insert_sql = (
        f"INSERT INTO {staging_table} ({insert_cols_sql}) "
        f"SELECT {insert_value_sql} FROM {raw_table} WHERE raw_id = %s;"
    )
    success_update_sql = (
        f"UPDATE {raw_table} "
        f"SET load_status = 'loaded', error_message = NULL, error_column = NULL, loaded_at = now(), updated_at = now() "
        f"WHERE raw_id = %s;"
    )
    error_update_sql = (
        f"UPDATE {raw_table} "
        f"SET load_status = 'error', error_message = %s, error_column = NULL, loaded_at = NULL, updated_at = now() "
        f"WHERE raw_id = %s;"
    )

    inserted_rows = 0
    error_rows = 0
    with conn.cursor() as cur:
        cur.execute(f"SELECT raw_id FROM {raw_table} WHERE load_status <> 'loaded' ORDER BY raw_id;")
        for (raw_id,) in cur.fetchall():
            cur.execute("SAVEPOINT promote_row")
            try:
                cur.execute(insert_sql, (raw_id,))
                cur.execute(success_update_sql, (raw_id,))
                cur.execute("RELEASE SAVEPOINT promote_row")
                inserted_rows += 1
            except Exception as exc:
                cur.execute("ROLLBACK TO SAVEPOINT promote_row")
                cur.execute("RELEASE SAVEPOINT promote_row")
                cur.execute(error_update_sql, (str(exc), raw_id))
                error_rows += 1

    return {
        "inserted_rows": int(inserted_rows),
        "error_rows": int(error_rows),
        "raw_table": raw_table,
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
    """Return a mapping of lowercase column_name -> SQL type (suitable for CREATE TABLE)."""
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
