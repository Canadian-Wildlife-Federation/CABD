from __future__ import annotations

from pathlib import Path
import csv
import re
from io import StringIO
from typing import Iterable, Sequence


_NUMERIC_TYPE_HINTS = ("double precision", "numeric", "real", "int", "smallint", "bigint")
_THOUSANDS_RE = re.compile(r"^-?\d{1,3}(,\d{3})+$")

# Code translation tables share a consistent layout across all features:
#   code, name_en, description_en, name_fr, description_fr
_CODE_MATCH_COLUMN = "name_en"   # human-readable English name compared to the CSV
_CODE_VALUE_COLUMN = "code"      # numeric code written back into the raw column


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


def _canonical_sql(expr: str) -> str:
    """SQL: lowercase + strip all non-alphanumeric characters (for matching)."""
    return f"lower(regexp_replace({expr}, '[^a-zA-Z0-9]', '', 'g'))"


def stage_updates_csv_to_table(
    conn,
    csv_path: Path,
    staging_table: str,
    *,
    column_types: dict | None = None,
    coded_values: dict | None = None,
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
    lower_columns: Iterable[str] = ("province_territory_code","data_source_short_name",),
    set_status_from_reviewer_comments: bool = True,
    default_update_type: str | None = "cwf",
    generate_cabd_id_for_new_feature: bool = True,
    province_boundary_table: str | None = "cabd.province_territory_codes",
    province_code_column: str | None = "code",
    province_srid: int = 4617,
) -> dict:
    """Stage CSV updates into a persistent feature raw table and promote rows into staging."""
    if not column_types:
        raise ValueError("column_types must be a non-empty mapping")
    required_columns = list(column_types.keys())
    cast_map = {k.lower(): v for k, v in column_types.items() if v is not None and str(v).strip() != ""}

    # Every coded-value field must also be declared in column_types so it is
    # promoted and cast (to smallint) after translation.
    if coded_values:
        declared = {c.lower() for c in required_columns}
        missing_types = [f for f in coded_values if f.lower() not in declared]
        if missing_types:
            raise ValueError(
                f"coded_values fields not present in column_types: {missing_types}"
            )

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        header = [h.strip() for h in header]
        header_lc = {h.lower(): h for h in header}

    missing = [c for c in required_columns if c.lower() not in header_lc]
    if missing:
        raise ValueError(f"{csv_path}: missing required columns for staging: {missing}")

    raw_table = _raw_table_name(staging_table)

    with conn.cursor() as cur:
        _ensure_raw_table(conn, raw_table, header)
        # Single active batch: drop already-loaded rows, and reset any prior
        # errors back to pending so they are re-validated (auto-retry) this run.
        cur.execute(f"DELETE FROM {raw_table} WHERE load_status = 'loaded';")
        cur.execute(
            f"""
            UPDATE {raw_table}
            SET load_status = 'pending',
                error_message = NULL,
                error_column = NULL,
                loaded_at = NULL,
                updated_at = now()
            WHERE load_status = 'error';
            """
        )

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

    # Normalization (trim, lowercase, status cleanup, cabd_id generation, and the
    # province_territory_code spatial lookup) lives entirely in _normalize_raw_table.
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

    # Translate human-readable coded values into their numeric codes, then flag
    # any unmatched (non-blank) coded values with a friendly, table-specific
    # message so they route to the error log instead of failing the smallint cast.
    if coded_values:
        _translate_coded_values(conn, raw_table, coded_values)
        _flag_unmatched_coded_values(conn, raw_table, coded_values)

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


def _translate_coded_values(conn, raw_table: str, coded_values: dict) -> None:
    """
    Translate human-readable coded-value descriptions in the raw table into their
    numeric codes, IN PLACE, using each field's code translation table.

    Matching is case-insensitive and ignores all non-alphanumeric characters
    (e.g. 'charity/non-profit' matches 'Charity/ Non-profit').

    - Blank/NULL values are left untouched (they become NULL at promotion).
    - Unmatched non-blank values are left as-is here; they are flagged separately
      by _flag_unmatched_coded_values().
    - Idempotent: once a value is '1', its canonical form no longer matches any
      name_en, so re-runs leave it alone.
    """
    raw_cols = _get_table_columns(conn, raw_table)
    match_ident = quote_ident(_CODE_MATCH_COLUMN)
    value_ident = quote_ident(_CODE_VALUE_COLUMN)

    with conn.cursor() as cur:
        for field, cfg in coded_values.items():
            if field.lower() not in raw_cols:
                continue  # field not present in this CSV/raw table; skip
            code_table = cfg["code_table"]
            field_ident = quote_ident(field)
            cur.execute(
                f"""
                UPDATE {raw_table} rt
                SET {field_ident} = ct.{value_ident}::text,
                    updated_at = now()
                FROM {code_table} ct
                WHERE rt.{field_ident} IS NOT NULL
                  AND btrim(rt.{field_ident}) <> ''
                  AND rt.load_status <> 'loaded'
                  AND {_canonical_sql(f'rt.{field_ident}')}
                      = {_canonical_sql(f'ct.{match_ident}')}
                """
            )


def _flag_unmatched_coded_values(conn, raw_table: str, coded_values: dict) -> None:
    """
    Flag rows whose coded-value column still holds a non-blank value that is NOT a
    valid code in its translation table (i.e. translation found no match).

    Sets load_status = 'error' with a friendly, table-specific message so the
    reviewer can see exactly which value / which code table failed. The first
    unmatched coded column per row wins the message (the 'pending' guard prevents
    a later column from overwriting it).
    """
    raw_cols = _get_table_columns(conn, raw_table)
    value_ident = quote_ident(_CODE_VALUE_COLUMN)

    with conn.cursor() as cur:
        for field, cfg in coded_values.items():
            if field.lower() not in raw_cols:
                continue
            code_table = cfg["code_table"]
            field_ident = quote_ident(field)
            cur.execute(
                f"""
                UPDATE {raw_table} rt
                SET load_status = 'error',
                    error_message = 'coded value '
                        || quote_literal(btrim(rt.{field_ident}))
                        || ' not found in {code_table} (column {field})',
                    error_column = %s,
                    loaded_at = NULL,
                    updated_at = now()
                WHERE rt.load_status = 'pending'
                  AND rt.{field_ident} IS NOT NULL
                  AND btrim(rt.{field_ident}) <> ''
                  AND NOT EXISTS (
                      SELECT 1 FROM {code_table} ct
                      WHERE ct.{value_ident}::text = btrim(rt.{field_ident})
                  )
                """,
                (field,),
            )


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

        # Populate province_territory_code from the authoritative boundary table
        # via PostGIS. Non-fatal: a failure here (missing extension, bad geometry,
        # etc.) is reported but does not block staging.
        try:
            cur.execute(f"ALTER TABLE {raw_table} ADD COLUMN IF NOT EXISTS province_territory_code text;")
            if province_boundary_table:
                geom_col = "geometry"
                code_col = province_code_column or "code"
                update_sql = f"""
                UPDATE {raw_table} rt
                SET province_territory_code = b.{quote_ident(code_col)}
                FROM {province_boundary_table} b
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
        except Exception as exc:
            print(f"warning: province_territory_code spatial lookup skipped: {exc}")


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
        # Only promote 'pending' rows. Rows already flagged 'error' (e.g. by the
        # coded-value check) are skipped so their friendly message is preserved;
        # they are reset to 'pending' and re-validated on the next run.
        cur.execute(f"SELECT raw_id FROM {raw_table} WHERE load_status = 'pending' ORDER BY raw_id;")
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


def quote_ident(ident: str) -> str:
    ident = ident.replace('"', '""')
    return f'"{ident}"'
