from __future__ import annotations

from pathlib import Path
from io import StringIO
from datetime import datetime
import csv
import re

from cabd_updates.db.call import call_scalar


# CSV column name -> staging table column name
COLUMN_RENAMES = {
    "data_source_short_name": "name",
    "last_updated": "version_date",
    "reference": "source",
    "data_source_type": "data_source_category",
    "status": "update_status",
}


# CSV columns to ignore entirely (matched by ORIGINAL name, pre-rename).
# The original 'name' column is unrelated to the staging 'name' (which comes
# from data_source_short_name), so drop it to avoid a collision.
IGNORE_SOURCE_COLUMNS = {"name"}


# Only these columns get copied into staging (names are POST-rename).
STAGING_COLUMNS = [
    "id",
    "name",
    "version_date",
    "source",
    "source_type",
    "full_name",
    "organization_name",
    "data_source_category",
    "submitted_on",
    "update_status",
]


# Rows whose update_status (post-rename) is one of these are dropped BEFORE COPY.
# Comparison is case-insensitive and whitespace-trimmed.
DROP_STATUSES = {"complete", "do not process", "on hold"}


# Explicit date formats tried for version_date (order doesn't matter; all tried).
# strptime accepts non-zero-padded input, so '%Y-%m-%d' also covers '2022-10-7'.
# Month-name formats default the day to the 1st when no day is present
# (e.g. 'October 1993' -> 1993-10-01).
_VERSION_DATE_FORMATS = (
    "%Y-%m-%d",       # 1997-06-10  (ISO dash; also 2022-10-7)
    "%Y/%m/%d",       # 2023/10/02  (ISO slash; also 2005/10/5)
    "%m/%d/%Y",       # 6/10/1997
    "%m-%d-%Y",       # 06-10-1997
    "%B %d, %Y",      # July 1, 2024   /  January 6, 2023
    "%B %d %Y",       # July 1 2024
    "%d %B, %Y",      # 16 March, 2022
    "%d %B %Y",       # 16 March 2022
    "%B %Y",          # October 1993   -> 1st of month (day defaults to 1)
    "%b %d, %Y",      # Jul 1, 2024     (abbreviated month)
    "%b %d %Y",       # Jul 1 2024
    "%b %Y",          # Oct 1993        -> 1st of month
)

def _is_no_date(value: str) -> bool:
    """Return True for n.d. / n/a / no date / none / unknown variants."""
    letters = re.sub(r"[^a-z]", "", value.lower())
    return letters in {"nd", "na", "nodate", "none", "unknown"}


def _parse_version_date(value: str) -> str | None:
    """
    Normalize a messy date string to ISO 'YYYY-MM-DD', or None.

    Handles:
      - year only            -> Jan 1 of that year   (2018 -> 2018-01-01)
      - year-month only      -> 1st of that month    (2014/01 -> 2014-01-01)
      - n.d./n/a/no date etc -> None
      - ISO 'YYYY-MM-DD' / 'YYYY/MM/DD' (padded or not)
      - MM/DD/YYYY, MM-DD-YYYY
      - YYYYMMDD             (20221219)
      - month names          ('July 1, 2024' -> 2024-07-01;
                              'October 1993' -> 1993-10-01, 1st of month)
    """
    if value is None:
        return None

    v = value.strip().rstrip(".,").strip()  # drop trailing . or , and spaces
    if v == "" or _is_no_date(v):
        return None

    # Year only -> Jan 1
    if re.fullmatch(r"\d{4}", v):
        return f"{v}-01-01"

    # Year + month only (YYYY-MM or YYYY/MM) -> 1st of that month
    ym = re.fullmatch(r"(\d{4})\d{1,2}", v)
    if ym:
        year, month = ym.group(1), int(ym.group(2))
        if 1 <= month <= 12:
            return f"{year}-{month:02d}-01"
        return None

    # YYYYMMDD compact
    if re.fullmatch(r"\d{8}", v):
        try:
            return datetime.strptime(v, "%Y%m%d").date().isoformat()
        except ValueError:
            return None

    # Try a series of explicit formats (includes month-name variants)
    for fmt in _VERSION_DATE_FORMATS:
        try:
            return datetime.strptime(v, fmt).date().isoformat()
        except ValueError:
            continue

    # Unrecognized -> None (safer than guessing)
    return None


def _parse_submitted_on(value: str) -> str | None:
    """
    Normalize submitted_on to ISO 'YYYY-MM-DD HH:MM:SS', or None.

    Handles the M/D/YYYY h:MM AM/PM form (e.g. '4/16/2026 3:22 PM') plus a few
    common variants. The value is stored naive; the session TimeZone is set to
    America/Toronto before COPY so it is interpreted correctly.
    """
    if value is None:
        return None

    v = value.strip()
    if v == "":
        return None

    for fmt in (
        "%m/%d/%Y %I:%M %p",   # 4/16/2026 3:22 PM
        "%m/%d/%Y %H:%M",      # 4/16/2026 15:22
        "%m/%d/%Y",            # 4/16/2026 (date only)
        "%Y-%m-%d %H:%M:%S",   # already ISO-ish
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(v, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    return None


def _intended_null(value: str) -> bool:
    """
    True if a value is *meant* to become NULL (empty or a no-date variant),
    so it should NOT be reported as an unparsed date.
    """
    if value is None:
        return True
    v = value.strip().rstrip(".,").strip()
    return v == "" or _is_no_date(v)


def stage_data_sources_csv_to_table(
    conn,
    csv_path: Path,
    staging_table: str = "cabd.data_source_updates",
    dedupe_on: str = "name",
) -> dict:
    """
    Stage data sources into cabd.data_source_updates using COPY, then perform a
    small amount of normalization in SQL.

    Expected CSV columns (recommended):
        - data_source_short_name  (renamed to name on import)
        - last_updated            (renamed to version_date on import)
        - reference               (renamed to source on import)
        - source_type
        - full_name
        - organization_name
        - data_source_type        (renamed to data_source_category on import)
        - submitted_on
        - status                  (renamed to update_status on import)
        - id (optional; if absent, IDs will be generated)

    Notes:
        - The original CSV 'name' column is ignored (see IGNORE_SOURCE_COLUMNS);
          the staging 'name' comes from data_source_short_name.
        - Rows whose status (update_status) is in DROP_STATUSES are dropped in
          Python BEFORE COPY.
        - Only columns listed in STAGING_COLUMNS (post-rename) are copied into
          the staging table; all other CSV columns are ignored.
        - De-duplication happens in Python BEFORE COPY: one row per name,
          preferring the FIRST submitted record (earliest submitted_on, NULLs
          last), with CSV order as a deterministic tie-breaker.
        - version_date values are parsed/normalized before COPY:
          year-only -> Jan 1; year-month -> 1st of month; n.d. variants -> NULL.
        - submitted_on is parsed to ISO before COPY; the session TimeZone is set
          to America/Toronto so naive values are interpreted correctly.
        - Values that are neither a valid date nor an intended-NULL are set to
          NULL and printed to the console so new/unexpected formats can be caught.
        - This function intentionally does NOT restrict to sources referenced by
          staged updates. That restriction is enforced inside
          cabd.upsert_data_sources().
        - This function does NOT publish; it only stages.
    """

    # Read header; drop ignored source columns FIRST, then apply renames
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        header = [h.strip() for h in header]

    # Positions of columns whose ORIGINAL name isn't ignored
    source_keep_indices = [
        i for i, h in enumerate(header) if h.lower() not in IGNORE_SOURCE_COLUMNS
    ]

    # Apply renames only to the surviving columns
    header = [
        COLUMN_RENAMES.get(header[i].lower(), header[i]) for i in source_keep_indices
    ]

    # Restrict to desired staging columns (post-rename), preserving CSV order
    keep_pairs = [(pos, h) for pos, h in enumerate(header) if h in STAGING_COLUMNS]
    # Map reduced-header positions back to ORIGINAL CSV column indices
    keep_indices = [source_keep_indices[pos] for pos, _ in keep_pairs]
    copy_header = [h for _, h in keep_pairs]

    if not copy_header:
        raise ValueError(
            f"{csv_path}: none of the CSV columns match STAGING_COLUMNS "
            f"after renaming; nothing to copy"
        )

    # Create staging table if it doesn't exist
    # (based on cabd.data_source structure)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {staging_table}
            (LIKE cabd.data_source INCLUDING ALL);

            ALTER TABLE {staging_table}
            ALTER COLUMN "id" DROP NOT NULL;

            ALTER TABLE {staging_table}
            ADD COLUMN IF NOT EXISTS submitted_on timestamptz;

            ALTER TABLE {staging_table}
            ADD COLUMN IF NOT EXISTS update_status text;
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

    # Locate the relevant columns in the FILTERED header
    name_idx = copy_header.index("name") if "name" in copy_header else None
    version_date_idx = (
        copy_header.index("version_date") if "version_date" in copy_header else None
    )
    submitted_on_idx = (
        copy_header.index("submitted_on") if "submitted_on" in copy_header else None
    )
    update_status_idx = (
        copy_header.index("update_status") if "update_status" in copy_header else None
    )

    # Track values that could not be parsed (and were not intended NULLs)
    unparsed_dates: list[tuple[int, str]] = []
    unparsed_submitted: list[tuple[int, str]] = []
    dropped_status_count = 0

    # ---- Pass 1: read + clean rows (drop by status, parse dates) ----
    cleaned_rows: list[tuple[int, list[str]]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip original header
        for line_no, row in enumerate(reader, start=2):
            # keep only the desired columns, using ORIGINAL CSV positions
            row = [row[i] if i < len(row) else "" for i in keep_indices]

            # Drop rows with a terminal/hold status BEFORE any further work
            if update_status_idx is not None and update_status_idx < len(row):
                status_val = row[update_status_idx].strip().lower()
                if status_val in DROP_STATUSES:
                    dropped_status_count += 1
                    continue

            if version_date_idx is not None and version_date_idx < len(row):
                original = row[version_date_idx]
                parsed = _parse_version_date(original)
                if parsed is None and not _intended_null(original):
                    unparsed_dates.append((line_no, original))
                row[version_date_idx] = parsed if parsed is not None else ""

            if submitted_on_idx is not None and submitted_on_idx < len(row):
                original = row[submitted_on_idx]
                parsed = _parse_submitted_on(original)
                if parsed is None and original.strip() != "":
                    unparsed_submitted.append((line_no, original))
                row[submitted_on_idx] = parsed if parsed is not None else ""

            cleaned_rows.append((line_no, row))

    # ---- De-dupe BEFORE COPY: one row per name, earliest submitted_on wins ----
    best_by_name: dict[str, tuple[tuple, int, list[str]]] = {}
    passthrough: list[tuple[int, list[str]]] = []
    for line_no, row in cleaned_rows:
        name_key = row[name_idx].strip() if name_idx is not None else ""
        if name_key == "":
            passthrough.append((line_no, row))
            continue
        submitted_val = row[submitted_on_idx] if submitted_on_idx is not None else ""
        # NULLS LAST via the leading flag; ISO strings sort chronologically;
        # line_no gives the deterministic tie-break (mirrors the old ctid ASC).
        sort_key = (0 if submitted_val else 1, submitted_val, line_no)
        existing = best_by_name.get(name_key)
        if existing is None or sort_key < existing[0]:
            best_by_name[name_key] = (sort_key, line_no, row)

    survivors = passthrough + [(ln, r) for (_, ln, r) in best_by_name.values()]
    survivors.sort(key=lambda t: t[0])  # write in original CSV order

    # ---- Pass 2: write de-duplicated rows to the COPY buffer and load ----
    with StringIO() as buffer:
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(copy_header)
        for _, row in survivors:
            writer.writerow(row)
        buffer.seek(0)

        # COPY into staging table (empty cells -> SQL NULL). Set the session
        # time zone first so naive submitted_on values are read as Toronto local.
        copy_sql = (
            f"COPY {staging_table} "
            f"({', '.join([quote_ident(h) for h in copy_header])}) "
            f"FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')"
        )
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'America/Toronto';")
            cur.copy_expert(copy_sql, buffer)

    # Normalize inside the staging table (dedupe already handled in Python)
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

    row_count = call_scalar(
        conn,
        "SELECT COUNT(*) FROM " + staging_table,
    )

    # Report drops / unparsed values to the console
    if dropped_status_count:
        print(
            f"{csv_path}: dropped {dropped_status_count} row(s) with "
            f"status in {sorted(DROP_STATUSES)} before COPY"
        )

    if unparsed_dates:
        print(
            f"{csv_path}: {len(unparsed_dates)} version_date value(s) "
            f"could not be parsed and were set to NULL:"
        )
        for line_no, original in unparsed_dates:
            print(f"  line {line_no}: unparsed version_date {original!r} -> NULL")

    if unparsed_submitted:
        print(
            f"{csv_path}: {len(unparsed_submitted)} submitted_on value(s) "
            f"could not be parsed and were set to NULL:"
        )
        for line_no, original in unparsed_submitted:
            print(f"  line {line_no}: unparsed submitted_on {original!r} -> NULL")

    return {
        "staging_table": staging_table,
        "row_count": int(row_count or 0),
        "dropped_status_rows": dropped_status_count,
        "unparsed_version_dates": unparsed_dates,
        "unparsed_submitted_on": unparsed_submitted,
    }


def quote_ident(ident: str) -> str:
    ident = ident.replace('"', '""')
    return f'"{ident}"'
