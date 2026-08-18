from __future__ import annotations

from cabd_updates.config import load_config
from cabd_updates.db.call import call_scalar


# ---------------------------------------------------------------------------
# Column derivation (single source of truth = feature YAML column_types)
# ---------------------------------------------------------------------------

# Columns that don't need a comparison to live data
# or are workflow/staging-only columns
_EXCLUDED_COLUMNS = {
    "cabd_id",
    "province_territory_code",
    "entry_classification",
    "data_source_short_name",
    "status",
    "reviewer_comments",
    "submitted_on",
    "update_type",
}

# Columns that are always CWF-team determined and therefore have NO _ds
# provenance counterpart (still written to the live table, just not tracked).
_NO_PROVENANCE_COLUMNS = {"use_analysis"}

# Live table per feature (used for the defensive "is this a real column?" skip).
_LIVE_TABLE = {
    "dams": "dams.dams",
    "fishways": "fishways.fishways",
    "waterfalls": "waterfalls.waterfalls",
}


def _get_live_columns(conn, table_name: str) -> set[str]:
    """Return the set of lowercase column names on a schema-qualified table."""
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


def _derive_publish_columns(
    column_types: dict, live_columns: set[str]
) -> tuple[list[str], list[str]]:
    """
    Derive (attr_columns, ds_columns) from a feature's YAML column_types.

    attr_columns: publishable attributes (INCLUDES use_analysis), in YAML order,
        excluding identity/workflow columns and any column not present on the
        live table (defensive skip).
    ds_columns:   attr_columns MINUS the no-provenance columns (use_analysis).
    """
    attr_columns: list[str] = []
    for col in column_types.keys():
        cl = col.lower()
        if cl in _EXCLUDED_COLUMNS:
            continue
        if cl not in live_columns:
            continue  # defensive skip: not a real column on the live table
        attr_columns.append(col)

    ds_columns = [c for c in attr_columns if c.lower() not in _NO_PROVENANCE_COLUMNS]
    return attr_columns, ds_columns


# ---------------------------------------------------------------------------
# Publish entry points
# ---------------------------------------------------------------------------

def publish_data_sources(conn, ds_staging_table: str) -> None:
    call_scalar(conn, "SELECT cabd.upsert_data_sources(%s::regclass)", [ds_staging_table])


def publish_feature(conn, feature: str, staging_table: str) -> None:
    if feature == "dams":
        # Column-driven publish: derive the attribute + provenance column lists
        # from the dams YAML column_types and pass them into the SQL function.
        cfg = load_config(feature)
        column_types = cfg.get("column_types") or {}
        live_columns = _get_live_columns(conn, _LIVE_TABLE["dams"])
        attr_columns, ds_columns = _derive_publish_columns(column_types, live_columns)
        call_scalar(
            conn,
            "SELECT cabd.publish_dam_updates(%s::regclass, %s, %s)",
            [staging_table, attr_columns, ds_columns],
        )
    elif feature == "fishways":
        sql = "SELECT cabd.publish_fishway_updates(%s::regclass)"
        call_scalar(conn, sql, [staging_table])
    elif feature == "waterfalls":
        sql = "SELECT cabd.publish_waterfall_updates(%s::regclass)"
        call_scalar(conn, sql, [staging_table])
    else:
        raise ValueError(f"Unknown feature: {feature}")


# TO DO: need to update SharePoint lists with status = 'complete' when all of this
# is published so we don't re-ingest the updates
