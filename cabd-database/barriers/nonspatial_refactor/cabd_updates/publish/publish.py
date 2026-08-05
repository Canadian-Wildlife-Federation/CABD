from __future__ import annotations

from cabd_updates.db.call import call_scalar

def publish_data_sources(conn, ds_staging_table: str) -> None:
    call_scalar(conn, "SELECT cabd.upsert_data_sources(%s::regclass)", [ds_staging_table])

def publish_feature(conn, feature: str, staging_table: str) -> None:
    if feature == "dams":
        sql = "SELECT cabd.publish_dam_updates(%s::regclass)"
    elif feature == "fishways":
        sql = "SELECT cabd.publish_fishway_updates(%s::regclass)"
    elif feature == "waterfalls":
        sql = "SELECT cabd.publish_waterfall_updates(%s::regclass)"
    else:
        raise ValueError(f"Unknown feature: {feature}")
    call_scalar(conn, sql, [staging_table])


# TO DO: need to update SharePoint lists with status = 'complete' when all of this is published so we don't re-ingest the updates