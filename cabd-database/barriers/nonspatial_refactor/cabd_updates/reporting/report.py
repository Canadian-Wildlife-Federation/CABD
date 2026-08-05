from __future__ import annotations

from cabd_updates.db.call import call_rows

def report_feature(conn, feature: str, staging_table: str) -> None:
    rows = call_rows(
        conn,
        f"""
        SELECT update_status, COUNT(*) AS n
        FROM {staging_table}
        GROUP BY update_status
        ORDER BY update_status NULLS FIRST
        """
    )
    print(f"Report for {feature} ({staging_table}):")
    for r in rows:
        print(f"  {r['update_status']}: {r['n']}")
