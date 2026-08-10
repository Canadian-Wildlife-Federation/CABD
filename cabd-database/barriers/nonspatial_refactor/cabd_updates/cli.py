from __future__ import annotations

import argparse
import os
from pathlib import Path

from cabd_updates.config import load_config
from cabd_updates.db.connect import connect
from cabd_updates.ingest.validate import validate_updates_csv_basic
from cabd_updates.staging.stage_updates import stage_updates_csv_to_table
from cabd_updates.staging.stage_data_sources import stage_data_sources_csv_to_table
from cabd_updates.publish.publish import publish_feature, publish_data_sources
from cabd_updates.reporting.report import report_feature

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cabd-updates")
    p.add_argument("--dsn", default=os.getenv("CABD_DSN"), help="Postgres DSN (or set CABD_DSN)")
    sub = p.add_subparsers(dest="cmd", required=True)

    # validate
    pv = sub.add_parser("validate", help="Validate an updates CSV and optionally run dup check.")
    pv.add_argument("--feature", required=True, choices=["dams", "fishways", "waterfalls"])
    pv.add_argument("--updates", required=True, help="Path to updates CSV")
    pv.add_argument("--dup-check", action="store_true", help="Run duplicate/conflict check")

    # stage updates
    ps = sub.add_parser("stage", help="Stage updates CSV into configured staging table (COPY).")
    ps.add_argument("--feature", required=True, choices=["dams", "fishways", "waterfalls"])
    ps.add_argument("--updates", required=True, help="Path to updates CSV")
    ps.add_argument("--staging-table", default=None, help="Override staging table (schema.table)")

    # stage data sources
    pds = sub.add_parser("stage-data-sources", help="Stage data sources CSV into cabd.data_source_updates (COPY).")
    pds.add_argument("--sources", required=True, help="Path to data sources CSV")
    pds.add_argument("--staging-table", default=None, help="Override staging table (schema.table)")

    # publish
    pp = sub.add_parser("publish", help="Publish staged updates into live tables via DB functions.")
    pp.add_argument("--feature", required=True, choices=["dams", "fishways", "waterfalls"])
    pp.add_argument("--staging-table", default=None, help="Override staging table (schema.table)")
    pp.add_argument("--data-sources-staging-table", default=None, help="Override data source staging table (schema.table)")

    # report
    pr = sub.add_parser("report", help="Report on staging table status counts.")
    pr.add_argument("--feature", required=True, choices=["dams", "fishways", "waterfalls"])
    pr.add_argument("--staging-table", default=None, help="Override staging table (schema.table)")

    return p

def main() -> None:
    p = build_parser()
    args = p.parse_args()

    if not args.dsn and args.cmd in ("stage", "stage-data-sources", "publish", "report"):
        raise SystemExit("Missing DSN. Pass --dsn or set CABD_DSN.")

    if args.cmd == "validate":
        validate_updates_csv_basic(Path(args.updates), feature=args.feature)
        if args.dup_check:
            out = dup_check_csv(Path(args.updates))
            print(f"dup_check: wrote {out}")
        print("validate: OK")
        return

    with connect(args.dsn) as conn:
        if args.cmd == "stage":
            cfg = load_config(args.feature)
            staging_table = args.staging_table or cfg["staging_table"]
            column_types = cfg.get("column_types") or {}
            stage_updates_csv_to_table(
                conn,
                Path(args.updates),
                staging_table=staging_table,
                column_types=column_types,
            )
            print(f"stage: loaded {args.feature} updates into {staging_table}")
            return

        if args.cmd == "stage-data-sources":
            cfg = load_config("data_sources")
            staging_table = args.staging_table or cfg["staging_table"]
            stage_data_sources_csv_to_table(conn, Path(args.sources), staging_table=staging_table)
            print(f"stage-data-sources: loaded data sources into {staging_table}")
            return

        if args.cmd == "publish":
            cfg = load_config(args.feature)
            staging_table = args.staging_table or cfg["staging_table"]
            ds_cfg = load_config("data_sources")
            ds_staging = args.data_sources_staging_table or ds_cfg["staging_table"]

            publish_data_sources(conn, ds_staging_table=ds_staging)
            publish_feature(conn, feature=args.feature, staging_table=staging_table)
            print("publish: OK")
            return

        if args.cmd == "report":
            cfg = load_config(args.feature)
            staging_table = args.staging_table or cfg["staging_table"]
            report_feature(conn, feature=args.feature, staging_table=staging_table)
            return

if __name__ == "__main__":
    main()
    