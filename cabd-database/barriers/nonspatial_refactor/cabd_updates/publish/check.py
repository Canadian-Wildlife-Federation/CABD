from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

# Read-only SQL check function per feature. The same function is called by the
# feature's publish function to decide which rows to block, so the preview and
# publish always apply the same rules.
_CHECK_FUNCTIONS = {
    "dams": "cabd.check_dam_updates",
}

_COLUMNS = (
    "id",
    "cabd_id",
    "entry_classification",
    "processing_status",
    "severity",
    "issue_code",
    "issue",
)


def check_feature(conn, feature: str, staging_table: str) -> list[dict]:
    """
    Run the feature's check function against its staging table and return one
    dict per issue. Read-only: nothing in the database is changed.

    Data sources that are staged but not yet published count as available,
    because publish inserts them before publishing features.
    """
    check_fn = _CHECK_FUNCTIONS.get(feature)
    if check_fn is None:
        raise ValueError(f"check is not implemented for feature: {feature}")

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM {check_fn}(%s::regclass, true)",
            (staging_table,),
        )
        return [dict(zip(_COLUMNS, row)) for row in cur.fetchall()]


def print_check_report(staging_table: str, issues: list[dict]) -> None:
    """Print a summary of issues, then one line per row that would be blocked."""
    if not issues:
        print(f"check: no issues found in {staging_table}")
        return

    blocked_rows = {i["id"] for i in issues if i["severity"] == "block"}
    deferred_rows = {i["id"] for i in issues if i["severity"] == "info"}
    print(
        f"check: {staging_table}: {len(blocked_rows)} row(s) would be blocked, "
        f"{len(deferred_rows)} row(s) deferred to a later publish run"
    )

    counts = Counter((i["severity"], i["issue_code"]) for i in issues)
    for (severity, code), count in sorted(counts.items()):
        print(f"  {severity:<5} {code}: {count}")

    blocking = [i for i in issues if i["severity"] == "block"]
    if blocking:
        print("blocking issues:")
        for i in blocking:
            print(
                f"  id {i['id']} ({i['entry_classification']}, "
                f"{i['processing_status']}): {i['issue']}"
            )


def write_check_csv(path: Path, issues: list[dict]) -> None:
    """Write every issue (blocking and informational) to a CSV file."""
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(_COLUMNS))
        writer.writeheader()
        writer.writerows(issues)
