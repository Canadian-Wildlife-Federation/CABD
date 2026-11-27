from __future__ import annotations

import csv
from pathlib import Path

# Columns every updates CSV must have, whatever the feature.
REQUIRED_COMMON = (
    "cabd_id",
    "entry_classification",
    "data_source_short_name",
    "submitted_on",
)

# Must match the entry_classification CHECK constraint on the staging tables,
# which is case-sensitive.
ALLOWED_ENTRY = {"new feature", "modify feature", "delete feature"}

# CSV column(s) holding the SharePoint status. Rows with a nonblank value are
# skipped by staging, so they are skipped here too.
SHAREPOINT_STATUS_COLUMNS = ("status", "sharepoint_status")


def validate_updates_csv_basic(path: Path, feature: str) -> list[str]:
    """
    Check an updates CSV before staging. Does not connect to the database.

    Returns a list of problems (empty if the file is OK). Rows that staging
    would skip (blank rows, rows with a nonblank SharePoint status) are skipped
    here as well, so they are never reported.

    Row-level publishing rules (missing data sources, conflicting updates,
    unknown cabd_ids, etc.) are checked after staging by `cabd-updates check`.
    """
    problems: list[str] = []

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            header = [h.strip().lower() for h in next(reader)]
        except StopIteration:
            return [f"{path}: file is empty"]

        missing = [c for c in REQUIRED_COMMON if c not in header]
        if missing:
            return [f"{path}: missing required columns: {missing}"]

        entry_idx = header.index("entry_classification")
        status_idxs = [header.index(c) for c in SHAREPOINT_STATUS_COLUMNS if c in header]

        for line_no, row in enumerate(reader, start=2):
            if not any(cell.strip() for cell in row):
                continue  # blank row; staging skips these
            if any(i < len(row) and row[i].strip() for i in status_idxs):
                continue  # nonblank SharePoint status; staging skips these

            entry = row[entry_idx].strip() if entry_idx < len(row) else ""
            if entry not in ALLOWED_ENTRY:
                shown = repr(entry) if entry else "(blank)"
                problems.append(
                    f"line {line_no}: invalid entry_classification {shown}; "
                    f"expected one of {sorted(ALLOWED_ENTRY)} (lowercase)"
                )

    # Feature-specific checks can be added here
    # if feature == "fishways":
    #     pass

    return problems
