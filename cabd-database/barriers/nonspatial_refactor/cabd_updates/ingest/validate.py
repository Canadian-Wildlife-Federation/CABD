from __future__ import annotations

from pathlib import Path
import pandas as pd

REQUIRED_COMMON = {
    "cabd_id",
    "entry_classification",
    "data_source_short_name",
    "submitted_on",
}

def validate_updates_csv_basic(path: Path, feature: str) -> None:
    df = pd.read_csv(path)
    cols = set(df.columns.str.lower())
    missing = [c for c in REQUIRED_COMMON if c not in cols]
    if missing:
        raise ValueError(f"{path}: missing required columns: {missing}")

    allowed_entry = {"new feature", "modify feature", "delete feature"}
    bad = df[~df["entry_classification"].astype(str).str.lower().isin(allowed_entry)]
    if not bad.empty:
        raise ValueError(f"{path}: invalid entry_classification values present")

    # Feature-specific checks can be added here
    # if feature == "fishways":
    #     pass
