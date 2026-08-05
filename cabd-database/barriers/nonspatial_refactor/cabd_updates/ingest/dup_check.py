from __future__ import annotations

from pathlib import Path
import pandas as pd

def dup_check_csv(path: Path) -> Path:
    df = pd.read_csv(path, encoding="utf-8", dtype=str)
    if "status" in df.columns:
        df = df[df["status"].fillna("").str.lower() != "complete"]

    colskip = {
        "submitted_on","email","latitude","longitude","reviewer_comments",
        "entry_classification","data_source_short_name","use_analysis",
        "name","organization","status","release_version",
    }
    df.columns = [c.strip() for c in df.columns]
    if "cabd_id" not in df.columns:
        raise ValueError("dup_check requires cabd_id column")

    dups = df[df.duplicated("cabd_id", keep=False)]
    dups = dups[dups["cabd_id"].notna()].copy()

    out_rows = []
    for col in dups.columns:
        if col in colskip or col == "cabd_id":
            continue
        g = dups.groupby("cabd_id")[col].nunique(dropna=True)
        conflict_ids = g[g > 1].index.tolist()
        if not conflict_ids:
            continue
        for cabd_id in conflict_ids:
            sub = dups[dups["cabd_id"] == cabd_id]
            out_rows.append({
                "cabd_id": cabd_id,
                "colname": col,
                "conflict": sorted(set(sub[col].dropna().astype(str).tolist())),
                "emails": sorted(set(sub.get("email", pd.Series([], dtype=str)).dropna().astype(str).tolist())),
                "entry_classification": sorted(set(sub["entry_classification"].dropna().astype(str).tolist())) if "entry_classification" in sub.columns else [],
            })

    out = path.parent / "dup_conflicts.csv"
    pd.DataFrame(out_rows).to_csv(out, index=False)
    return out
