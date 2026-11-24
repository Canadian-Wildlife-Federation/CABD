# CABD nonspatial updates refactor (scaffold)

## Install (example)
```

python -m venv .venv

\\path\\to\\env\\Scripts\\activate

pip install -r requirements.txt

```

## Configure DB connection
```

set CABD_DSN=host=... port=5432 dbname=cabd user=... password=...

```

## Run migrations
Before using the updated staging or publishing scripts, run
`sql/migrations/separate_update_status_columns.sql` against **both dev and
production**. It is an idempotent, transactional migration: it creates
`sharepoint_status` and `processing_status`. For feature updates it maps known
workflow values to `processing_status` and preserves other legacy values as
`sharepoint_status`. Because data-source `update_status` may contain original
SharePoint values, it treats those values as `sharepoint_status`; a legacy
`blocked` value with a nonblank `blocked_reason` is instead restored as an
internal `blocked` state. It removes
rows with nonblank SharePoint statuses, which the importers skip, and drops the
ambiguous legacy `status` / `update_status` columns. Back up the staging tables
first and review the migration output for notices or errors.

Then apply the relevant files in `sql/constraints/`, followed by the functions
in `sql/functions/`. Do not run the updated CLI against a database until the
migration and updated constraints/functions have been applied.

`sharepoint_status` is the original status supplied by SharePoint. `processing_status`
is the CABD workflow state (`needs review`, `ready`, `wait`, `blocked`, and, for
data-source updates, `done`). Incoming `status` CSV columns are loaded into
`sharepoint_status`; the staging process sets `processing_status` itself. Any
nonblank `sharepoint_status` value is skipped because the SharePoint values are
not controlled or validated; blank and whitespace-only values remain eligible
for processing.

## CLI
```

cabd-updates --help

Typical workflow (end-to-end)
​
cabd-updates validate --feature dams --updates updates.csv
cabd-updates stage --feature dams --updates updates.csv --staging-table cabd.dam_updates
cabd-updates stage-data-sources --sources data_sources.csv --staging-table cabd.data_source_updates
cabd-updates publish --feature dams
cabd-updates report --feature dams

```
### Order of updates
Order matters for publishing but not for staging/importing.
1) Import / stage order (CSV → staging tables)
You can stage in any order:
    stage-data-sources (into cabd.data_source_updates)
    stage --feature dams (into cabd.dam_updates)
    stage --feature fishways (into cabd.fishway_updates)
    stage --feature waterfalls (into cabd.waterfall_updates)
The staging tables are independent; staging fishways first won’t break anything.
Repeated new-feature rows are skipped when the same source and submitted feature
details (including coordinates) have already been imported, even after the
original update has been published. Blank rows in updates and data-source CSV
files are skipped. Rows with nonblank SharePoint statuses are skipped and counted.
The per-feature `*_import_fingerprints` table retains these
keys; remove a row from it only when intentionally re-importing that exact
feature update.
2) Publish order (staging → live tables)
Here’s the recommended order:
Upsert data sources
Run upsert_data_sources first so data_source_short_name can resolve to cabd.data_source.id. Otherwise, rows will be considered “unresolved” and kept in staging.
Publish dams
This ensures any new dams exist before fishways try to reference them via fishways.dam_id.
Publish fishways
Fishways reference dams. If you publish fishways first, any rows whose dam_id doesn’t exist yet will be marked blocked (per your blocked-row handling) and won’t publish until you retry after dams are published.
Publish waterfalls (this can happen at any time after upsert data sources)
Waterfalls don't have any relationship to fishways or dams in the CABD data structure.
So the “safe default” publish sequence is:
data sources → dams → fishways → waterfalls
3) If you accidentally publish fishways before dams
Nothing catastrophic happens:
Fishway rows with valid/NULL dam_id still publish (assuming data_source is resolved).
Fishway rows referencing missing dams become blocked and remain in staging.
After publishing dams, you just re-run fishway publish to pick up the previously blocked rows.
Rows blocked because their data source is missing are automatically retried on
the next publish; add the source and rerun publish.

If a feature has a delete update followed by a later modify update (by
`submitted_on`), both rows are blocked with a conflict reason and neither is
published until the conflict is resolved.
Updates without `submitted_on` are blocked because their order cannot be
determined. For dams, ready modify/delete actions for the same feature are
published one at a time in `submitted_on` order.