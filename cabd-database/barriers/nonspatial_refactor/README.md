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
Apply SQL files in `sql/` using psql (order is up to you, but constraints then functions is typical).

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