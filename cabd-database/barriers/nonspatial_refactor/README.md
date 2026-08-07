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
