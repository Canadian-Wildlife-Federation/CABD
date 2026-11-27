# Setup and usage

Requires **Python 3.10 or newer**. Commands assume PowerShell, run from the repo
root (`cabd-database/barriers/nonspatial_refactor`).

## First-time setup

### 1. Create the virtual environment

Check that Python 3.10+ is installed with `py -0`. If it isn't, install it with
`winget install Python.Python.3.10` or from https://www.python.org/downloads/,
then reopen the terminal.

If you're replacing an existing `.venv` (for example, to change Python version),
deactivate and delete it first:

```powershell
deactivate
Remove-Item -Recurse -Force .venv
```

Create and activate the venv:

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks the activation script, run
`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once, then activate again.

### 2. Install the package

```powershell
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e .
cabd-updates --help
```

Use `python`, not `py`, while the venv is active. `py` runs the global Python
and would install into the wrong environment.

### 3. Configure the database connection

Create `env.ps1` in the repo root with your connection details. The file is
gitignored, so your password is never committed.

```powershell
$env:CABD_DSN = "host=localhost port=5432 dbname=cabd user=YOUR_USER password=YOUR_PASSWORD"
```

### 4. Point VS Code at the venv

Press `Ctrl+Shift+P`, run **Python: Select Interpreter**, and choose the `.venv`
interpreter. New VS Code terminals will then activate the venv automatically.

## Database migrations (once per database)

Apply these to **both dev and production** before running the CLI against
either. Back up the staging tables first.

1. Run `sql/migrations/separate_update_status_columns.sql`.
2. Apply the relevant files in `sql/constraints/`.
3. Apply the functions in `sql/functions/`.

The migration can safely be run more than once and runs as a single
transaction. Review its output for notices or errors. It does the following:

- Creates the `sharepoint_status` and `processing_status` columns.
- **Feature updates:** moves known workflow values to `processing_status` and
  keeps any other legacy values as `sharepoint_status`.
- **Data-source updates:** treats legacy `update_status` values as
  `sharepoint_status`. The exception is a legacy `blocked` value with a
  nonblank `blocked_reason`, which is restored as `processing_status = 'blocked'`.
- Deletes rows with a nonblank `sharepoint_status`, since the importers skip them.
- Drops the old `status` / `update_status` columns.

## Daily use

In each new terminal, activate the venv (if VS Code hasn't already) and load
the connection settings:

```powershell
.\.venv\Scripts\Activate.ps1
. .\env.ps1
```

The leading `. ` before `.\env.ps1` is required. Without it, `CABD_DSN` is lost
as soon as the script finishes.

### Typical workflow

```powershell
cabd-updates validate --feature dams --updates path\to\updates.csv
cabd-updates stage --feature dams --updates path\to\updates.csv
cabd-updates stage-data-sources --sources path\to\data_sources.csv
cabd-updates publish --feature dams
cabd-updates report --feature dams
```

- `--feature` accepts `dams`, `fishways` or `waterfalls`.
- `publish` publishes the staged data sources first, then the feature updates.
- `stage`, `stage-data-sources`, `publish` and `report` accept
  `--staging-table schema.table` to override the table set in the YAML config.
- To run a single command against another database (for example, dev), pass
  `--dsn` before the command name:

  ```powershell
  cabd-updates --dsn "host=localhost port=5432 dbname=cabd_dev_2026 user=YOUR_USER password=YOUR_PASSWORD" report --feature dams
  ```

## How staging and publishing work

### Status columns

- `sharepoint_status` holds the original status from SharePoint. A `status`
  column in an incoming CSV is loaded here.
- `processing_status` holds the CABD workflow state: `needs review`, `ready`,
  `wait`, `blocked`, and `done` (data-source updates only). Staging sets it.

Rows with any nonblank `sharepoint_status` are skipped and counted, because
SharePoint values aren't controlled or validated. Blank and whitespace-only
values are processed.

### Staging

Staging order doesn't matter. Each feature and the data sources have their own
staging table.

- Blank rows in update and data-source CSVs are skipped.

### Publish order

If you are publishing updates to multiple feature types, publish in this order: **dams → fishways → waterfalls**.

- **Dams before fishways.** Fishways reference dams through `dam_id`, so any
  new dams must exist first.
- **Waterfalls** have no link to dams or fishways and can be published at any
  time.

If fishways are published before dams, nothing breaks. Fishways with a valid or
NULL `dam_id` still publish. Those that reference a dam not yet published are
blocked and stay in staging. Publish dams, then rerun the fishway publish.

### Blocked and deferred rows

- **Missing data source:** the row is blocked and automatically retried on the
  next publish. Add the source, then rerun publish.
- **Delete followed by a later modify** (by `submitted_on`) for the same
  feature: both rows are blocked with a conflict reason, and neither publishes
  until you resolve the conflict.
- **No `submitted_on`:** the row is blocked, because its order can't be
  determined.
- **Several ready modify/delete updates for one dam:** these publish one at a
  time, in `submitted_on` order, over successive runs.

## Maintenance

- The install is editable, so code changes take effect immediately. Run
  `python -m pip install -e .` again only after changing `pyproject.toml`.
- To rebuild the environment, repeat First-time setup steps 1 and 2.