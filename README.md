# GH Archive Lakehouse on Databricks

GitHub public events from [GH Archive](https://www.gharchive.org/) flow through a Delta Lake
medallion pipeline on Databricks Free Edition:

```
GH Archive hourly files -> UC volume (landing) -> bronze -> silver -> gold -> dashboard
```

> Work in progress. Architecture, design decisions and trade-offs are documented as the
> pipeline is built.

## Setup

Everything runs in a Docker container: Python, Spark, Java and the Databricks CLI are
installed in the image, not on your machine. You need Docker (with WSL integration enabled
on Windows), `make` and `git`.

### 1. Build the dev image

```bash
make build      # also creates .env from .env.example on first run
```

The image pins Python 3.12 (conda base env) and PySpark 4.2.0 to match Databricks
serverless, plus Delta Lake 4.4.1, a Java 17 runtime for local Spark, and Databricks
CLI 1.19.0. The repo is bind-mounted into the container, so code changes don't need a
rebuild; only changes to `pyproject.toml` do.

### 2. Log in to Databricks (OAuth)

The CLI is already in the image. Log in once with your workspace URL:

```bash
make auth-login HOST=https://<your-workspace>.cloud.databricks.com
```

1. The command prints a URL. Open it in your browser and sign in.
2. Databricks redirects your browser to `http://localhost:8020`, which Docker forwards to the
   CLI inside the container (see `scripts/auth-login.sh`). The terminal then shows the
   profile details.
3. Check it: `make whoami` prints your Databricks user.

The profile name (`gh-lakehouse`) comes from `DATABRICKS_CONFIG_PROFILE` in `.env`. The
CLI config and OAuth tokens are stored in the Docker volume `gh-archive-lakehouse_databricks-auth`,
not in your home directory, and are never in the repo. To log out completely:
`docker volume rm gh-archive-lakehouse_databricks-auth`.

### 3. Develop

```bash
make help       # list all targets
make lint       # ruff check + format check
make format     # auto-fix
make test       # pytest on a local Spark session
make shell      # shell inside the container
```

### 4. Create the Unity Catalog objects and load data

```bash
make setup-uc                                   # schemas + volumes; safe to rerun
make ingest START=2026-10-06 ARGS=--dry-run     # show what would be uploaded
make ingest START=2026-10-06                    # one day = 24 files, ~330 MB
make ingest START=2026-10-06 END=2026-10-12     # a range of days (inclusive)
```

Ingestion runs locally because outbound internet from a Free Edition workspace is
restricted. Files land in
`/Volumes/<catalog>/gh_raw/landing/gharchive/date=YYYY-MM-DD/YYYY-MM-DD-H.json.gz`.
Rerunning is safe: an hour is skipped when the volume already holds a file of the same
size as on GH Archive, and re-uploaded when the sizes differ (e.g. an interrupted upload).
Hours not yet published (HTTP 404) are reported as `missing`; the command exits non-zero
only if an hour `failed` after retries.

## Configuration

Unity Catalog names (catalog, schemas, volumes) are defined once, as variable defaults in
[`databricks.yml`](databricks.yml). `gh_lakehouse.config.load_settings()` reads the same
defaults locally; override any of them with `GHL_<NAME>` in `.env`
(e.g. `GHL_CATALOG=...`). On Databricks the job passes the values as task parameters.

| Setting | Default |
|---|---|
| catalog | `workspace` |
| raw_schema (volumes) | `gh_raw` |
| bronze_schema | `gh_bronze` |
| silver_schema (incl. quarantine) | `gh_silver` |
| gold_schema | `gh_gold` |
| landing_volume | `landing` |
| checkpoint_volume | `checkpoints` |

## Repository layout

```
src/gh_lakehouse/   transformation and ingestion code (plain functions on DataFrames)
tests/              pytest suite, runs on local Spark + Delta
scripts/            container helper scripts
databricks.yml      Asset Bundle and single source of configuration
Dockerfile, compose.yaml, Makefile
```
