# E-Commerce ETL Pipeline & Analytics System

A modular ETL pipeline that ingests historical e-commerce transaction data
(CSV) and a live product catalog (REST API), loads both into a normalized
MySQL schema, and serves the results through a public analytics dashboard.
The pipeline runs on AWS RDS, updates automatically on a daily schedule via
GitHub Actions, and is viewable at the link in the repo description.

**Stack:** Python, Pandas, SQLAlchemy, MySQL (AWS RDS), Streamlit, GitHub Actions

---

## Overview

The system has two independent data sources feeding one schema:

- **Historical data** — the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce),
  ~100K real anonymized orders across multiple marketplaces, loaded in bulk
  from CSV.
- **Live data** — a product catalog pulled from a public REST API
  (DummyJSON) on a daily schedule.

Both land in a normalized MySQL database, which a Streamlit dashboard reads
from to display revenue trends, customer behavior, delivery performance,
and live catalog data.

## Architecture

```
                ┌─────────────────┐        ┌──────────────────────┐
                │  Olist CSVs      │        │  DummyJSON (REST)     │
                │  (historical,    │        │  (live product       │
                │   batch)         │        │   catalog)            │
                └────────┬─────────┘        └──────────┬───────────┘
                         │ chunked read (pandas)          │ GET + retry/backoff
                         ▼                                 ▼
                ┌──────────────────────────────────────────────────┐
                │   extract.py → transform.py → load.py             │
                │   (every stage logged to pipeline_runs + files)    │
                └──────────────────────┬─────────────────────────────┘
                                        ▼
                              ┌──────────────────┐
                              │   MySQL (AWS RDS)   │
                              │  (normalized       │
                              │   schema)           │
                              └─────────┬───────────┘
                                        │
                       ┌────────────────┴─────────────────┐
                       ▼                                    ▼
              ┌─────────────────┐                 ┌──────────────────┐
              │  GitHub Actions  │                 │  Streamlit        │
              │  (daily schedule,│                 │  dashboard         │
              │   live data only)│                 │  (public, read-only)│
              └──────────────────┘                 └──────────────────┘
```

### Schema (`sql/schema.sql`)

- **customers, sellers, products, product_categories** — dimensions
- **orders, order_items, order_payments, order_reviews** — facts, all
  foreign-keyed back to their dimensions, indexed on the columns the
  reporting queries filter and join on
- **live_products** — kept as a separate table from `products` rather than
  merged with it. Olist and the live API describe two different real-world
  catalogs with non-overlapping IDs, so merging them would imply a
  relationship that doesn't exist. `fetched_at` is part of its primary key,
  so every scheduled pull is a new timestamped snapshot rather than an
  overwrite, which allows tracking catalog or price changes over time.
- **pipeline_runs** — an operational log table, one row per
  (source, stage) execution, recording status, timing, row counts, and the
  error message on failure. This is what the dashboard's pipeline-health
  panel reads from.

### Load strategy

The historical tables use a **full refresh** (truncate, then bulk insert)
since the source is a static export — this keeps reruns idempotent.
`live_products` is **append-only**, since each scheduled pull is a distinct
snapshot in time rather than a replacement of the previous one.

### Reliability

- Every stage (`extract`/`transform`/`load`) for both sources is wrapped in
  a `track_run()` context manager (`etl/db.py`) that writes a `running` row
  to `pipeline_runs` on start, then updates it to `success` with a row
  count, or `failed` with the error message, even on an unhandled
  exception.
- The two source branches (historical, live) are isolated at the top level
  — a failure in one does not prevent the other from running.
- The API extractor retries transient failures with exponential backoff
  before giving up.
- Structured logs (console + rotating file) via `etl/logging_setup.py`,
  independent of what's recorded in the database.

## Notable issues resolved during development

1. **MySQL refuses `TRUNCATE` on any table referenced by an active foreign
   key**, regardless of truncation order or whether the referencing table
   is already empty. Resolved with the standard pattern —
   `SET FOREIGN_KEY_CHECKS=0` for just the truncate phase, re-enabled
   immediately after (`etl/load.py`).

2. **A foreign key's two columns must match in collation, not just type.**
   MySQL's default collation can differ between installs or versions
   (`utf8mb4_0900_ai_ci` vs. `utf8mb4_unicode_ci`), so two identically
   declared `CHAR(32)` columns can still fail the constraint (`ERROR 3780`)
   if their inherited collation differs. Resolved by pinning
   `COLLATE utf8mb4_unicode_ci` explicitly on every primary/foreign key
   column, making the schema behave identically regardless of server
   defaults (`sql/schema.sql`).

3. **The live data source (originally FakeStoreAPI) had a backend outage**
   mid-project. The source was switched to DummyJSON, a different demo API
   with a different response shape (`{"products": [...]}` instead of a
   bare list, a flat `rating` number instead of `{"rate", "count"}`,
   `thumbnail` instead of `image`). `extract.py` and `transform.py` were
   written to tolerate either shape, so the source can be swapped back via
   a single `.env` value with no code change.

## Local setup

```bash
# 1. Get the data
#    Download from Kaggle: kaggle.com/datasets/olistbr/brazilian-ecommerce
#    Unzip the 9 CSVs into ./data/

# 2. Install
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
# Edit .env with your MySQL credentials and confirm the port MySQL is
# actually running on:  netstat -ano | findstr LISTENING | findstr 33

# 4. Create the schema
mysql -u root -p < sql/schema.sql
mysql -u root -p -e "CREATE USER IF NOT EXISTS 'etl_user'@'localhost' IDENTIFIED BY 'your_password_here'; GRANT ALL ON ecommerce_etl.* TO 'etl_user'@'localhost'; FLUSH PRIVILEGES;"
# Use the same password value in .env's DB_PASSWORD.

# 5. Run the pipeline
python -m etl.main

# 6. Run the dashboard
streamlit run dashboard/app.py
```

## Cloud deployment

### Database: AWS RDS (MySQL, free tier)

1. RDS console → Create database → MySQL → free-tier-eligible instance
   class (e.g. `db.t4g.micro`) → public access enabled → create a security
   group allowing inbound MySQL (port 3306) traffic
2. Update `.env` with the RDS endpoint, port, username, and password —
   nothing else in the code changes, since `etl/config.py` builds the
   connection string from environment variables
3. Load the schema against the cloud instance, then run the pipeline once
   to populate it:
   ```
   mysql -h <endpoint> -P 3306 -u <user> -p ecommerce_etl < sql/schema.sql
   python -m etl.main
   ```

### Scheduling: GitHub Actions

`.github/workflows/scheduled-pipeline.yml` runs
`scripts/run_scheduled_update.py` daily, using GitHub's own runners with no
separate worker or always-on service required.

This intentionally only re-runs the **live product API branch**, not the
full pipeline. The historical branch is a static export — rerunning it
produces identical output every time, and it depends on raw CSVs that are
not committed to the repository (see `.gitignore`). The live branch is the
only part of the system where a schedule has a genuine effect.

Setup:
1. Repo **Settings → Secrets and variables → Actions → New repository
   secret** — add `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`,
   `DB_PASSWORD`
2. Push the workflow file — GitHub picks it up automatically
3. The **Actions** tab shows run history, and includes a manual
   **"Run workflow"** trigger

### Dashboard: Streamlit Community Cloud

1. Push the repository to GitHub (`.gitignore` already excludes `.env`
   and the CSVs)
2. share.streamlit.io → "New app" → point at `dashboard/app.py`
3. In the app's "Secrets" settings, add the same five database values —
   Streamlit Cloud exposes these as environment variables automatically
4. This produces a public `*.streamlit.app` link

## Reporting queries (`sql/reporting_queries.sql`)

Eight queries back the dashboard, covering joins, aggregations, and a
window function:

1. Monthly revenue trend
2. Revenue by product category (3-table join)
3. Top 10 customers by lifetime spend
4. Repeat-purchase rate
5. Average delivery delay by state, ranked (`RANK() OVER`)
6. Review score vs. delivery delay
7. Live catalog snapshot by category
8. Pipeline health (latest run per source/stage)

A notable finding from query 6: review score and delivery delay correlate
in this dataset — 1-star reviews average only 4 days early relative to the
estimated delivery date, while 4-star reviews average 12.4 days early,
suggesting that less-early (or late) delivery tracks with lower review
scores.

## Project structure

```
etl/
  config.py          # env-driven settings
  logging_setup.py   # shared structured logging
  db.py               # engine + pipeline_runs tracking
  extract.py           # chunked CSV reads + retrying API client
  transform.py          # cleaning, type-casting, FK-integrity filtering
  load.py                # truncate+bulk-insert (historical) / append (live)
  main.py                 # orchestrator
sql/
  schema.sql                # DDL
  reporting_queries.sql       # the 8 reporting queries
dashboard/
  app.py                       # Streamlit dashboard
scripts/
  seed_live_products_sample.py  # offline test fixture — see its docstring
  run_scheduled_update.py        # entry point for the GitHub Actions schedule
.github/workflows/
  scheduled-pipeline.yml          # daily live-catalog update
data/                              # place the 9 Olist CSVs here (gitignored)
.env.example
requirements.txt
```

## Known limitations

- `olist_geolocation_dataset.csv` (the 10th file in the Kaggle download)
  is not loaded — it is zip-code-level latitude/longitude data with
  roughly 1M rows and no clean foreign key into the rest of the schema;
  out of scope for this version.
- The live and historical catalogs are intentionally not merged (see
  schema notes above).
- The dashboard is read-only by design, reflecting the last scheduled run
  rather than exposing a public trigger — this avoids a public link being
  used to repeatedly hit a free-tier database or API quota.
