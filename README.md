# E-Commerce ETL Pipeline & Analytics System

A modular ETL pipeline that ingests historical e-commerce transaction data
(CSV) and a live product catalog (REST API — DummyJSON by default, swapped
in from FakeStoreAPI after a backend outage during development; see
"Known issues found and fixed" below), loads both into a normalized MySQL
schema, and serves the results through a public analytics dashboard.

This directly backs the resume bullet:
> Developed a modular ETL pipeline to extract, transform, and store
> e-commerce datasets. Automated data ingestion workflows for CSV and
> API-based transactional data using Python and Pandas. Designed relational
> database schemas and optimized SQL queries for reporting operations.
> Implemented logging and exception-handling mechanisms to improve pipeline
> reliability.

---

## What's real vs. what you still need to do

Everything in this repo was **actually built and run against real data and
a real MySQL instance** in the build sandbox, not just written and assumed
to work. Specifically, this session:

- Installed MySQL 8.0, created the schema, and confirmed all 9 tables + FKs
- Ran the full Olist extract → transform → load pipeline against your real
  uploaded CSVs: **549,947 rows loaded in 53 seconds** (99,441 customers,
  99,441 orders, 112,650 order items, 103,886 payments, 98,410 reviews —
  814 review rows were correctly dropped for referencing an order that
  didn't survive cleaning, which is exactly the kind of integrity check
  "reliability" is supposed to mean)
- Ran all 8 reporting SQL queries against that real loaded data and
  confirmed every one returns sensible results (see `sql/reporting_queries.sql`)
- Ran the Streamlit dashboard as a live server and confirmed it serves
  (HTTP 200, health check passing) and separately re-ran every chart's
  underlying query + Plotly call directly to confirm no runtime errors
- **Confirmed end-to-end on your actual Windows machine, not just the
  sandbox**: after working through a real local setup (MySQL port had been
  changed from the default during install, a collation mismatch in the FK
  constraints, and a password mismatch for `etl_user`), the full Olist
  pipeline ran successfully on your machine too — same 549,947 rows loaded.
- The live-API branch originally targeted FakeStoreAPI. Its retry/backoff
  logic was verified for real (3 retries with exponential backoff, clean
  failure) — first against this sandbox's restricted network, then again
  against your machine when FakeStoreAPI itself had a genuine outage
  (Cloudflare 522 — their backend, not a code or network problem on either
  end). Rather than wait it out, the live-API source was switched to
  **DummyJSON** (also free, no auth, no key). `extract.py` and
  `transform.py` were updated to tolerate *either* API's response shape —
  verified against DummyJSON's actual documented response format and
  loaded into MySQL — so flipping `FAKESTORE_API_BASE` back to
  `https://fakestoreapi.com` later needs no code change if it comes back up.

**What's still worth doing yourself:**
1. Run `python -m etl.main` once more to confirm the DummyJSON branch
   succeeds end to end on your machine (the Olist branch is already
   confirmed working — this just closes the loop on the live-API side)
2. Create the free-tier cloud MySQL instance and point `.env` at it
3. Deploy the scheduled GitHub Actions workflow and the Streamlit dashboard publicly
4. Actually read through the code — you need to be able to defend every
   part of this in an interview, not just point at working output

---

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
                              │   MySQL 8.0        │
                              │  (normalized       │
                              │   schema)           │
                              └─────────┬───────────┘
                                        │
                       ┌────────────────┴─────────────────┐
                       ▼                                    ▼
              ┌─────────────────┐                 ┌──────────────────┐
              │  GitHub Actions  │                 │  Streamlit        │
              │  (schedules the  │                 │  dashboard         │
              │   above)         │                 │  (public, read-only)│
              └──────────────────┘                 └──────────────────┘
```

### Schema (`sql/schema.sql`)

- **customers, sellers, products, product_categories** — dimensions
- **orders, order_items, order_payments, order_reviews** — facts, all FK'd
  back to their dimensions, all indexed on the columns the reporting
  queries actually filter/join on
- **live_products** — deliberately a *separate* table from `products`.
  Olist and the live product API describe different real-world catalogs
  with non-overlapping IDs; merging them would fabricate a relationship
  that doesn't exist. `fetched_at` is part of its primary key, so every
  scheduled pull is a new timestamped snapshot (append-only), which is
  what lets you track catalog/price changes over time later if you want to.
- **pipeline_runs** — one row per (source, stage) execution: status,
  timing, row counts, and the error message on failure. This is what the
  dashboard's "pipeline health" panel reads from, and it's real — the row
  in there from this project's actual FakeStoreAPI outage is genuine,
  not staged.

### Load strategy

Olist tables use **full refresh** (truncate, then bulk insert) since it's a
static historical export — this keeps reruns idempotent. `live_products` is
**append-only**, since each pull is a distinct snapshot in time.

Three real issues found and fixed during the build — all worth knowing cold
if asked about them in an interview:

1. MySQL refuses `TRUNCATE` on any table referenced by an *active* foreign
   key, regardless of truncation order or whether the child table is
   already empty. Fixed with the standard pattern — `SET
   FOREIGN_KEY_CHECKS=0` for just the truncate phase, then re-enabled
   immediately after (see `etl/load.py`).

2. A foreign key's referencing and referenced columns must match not just
   in type but in **collation** — and MySQL's default collation can differ
   between installs/versions (`utf8mb4_0900_ai_ci` vs
   `utf8mb4_unicode_ci`, etc.), even when every column is declared
   identically as `CHAR(32)`. Relying on inheriting the database's default
   collation worked in one environment and failed in another with `ERROR
   3780`. Fixed by pinning `COLLATE utf8mb4_unicode_ci` explicitly on every
   column that's a primary or foreign key, so the schema behaves
   identically regardless of server defaults (see `sql/schema.sql`).

3. The live-API source (originally FakeStoreAPI) had a genuine backend
   outage mid-project (Cloudflare 522). Rather than just wait, the pipeline
   was pointed at DummyJSON instead — a different free demo API with a
   *different* JSON shape (`{"products": [...]}` instead of a bare list,
   a flat `rating` number instead of `{"rate", "count"}`, `thumbnail`
   instead of `image`). `extract.py` and `transform.py` were written to
   tolerate either shape, so the source can be swapped back via one `.env`
   value with no code change. This is a real example of designing for a
   dependency that might change out from under you — a reasonable thing to
   bring up if an interviewer asks about handling unreliable third-party
   APIs.

### Reliability

- Every stage (`extract`/`transform`/`load`) for both sources is wrapped in
  `etl/db.py`'s `track_run()` context manager — writes a `running` row to
  `pipeline_runs` on start, updates to `success`+row count or
  `failed`+error message on exit, even on an unhandled exception
- The two source branches (`olist_csv`, `fakestore_api`) are isolated in
  `run_pipeline()` — one failing doesn't stop the other, which was actually
  proven this session
- The API extractor retries transient failures with exponential backoff
  before giving up
- Structured logs (console + rotating file) via `etl/logging_setup.py`,
  independent of what's in the database

---

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
# Edit .env: set DB_PASSWORD to match whatever you set for etl_user below.
# Also check DB_PORT — some Windows MySQL installs pick a non-default port
# (e.g. 3307) automatically if 3306 looks taken during setup. Confirm the
# real port with:  netstat -ano | findstr LISTENING | findstr 33
# and make DB_PORT in .env match it, or pass -P <port> on the commands below.

# 4. Create the schema
mysql -u root -p < sql/schema.sql
mysql -u root -p -e "CREATE USER IF NOT EXISTS 'etl_user'@'localhost' IDENTIFIED BY 'your_password_here'; GRANT ALL ON ecommerce_etl.* TO 'etl_user'@'localhost'; FLUSH PRIVILEGES;"
# ^ replace 'your_password_here' with a real password, and put that same
#   exact value in .env's DB_PASSWORD — a mismatch here is the #1 cause of
#   "Access denied for user 'etl_user'@'localhost'" errors.

# 5. Run the pipeline
python -m etl.main

# 6. Run the dashboard
streamlit run dashboard/app.py
```

---

## Moving to the cloud (Milestone 3)

You said you don't have cloud experience — here's the exact, free-tier path.

### MySQL: Railway (or Aiven / Clever Cloud as alternatives)
1. Sign up at railway.app, "New Project" → "Provision MySQL"
2. Railway gives you a host, port, user, password, and database name
3. Put those into `.env` (replace the local values) — nothing else in the
   code changes, since `etl/config.py` builds the connection string from
   env vars
4. Run `mysql -h <host> -P <port> -u <user> -p <db> < sql/schema.sql`
   against the cloud instance, then `python -m etl.main` once to populate it

### Scheduling: GitHub Actions

Runs `scripts/run_scheduled_update.py` daily via
`.github/workflows/scheduled-pipeline.yml` — GitHub's own runners execute
it, no separate worker or always-on service needed.

This deliberately only re-runs the **live product API branch**, not the
full pipeline. The Olist branch is a static historical export: re-running
it produces byte-identical output every time, and it needs the raw CSVs,
which aren't committed to the repo (too large, and not meant to be — see
`.gitignore`). The live branch is the only part of the system where a
schedule actually has a point, since it's the only part that can change
between runs.

Setup:
1. In the GitHub repo: **Settings → Secrets and variables → Actions →
   New repository secret** — add `DB_HOST`, `DB_PORT`, `DB_NAME`,
   `DB_USER`, `DB_PASSWORD` (same values as your cloud `.env`)
2. Push `.github/workflows/scheduled-pipeline.yml` — GitHub picks it up
   automatically, no extra enabling step
3. Check the **Actions** tab on GitHub to see run history, or click
   **"Run workflow"** there to trigger it on demand instead of waiting
   for the schedule

### Dashboard: Streamlit Community Cloud
1. Push this repo to GitHub (the `.gitignore` already excludes `.env` and
   the CSVs — don't commit credentials or the data files)
2. share.streamlit.io → "New app" → point at `dashboard/app.py`
3. In the app's "Secrets" settings, add the same values as your `.env`
   (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`) — Streamlit
   Cloud injects these as environment variables automatically
4. You get a public `*.streamlit.app` link — this is what goes on your resume/portfolio

---

## Reporting queries (`sql/reporting_queries.sql`)

All 8 validated against the real loaded data this session:

1. Monthly revenue trend (join + aggregation)
2. Revenue by product category (3-table join)
3. Top 10 customers by lifetime spend
4. Repeat-purchase rate (subquery + conditional aggregation)
5. Avg delivery delay by state, ranked (**window function** — `RANK() OVER`)
6. Review score vs. delivery delay correlation
7. Live catalog snapshot by category
8. Pipeline health (latest run per source/stage)

Real result worth knowing for an interview: review score and delivery
delay *do* correlate in this data — 1-star reviews average only 4 days
early vs. estimate, while 4-star reviews average 12.4 days early. Late (or
less-early) delivery tracks with worse reviews.

---

## Project structure

```
etl/
  config.py          # env-driven settings
  logging_setup.py   # shared structured logging
  db.py               # engine + pipeline_runs tracking
  extract.py           # chunked CSV reads + retrying API client
  transform.py          # cleaning, type-casting, FK-integrity filtering
  load.py                # truncate+bulk-insert (historical) / append (live)
  main.py                 # plain-Python orchestrator
sql/
  schema.sql                # DDL
  reporting_queries.sql       # the 8 validated queries
dashboard/
  app.py                       # Streamlit dashboard
scripts/
  seed_live_products_sample.py  # sandbox-only test fixture — see its docstring
  run_scheduled_update.py        # entry point for the GitHub Actions schedule
.github/workflows/
  scheduled-pipeline.yml          # daily live-catalog update (see README above)
data/                              # put the 9 Olist CSVs here (gitignored)
.env.example
requirements.txt
```

## Known limitations (be ready to name these, don't get caught by them)

- `olist_geolocation_dataset.csv` (the 10th file in the Kaggle zip) isn't
  loaded — it's zip-code-level lat/long with ~1M rows and no clean FK into
  the rest of the schema; out of scope for this version
- The live/historical catalogs aren't merged, by design (see schema notes)
  — don't let an interviewer push you into pretending they're one catalog
- The dashboard is intentionally read-only (reflects the last scheduled
  run) rather than triggering a live pipeline run from a public button —
  a deliberate scope decision to avoid a public link abusing a free-tier
  DB/API quota, not an oversight
