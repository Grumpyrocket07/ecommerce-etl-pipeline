# E-Commerce ETL Pipeline & Analytics System

A modular ETL pipeline that ingests historical e-commerce transaction data
(CSV) and a live product catalog (REST API), loads both into a normalized
MySQL schema, and serves the results through a public analytics dashboard.

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
- Tested the FakeStoreAPI retry/backoff logic for real — it correctly
  retried 3 times with exponential backoff and failed cleanly. This
  sandbox's network is allowlisted to a fixed set of domains and
  `fakestoreapi.com` isn't on it, so this proves the *code path* works but
  **you need to confirm the live API call itself succeeds from your own
  machine**, where there's no such restriction.

**What you still need to do yourself** (can't be done from this sandbox):
1. Run `python -m etl.main` on your own machine and confirm the FakeStoreAPI
   branch succeeds (it should — nothing sandbox-specific in that code)
2. Create the free-tier cloud MySQL instance and point `.env` at it
3. Deploy the Prefect schedule and the Streamlit dashboard publicly
4. Actually read through the code — you need to be able to defend every
   part of this in an interview, not just point at working output

---

## Architecture

```
                ┌─────────────────┐        ┌──────────────────────┐
                │  Olist CSVs      │        │  FakeStoreAPI (REST) │
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
              │  Prefect flow    │                 │  Streamlit        │
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
  Olist and FakeStoreAPI describe different real-world catalogs with
  non-overlapping IDs; merging them would fabricate a relationship that
  doesn't exist. `fetched_at` is part of its primary key, so every
  scheduled pull is a new timestamped snapshot (append-only), which is
  what lets you track catalog/price changes over time later if you want to.
- **pipeline_runs** — one row per (source, stage) execution: status,
  timing, row counts, and the error message on failure. This is what the
  dashboard's "pipeline health" panel reads from, and it's real — the row
  in there right now from this session's FakeStoreAPI failure is genuine,
  not staged.

### Load strategy

Olist tables use **full refresh** (truncate, then bulk insert) since it's a
static historical export — this keeps reruns idempotent. `live_products` is
**append-only**, since each pull is a distinct snapshot in time.

One real bug found and fixed during the build: MySQL refuses `TRUNCATE` on
any table referenced by an *active* foreign key, regardless of truncation
order or whether the child table is already empty. Fixed with the standard
pattern — `SET FOREIGN_KEY_CHECKS=0` for just the truncate phase, then
re-enabled immediately after (see `etl/load.py`). Worth knowing this cold if
asked about it.

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
# edit .env if your local MySQL user/password differ from the defaults

# 4. Create the schema
mysql -u root -p < sql/schema.sql
mysql -u root -p -e "CREATE USER IF NOT EXISTS 'etl_user'@'localhost' IDENTIFIED BY 'your_password'; GRANT ALL ON ecommerce_etl.* TO 'etl_user'@'localhost';"

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

### Scheduling: Prefect
- Simplest: Prefect Cloud's free tier + a lightweight always-on worker
  (Railway can run this too — a small worker process on a cron-like
  schedule)
- `prefect deploy etl/flow.py:ecommerce_etl_flow --name daily-refresh` from
  the Prefect docs walks through connecting a deployment to a schedule

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
  flow.py                  # Prefect flow wrapping the same functions
sql/
  schema.sql                # DDL
  reporting_queries.sql       # the 8 validated queries
dashboard/
  app.py                       # Streamlit dashboard
scripts/
  seed_live_products_sample.py  # sandbox-only test fixture — see its docstring
data/                            # put the 9 Olist CSVs here (gitignored)
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
