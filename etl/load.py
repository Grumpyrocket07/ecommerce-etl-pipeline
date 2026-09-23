"""
Load stage.

Load strategy, and why:
  - The Olist tables are a static historical export, so each pipeline run
    does a full refresh: truncate then bulk-insert, in FK-safe order. This
    keeps the load idempotent (rerunning the pipeline never produces
    duplicate-key errors or double-counted rows) without disabling foreign
    key checks to cheat the ordering.
  - live_products is different: every scheduled FakeStoreAPI pull is a new
    timestamped snapshot (fetched_at is part of the primary key), so it's
    append-only — that's what lets the dashboard show price/catalog change
    over time instead of only the latest snapshot.

Bulk inserts use pandas.to_sql with method="multi" and a bounded chunksize,
so a much larger export still loads in bounded-size batches rather than one
giant statement.
"""
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.logging_setup import get_logger

logger = get_logger(__name__)

_LOAD_CHUNKSIZE = 1000

# Child-to-parent order for TRUNCATE (children first, so no FK violation).
_TRUNCATE_ORDER = [
    "order_reviews", "order_payments", "order_items", "orders",
    "products", "product_categories", "sellers", "customers",
]

# Parent-to-child order for INSERT (parents first).
_INSERT_ORDER = list(reversed(_TRUNCATE_ORDER))


def _bulk_insert(engine: Engine, df: pd.DataFrame, table: str) -> int:
    if df.empty:
        logger.info("Skipping load for %s — 0 rows", table)
        return 0
    df.to_sql(table, engine, if_exists="append", index=False,
              method="multi", chunksize=_LOAD_CHUNKSIZE)
    logger.info("Loaded %d rows into %s", len(df), table)
    return len(df)


def load_olist(engine: Engine, clean: dict[str, pd.DataFrame]) -> int:
    total = 0
    with engine.begin() as conn:
        for table in _TRUNCATE_ORDER:
            conn.execute(text(f"TRUNCATE TABLE {table}"))
        logger.info("Truncated %d historical tables ahead of full refresh", len(_TRUNCATE_ORDER))

    for table in _INSERT_ORDER:
        total += _bulk_insert(engine, clean[table], table)
    return total


def load_fakestore(engine: Engine, df: pd.DataFrame) -> int:
    return _bulk_insert(engine, df, "live_products")
