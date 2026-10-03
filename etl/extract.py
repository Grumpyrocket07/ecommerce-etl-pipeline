"""
Extract stage.

extract_olist_csvs() reads each Olist CSV in fixed-size chunks (rather than
pd.read_csv() loading the whole file into memory) and concatenates the
result — this is the "handles scale" decision called out in the project
README: a 10x larger export would still be read in bounded-memory pieces
without changing this code.

extract_fakestore_products() hits a live public REST API with retry +
exponential backoff, since unlike a local file, a network call can fail
transiently and the pipeline needs to survive that.
"""
import time
from pathlib import Path

import pandas as pd
import requests

from etl.config import (
    CSV_DATA_DIR,
    CSV_CHUNK_SIZE,
    FAKESTORE_API_BASE,
    API_MAX_RETRIES,
    API_RETRY_BACKOFF_SECONDS,
    API_TIMEOUT_SECONDS,
)
from etl.logging_setup import get_logger

logger = get_logger(__name__)

OLIST_FILES = {
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
    "products": "olist_products_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
}


def _read_csv_chunked(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Expected Olist CSV at {path} — check CSV_DATA_DIR in your .env"
        )
    chunks = []
    for chunk in pd.read_csv(path, chunksize=CSV_CHUNK_SIZE, low_memory=False):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    logger.info("Read %s rows from %s (%d chunks of <= %d rows)",
                len(df), path.name, len(chunks), CSV_CHUNK_SIZE)
    return df


def extract_olist_csvs(data_dir: Path = CSV_DATA_DIR) -> dict[str, pd.DataFrame]:
    """Returns {logical_name: raw_dataframe} for every Olist source file."""
    raw = {}
    for logical_name, filename in OLIST_FILES.items():
        raw[logical_name] = _read_csv_chunked(data_dir / filename)
    return raw


def extract_fakestore_products() -> list[dict]:
    """
    Pulls the full product catalog from the configured demo e-commerce API.

    Tolerant of two response shapes, since this was switched from
    FakeStoreAPI to DummyJSON mid-project when FakeStoreAPI had an outage
    (Cloudflare 522 — their backend, not a code problem) and they don't
    return JSON identically:
      - a bare list of product objects (FakeStoreAPI's shape), or
      - {"products": [...], "total": ..., ...} (DummyJSON's shape)
    Returns a plain list of product dicts either way, so transform_fakestore
    doesn't need to know which API actually answered.
    """
    # DummyJSON paginates at 30 items by default; limit=0 asks for all of them.
    url = f"{FAKESTORE_API_BASE}/products?limit=0"
    last_exc = None

    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=API_TIMEOUT_SECONDS)
            response.raise_for_status()
            data = response.json()
            products = data["products"] if isinstance(data, dict) and "products" in data else data
            logger.info("Fetched %d products from %s (attempt %d)", len(products), FAKESTORE_API_BASE, attempt)
            return products
        except (requests.RequestException, ValueError, KeyError) as exc:
            last_exc = exc
            wait = API_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "Product API request failed (attempt %d/%d): %s — retrying in %.1fs",
                attempt, API_MAX_RETRIES, exc, wait,
            )
            if attempt < API_MAX_RETRIES:
                time.sleep(wait)

    raise RuntimeError(f"Product API extraction failed after {API_MAX_RETRIES} attempts") from last_exc
