"""
Transform stage.

Takes the raw extracted frames and turns them into exactly what the schema
in sql/schema.sql expects: correct column names, correct types, no
duplicate primary keys, and no rows that would violate a foreign key.
Rows dropped for integrity reasons are counted and logged rather than
silently discarded, since that's a real reliability signal for the pipeline.
"""
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from etl.logging_setup import get_logger

logger = get_logger(__name__)

_TIMESTAMP_COLS_ORDERS = [
    "order_purchase_timestamp", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def _to_datetime(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    # MySQL/pymysql wants None, not NaN/NaT, for NULL-able columns.
    return df.replace({np.nan: None, pd.NaT: None})


def transform_olist(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    clean: dict[str, pd.DataFrame] = {}

    # --- customers -----------------------------------------------------
    customers = raw["customers"].drop_duplicates(subset=["customer_id"]).copy()
    clean["customers"] = _nan_to_none(customers)

    # --- sellers ---------------------------------------------------------
    sellers = raw["sellers"].drop_duplicates(subset=["seller_id"]).copy()
    clean["sellers"] = _nan_to_none(sellers)

    # --- product_categories ------------------------------------------------
    # Start from the translation table, then make sure every category name
    # that actually appears in products is present too (as English=NULL if
    # it wasn't in the translation file) — otherwise the products FK insert
    # would fail on an unmapped category.
    translation = raw["category_translation"].drop_duplicates(subset=["product_category_name"]).copy()
    product_categories_in_products = (
        raw["products"]["product_category_name"].dropna().unique()
    )
    known = set(translation["product_category_name"])
    missing = [c for c in product_categories_in_products if c not in known]
    if missing:
        logger.info("Adding %d product categories with no English translation on file", len(missing))
        translation = pd.concat(
            [translation, pd.DataFrame({
                "product_category_name": missing,
                "product_category_name_english": [None] * len(missing),
            })],
            ignore_index=True,
        )
    clean["product_categories"] = _nan_to_none(translation)

    # --- products -----------------------------------------------------------
    products = raw["products"].drop_duplicates(subset=["product_id"]).copy()
    products = products.rename(columns={
        "product_name_lenght": "product_name_length",
        "product_description_lenght": "product_description_length",
    })
    numeric_cols = [
        "product_name_length", "product_description_length", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
    ]
    for col in numeric_cols:
        products[col] = pd.to_numeric(products[col], errors="coerce").astype("Int64")
    clean["products"] = _nan_to_none(products)

    # --- orders --------------------------------------------------------------
    orders = raw["orders"].drop_duplicates(subset=["order_id"]).copy()
    orders = _to_datetime(orders, _TIMESTAMP_COLS_ORDERS)
    before = len(orders)
    orders = orders.dropna(subset=["order_purchase_timestamp"])  # required, non-nullable
    valid_customers = set(clean["customers"]["customer_id"])
    orders = orders[orders["customer_id"].isin(valid_customers)]
    dropped = before - len(orders)
    if dropped:
        logger.warning("Dropped %d order rows with missing purchase date or unknown customer_id", dropped)
    clean["orders"] = _nan_to_none(orders)

    valid_orders = set(clean["orders"]["order_id"])
    valid_products = set(clean["products"]["product_id"])
    valid_sellers = set(clean["sellers"]["seller_id"])

    # --- order_items -----------------------------------------------------------
    order_items = raw["order_items"].copy()
    order_items = _to_datetime(order_items, ["shipping_limit_date"])
    before = len(order_items)
    order_items = order_items[
        order_items["order_id"].isin(valid_orders)
        & order_items["product_id"].isin(valid_products)
        & order_items["seller_id"].isin(valid_sellers)
    ]
    dropped = before - len(order_items)
    if dropped:
        logger.warning("Dropped %d order_items rows with a dangling FK reference", dropped)
    order_items["price"] = pd.to_numeric(order_items["price"], errors="coerce")
    order_items["freight_value"] = pd.to_numeric(order_items["freight_value"], errors="coerce")
    order_items = order_items.dropna(subset=["price", "freight_value"])
    clean["order_items"] = _nan_to_none(order_items)

    # --- order_payments -------------------------------------------------------
    order_payments = raw["order_payments"].copy()
    before = len(order_payments)
    order_payments = order_payments[order_payments["order_id"].isin(valid_orders)]
    dropped = before - len(order_payments)
    if dropped:
        logger.warning("Dropped %d order_payments rows referencing an unknown order_id", dropped)
    order_payments["payment_value"] = pd.to_numeric(order_payments["payment_value"], errors="coerce")
    order_payments = order_payments.dropna(subset=["payment_value"])
    clean["order_payments"] = _nan_to_none(order_payments)

    # --- order_reviews ----------------------------------------------------------
    order_reviews = raw["order_reviews"].drop_duplicates(subset=["review_id"]).copy()
    order_reviews = _to_datetime(order_reviews, ["review_creation_date", "review_answer_timestamp"])
    before = len(order_reviews)
    order_reviews = order_reviews[order_reviews["order_id"].isin(valid_orders)]
    dropped = before - len(order_reviews)
    if dropped:
        logger.warning("Dropped %d order_reviews rows referencing an unknown order_id", dropped)
    order_reviews["review_score"] = pd.to_numeric(order_reviews["review_score"], errors="coerce").astype("Int64")
    order_reviews = order_reviews.dropna(subset=["review_score"])
    clean["order_reviews"] = _nan_to_none(order_reviews)

    return clean


def transform_fakestore(raw_products: list[dict]) -> pd.DataFrame:
    fetched_at = datetime.now(timezone.utc)
    rows = []
    for p in raw_products:
        rating = p.get("rating") or {}
        rows.append({
            "source_product_id": p.get("id"),
            "title": p.get("title"),
            "price": p.get("price"),
            "category": p.get("category"),
            "description": p.get("description"),
            "image_url": p.get("image"),
            "rating_rate": rating.get("rate"),
            "rating_count": rating.get("count"),
            "fetched_at": fetched_at,
        })
    df = pd.DataFrame(rows)
    before = len(df)
    df = df.dropna(subset=["source_product_id", "title", "price"])
    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %d live_products rows missing required fields", dropped)
    return _nan_to_none(df)
