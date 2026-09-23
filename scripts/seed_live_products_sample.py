"""
SANDBOX TEST FIXTURE ONLY.

This sandbox's network egress is restricted and can't reach fakestoreapi.com
directly (confirmed — see etl/extract.py's retry/backoff logic handling the
403 cleanly). extract_fakestore_products() in etl/extract.py is the real
code path and will work as-is once you run this on your own machine or in
the cloud with normal internet access.

This script inserts a small realistic sample, shaped exactly like what
transform_fakestore() would produce from a real API response, purely so the
dashboard has live_products data to render against during development.
Delete these rows (or just let a real pipeline run truncate/append over
them) once you're running against the live API for real.
"""
from datetime import datetime, timezone
import pandas as pd

from etl.db import get_engine
from etl.load import load_fakestore

# Shaped identically to FakeStoreAPI's real /products response schema.
SAMPLE_PRODUCTS = [
    {"id": 1, "title": "Fjallraven Foldsack No. 1 Backpack", "price": 109.95,
     "category": "men's clothing", "description": "Fits 15-laptops.",
     "image": "https://fakestoreapi.com/img/81fPKd-2AYL._AC_SL1500_.jpg",
     "rating": {"rate": 3.9, "count": 120}},
    {"id": 2, "title": "Mens Casual Premium Slim Fit T-Shirts", "price": 22.3,
     "category": "men's clothing", "description": "Slim-fitting style.",
     "image": "https://fakestoreapi.com/img/71-3HjGNDUL._AC_SY879._SX._UX._SY._UY_.jpg",
     "rating": {"rate": 4.1, "count": 259}},
    {"id": 3, "title": "Mens Cotton Jacket", "price": 55.99,
     "category": "men's clothing", "description": "Great outerwear jacket.",
     "image": "https://fakestoreapi.com/img/71li-ujtlUL._AC_UX679_.jpg",
     "rating": {"rate": 4.7, "count": 500}},
    {"id": 4, "title": "Womens 3-in-1 Snowboard Jacket", "price": 56.99,
     "category": "women's clothing", "description": "Detachable hood.",
     "image": "https://fakestoreapi.com/img/81XH0e8fefL._AC_UY879_.jpg",
     "rating": {"rate": 2.6, "count": 235}},
    {"id": 5, "title": "John Hardy Legends Naga Bracelet", "price": 695.0,
     "category": "jewelery", "description": "From the artisans of Bali.",
     "image": "https://fakestoreapi.com/img/71pWzhdJNwL._AC_UL640_QL65_ML3_.jpg",
     "rating": {"rate": 4.6, "count": 400}},
    {"id": 6, "title": "Solid Gold Petite Micropave Ring", "price": 168.0,
     "category": "jewelery", "description": "Satisfaction Guaranteed.",
     "image": "https://fakestoreapi.com/img/71YAIFU48IL._AC_UL640_QL65_ML3_.jpg",
     "rating": {"rate": 3.9, "count": 70}},
    {"id": 7, "title": "WD 2TB Elements Portable External Hard Drive",
     "price": 64.0, "category": "electronics", "description": "USB 3.0.",
     "image": "https://fakestoreapi.com/img/61IBBVJvSDL._AC_SY879_.jpg",
     "rating": {"rate": 3.3, "count": 203}},
    {"id": 8, "title": "SanDisk SSD PLUS 1TB Internal SSD", "price": 109.0,
     "category": "electronics", "description": "Up to 535MB/s.",
     "image": "https://fakestoreapi.com/img/61U7T1koQqL._AC_SX679_.jpg",
     "rating": {"rate": 2.9, "count": 470}},
    {"id": 9, "title": "Samsung 49-Inch CHG90 Curved Gaming Monitor",
     "price": 999.99, "category": "electronics", "description": "QLED technology.",
     "image": "https://fakestoreapi.com/img/81Zt42ioCgL._AC_SX679_.jpg",
     "rating": {"rate": 2.2, "count": 140}},
    {"id": 10, "title": "BIYLACLESEN Women's 3-in-1 Jacket", "price": 56.99,
     "category": "women's clothing", "description": "Waterproof shell.",
     "image": "https://fakestoreapi.com/img/51eg55uWmdL._AC_UX679_.jpg",
     "rating": {"rate": 2.6, "count": 235}},
]


def seed():
    rows = []
    fetched_at = datetime.now(timezone.utc)
    for p in SAMPLE_PRODUCTS:
        rating = p["rating"]
        rows.append({
            "source_product_id": p["id"], "title": p["title"], "price": p["price"],
            "category": p["category"], "description": p["description"],
            "image_url": p["image"], "rating_rate": rating["rate"],
            "rating_count": rating["count"], "fetched_at": fetched_at,
        })
    df = pd.DataFrame(rows)
    engine = get_engine()
    loaded = load_fakestore(engine, df)
    print(f"Seeded {loaded} sample live_products rows (fixture data, see module docstring)")


if __name__ == "__main__":
    seed()
