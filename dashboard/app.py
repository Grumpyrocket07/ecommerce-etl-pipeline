"""
Read-only analytics dashboard.

By design this is read-only: it displays results from the last scheduled
pipeline run (whatever's currently in MySQL) rather than exposing a public
"run pipeline now" trigger, so a public link can't be used to hammer a
free-tier database or API quota.

Run locally:
    streamlit run dashboard/app.py
Deploy:
    push this repo to GitHub, then deploy on Streamlit Community Cloud
    pointing at dashboard/app.py (see README for the exact steps + the
    secrets you'll need to set there for the cloud DB connection).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from etl.db import get_engine

st.set_page_config(page_title="E-Commerce ETL & Analytics", layout="wide")

REFRESH_TTL_SECONDS = 300  # cached results refresh at most every 5 minutes


@st.cache_data(ttl=REFRESH_TTL_SECONDS)
def run_query(sql: str) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


# ---------------------------------------------------------------------------
# Header + pipeline health panel
# ---------------------------------------------------------------------------
st.title("E-Commerce ETL Pipeline & Analytics")
st.caption(
    "Historical data: Olist Brazilian E-Commerce dataset (batch). "
    "Live catalog: FakeStoreAPI (scheduled pulls). "
    "This view reflects the last scheduled pipeline run — not a live query trigger."
)

st.subheader("Pipeline health")
health = run_query("""
    SELECT source_name, stage, status, started_at, finished_at,
           TIMESTAMPDIFF(SECOND, started_at, finished_at) AS duration_seconds,
           rows_processed, error_message
    FROM pipeline_runs pr
    WHERE run_id IN (SELECT MAX(run_id) FROM pipeline_runs GROUP BY source_name, stage)
    ORDER BY source_name, stage
""")
if health.empty:
    st.info("No pipeline runs recorded yet.")
else:
    def _status_badge(row):
        icon = {"success": "🟢", "failed": "🔴", "running": "🟡"}.get(row["status"], "⚪")
        return f"{icon} {row['status']}"
    display = health.copy()
    display["status"] = display.apply(_status_badge, axis=1)
    st.dataframe(display, use_container_width=True, hide_index=True)

st.divider()

# ---------------------------------------------------------------------------
# Historical (Olist) analytics
# ---------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Monthly revenue (delivered orders)")
    revenue = run_query("""
        SELECT DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS month,
               ROUND(SUM(oi.price), 2) AS revenue,
               COUNT(DISTINCT o.order_id) AS order_count
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY month ORDER BY month
    """)
    if not revenue.empty:
        fig = px.line(revenue, x="month", y="revenue", markers=True)
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Top 10 categories by revenue")
    categories = run_query("""
        SELECT COALESCE(pc.product_category_name_english, p.product_category_name, 'unknown') AS category,
               ROUND(SUM(oi.price), 2) AS revenue
        FROM order_items oi
        JOIN products p ON p.product_id = oi.product_id
        LEFT JOIN product_categories pc ON pc.product_category_name = p.product_category_name
        GROUP BY category ORDER BY revenue DESC LIMIT 10
    """)
    if not categories.empty:
        fig = px.bar(categories, x="revenue", y="category", orientation="h")
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    st.subheader("Top 10 customers by lifetime spend")
    top_customers = run_query("""
        SELECT c.customer_unique_id, c.customer_state,
               COUNT(DISTINCT o.order_id) AS orders_placed,
               ROUND(SUM(oi.price), 2) AS lifetime_spend
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        JOIN order_items oi ON oi.order_id = o.order_id
        GROUP BY c.customer_unique_id, c.customer_state
        ORDER BY lifetime_spend DESC LIMIT 10
    """)
    st.dataframe(top_customers, use_container_width=True, hide_index=True)

with col4:
    st.subheader("Repeat-purchase rate")
    repeat = run_query("""
        SELECT COUNT(*) AS total_customers,
               SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) AS repeat_customers,
               ROUND(100.0 * SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS repeat_rate_pct
        FROM (
            SELECT c.customer_unique_id, COUNT(DISTINCT o.order_id) AS order_count
            FROM customers c JOIN orders o ON o.customer_id = c.customer_id
            GROUP BY c.customer_unique_id
        ) per_customer
    """)
    if not repeat.empty:
        r = repeat.iloc[0]
        st.metric("Repeat customers", f"{int(r['repeat_customers']):,} / {int(r['total_customers']):,}")
        st.metric("Repeat purchase rate", f"{r['repeat_rate_pct']}%")

st.divider()

st.subheader("Review score vs. delivery timing")
review_delay = run_query("""
    SELECT r.review_score, COUNT(*) AS review_count,
           ROUND(AVG(DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date)), 1) AS avg_days_late
    FROM order_reviews r
    JOIN orders o ON o.order_id = r.order_id
    WHERE o.order_delivered_customer_date IS NOT NULL
    GROUP BY r.review_score ORDER BY r.review_score
""")
if not review_delay.empty:
    fig = px.bar(review_delay, x="review_score", y="avg_days_late",
                 hover_data=["review_count"],
                 labels={"avg_days_late": "Avg days vs. estimate (negative = early)"})
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Live catalog (FakeStoreAPI)
# ---------------------------------------------------------------------------
st.subheader("Live product catalog (most recent fetch)")
live = run_query("""
    SELECT category, COUNT(*) AS product_count,
           ROUND(AVG(price), 2) AS avg_price, ROUND(AVG(rating_rate), 2) AS avg_rating
    FROM live_products
    WHERE fetched_at = (SELECT MAX(fetched_at) FROM live_products)
    GROUP BY category ORDER BY avg_price DESC
""")
if live.empty:
    st.info("No live_products data yet — the FakeStoreAPI pull hasn't run successfully.")
else:
    fig = px.bar(live, x="category", y="avg_price", hover_data=["product_count", "avg_rating"])
    st.plotly_chart(fig, use_container_width=True)
