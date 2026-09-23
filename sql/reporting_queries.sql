-- ============================================================================
-- Reporting queries — these back the Streamlit dashboard.
-- Each is deliberately non-trivial (multi-table join, aggregation, or a
-- window function) so they're worth defending in an interview, not just
-- SELECT * dressed up as "analytics".
-- ============================================================================

-- 1. Monthly revenue trend (delivered orders only)
-- Join: orders -> order_items. Aggregation: SUM + GROUP BY month.
SELECT
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS month,
    ROUND(SUM(oi.price), 2)                          AS revenue,
    COUNT(DISTINCT o.order_id)                        AS order_count
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY month
ORDER BY month;

-- 2. Revenue by product category (English name), top 10
-- Join: order_items -> products -> product_categories.
SELECT
    COALESCE(pc.product_category_name_english, p.product_category_name, 'unknown') AS category,
    ROUND(SUM(oi.price), 2)  AS revenue,
    COUNT(*)                  AS items_sold
FROM order_items oi
JOIN products p            ON p.product_id = oi.product_id
LEFT JOIN product_categories pc ON pc.product_category_name = p.product_category_name
GROUP BY category
ORDER BY revenue DESC
LIMIT 10;

-- 3. Top 10 customers by lifetime spend
-- Join: customers -> orders -> order_items. Aggregation across all their orders.
SELECT
    c.customer_unique_id,
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS orders_placed,
    ROUND(SUM(oi.price), 2)     AS lifetime_spend
FROM customers c
JOIN orders o       ON o.customer_id = c.customer_id
JOIN order_items oi ON oi.order_id = o.order_id
GROUP BY c.customer_unique_id, c.customer_state
ORDER BY lifetime_spend DESC
LIMIT 10;

-- 4. Repeat-purchase rate
-- customer_unique_id (not customer_id — Olist mints a new customer_id per
-- order) lets us tell whether the same real person ordered more than once.
SELECT
    COUNT(*)                                                     AS total_customers,
    SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END)             AS repeat_customers,
    ROUND(100.0 * SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS repeat_rate_pct
FROM (
    SELECT c.customer_unique_id, COUNT(DISTINCT o.order_id) AS order_count
    FROM customers c
    JOIN orders o ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id
) per_customer;

-- 5. Average delivery delay vs. estimate, by state — a window-function example
-- Positive days_late means delivered after the estimate.
SELECT
    c.customer_state,
    ROUND(AVG(DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date)), 1) AS avg_days_late,
    RANK() OVER (
        ORDER BY AVG(DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date)) DESC
    ) AS lateness_rank
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY avg_days_late DESC;

-- 6. Review score distribution vs. average delivery delay
-- Shows whether late delivery correlates with lower review scores.
SELECT
    r.review_score,
    COUNT(*) AS review_count,
    ROUND(AVG(DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date)), 1) AS avg_days_late
FROM order_reviews r
JOIN orders o ON o.order_id = r.order_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY r.review_score
ORDER BY r.review_score;

-- 7. Live product catalog snapshot (most recent fetch), by category
-- Pulls from the separate live_products source (FakeStoreAPI), not Olist.
SELECT
    category,
    COUNT(*)              AS product_count,
    ROUND(AVG(price), 2)  AS avg_price,
    ROUND(AVG(rating_rate), 2) AS avg_rating
FROM live_products
WHERE fetched_at = (SELECT MAX(fetched_at) FROM live_products)
GROUP BY category
ORDER BY avg_price DESC;

-- 8. Pipeline health — latest run per (source, stage)
-- This is what the dashboard's "pipeline status" panel reads from.
SELECT
    source_name,
    stage,
    status,
    started_at,
    finished_at,
    TIMESTAMPDIFF(SECOND, started_at, finished_at) AS duration_seconds,
    rows_processed,
    error_message
FROM pipeline_runs pr
WHERE run_id IN (
    SELECT MAX(run_id) FROM pipeline_runs GROUP BY source_name, stage
)
ORDER BY source_name, stage;
