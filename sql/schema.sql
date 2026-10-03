-- ============================================================================
-- E-Commerce ETL Pipeline & Analytics System — Schema
-- ============================================================================
-- Source data:
--   1) Olist Brazilian E-Commerce dataset (historical, batch-loaded from CSV)
--   2) FakeStoreAPI (live product catalog, pulled on a schedule)
-- Design notes:
--   - Historical (Olist) tables are normalized around orders as the fact table,
--     with customers/sellers/products/payments/reviews as related dimensions.
--   - live_products is a SEPARATE table, not merged into `products`. The two
--     sources describe different real-world catalogs with non-overlapping IDs;
--     forcing a fake foreign-key relationship between them would misrepresent
--     the data. Reporting queries treat them as two distinct, clearly-labeled
--     sources.
--   - pipeline_runs is an operational log table: every ETL run (from any
--     source) writes a row here, giving the pipeline's own reliability story
--     a place to live in the database, not just in text logs.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS ecommerce_etl CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE ecommerce_etl;

-- ---------------------------------------------------------------------------
-- Dimension: customers
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id             CHAR(32) COLLATE utf8mb4_unicode_ci NOT NULL PRIMARY KEY,
    customer_unique_id      CHAR(32)     NOT NULL,
    customer_zip_code_prefix VARCHAR(10) NULL,
    customer_city           VARCHAR(100) NULL,
    customer_state          CHAR(2)      NULL,
    INDEX idx_customers_unique_id (customer_unique_id),
    INDEX idx_customers_state (customer_state)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Dimension: sellers
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sellers (
    seller_id             CHAR(32) COLLATE utf8mb4_unicode_ci NOT NULL PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10) NULL,
    seller_city            VARCHAR(100) NULL,
    seller_state            CHAR(2)     NULL,
    INDEX idx_sellers_state (seller_state)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Dimension: product category translation (small reference table)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product_categories (
    product_category_name         VARCHAR(100) COLLATE utf8mb4_unicode_ci NOT NULL PRIMARY KEY,
    product_category_name_english VARCHAR(100) NULL
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Dimension: products (historical / Olist catalog)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products (
    product_id               CHAR(32) COLLATE utf8mb4_unicode_ci NOT NULL PRIMARY KEY,
    product_category_name    VARCHAR(100) COLLATE utf8mb4_unicode_ci NULL,
    product_name_length      SMALLINT UNSIGNED NULL,
    product_description_length INT UNSIGNED NULL,
    product_photos_qty       SMALLINT UNSIGNED NULL,
    product_weight_g         INT UNSIGNED NULL,
    product_length_cm        SMALLINT UNSIGNED NULL,
    product_height_cm        SMALLINT UNSIGNED NULL,
    product_width_cm         SMALLINT UNSIGNED NULL,
    INDEX idx_products_category (product_category_name),
    CONSTRAINT fk_products_category FOREIGN KEY (product_category_name)
        REFERENCES product_categories (product_category_name)
        ON UPDATE CASCADE ON DELETE SET NULL
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Fact: orders
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    order_id                      CHAR(32) COLLATE utf8mb4_unicode_ci NOT NULL PRIMARY KEY,
    customer_id                   CHAR(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    order_status                  VARCHAR(20) NOT NULL,
    order_purchase_timestamp      DATETIME NOT NULL,
    order_approved_at             DATETIME NULL,
    order_delivered_carrier_date  DATETIME NULL,
    order_delivered_customer_date DATETIME NULL,
    order_estimated_delivery_date DATETIME NULL,
    INDEX idx_orders_customer (customer_id),
    INDEX idx_orders_purchase_ts (order_purchase_timestamp),
    INDEX idx_orders_status (order_status),
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id)
        REFERENCES customers (customer_id)
        ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Fact: order_items (order x product x seller, one row per line item)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_items (
    order_id           CHAR(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    order_item_id       SMALLINT UNSIGNED NOT NULL,
    product_id          CHAR(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    seller_id            CHAR(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    shipping_limit_date DATETIME NULL,
    price                DECIMAL(10,2) NOT NULL,
    freight_value        DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id),
    INDEX idx_order_items_product (product_id),
    INDEX idx_order_items_seller (seller_id),
    CONSTRAINT fk_order_items_order FOREIGN KEY (order_id)
        REFERENCES orders (order_id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_order_items_product FOREIGN KEY (product_id)
        REFERENCES products (product_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT fk_order_items_seller FOREIGN KEY (seller_id)
        REFERENCES sellers (seller_id) ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Fact: order_payments (an order can have multiple payment installments/methods)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_payments (
    order_id             CHAR(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    payment_sequential    SMALLINT UNSIGNED NOT NULL,
    payment_type          VARCHAR(20) NOT NULL,
    payment_installments SMALLINT UNSIGNED NOT NULL,
    payment_value         DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (order_id, payment_sequential),
    INDEX idx_payments_type (payment_type),
    CONSTRAINT fk_payments_order FOREIGN KEY (order_id)
        REFERENCES orders (order_id) ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Fact: order_reviews
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_reviews (
    review_id               CHAR(32) COLLATE utf8mb4_unicode_ci NOT NULL PRIMARY KEY,
    order_id                 CHAR(32) COLLATE utf8mb4_unicode_ci NOT NULL,
    review_score              TINYINT UNSIGNED NOT NULL,
    review_comment_title      VARCHAR(255) NULL,
    review_comment_message    TEXT NULL,
    review_creation_date       DATETIME NULL,
    review_answer_timestamp   DATETIME NULL,
    INDEX idx_reviews_order (order_id),
    INDEX idx_reviews_score (review_score),
    CONSTRAINT fk_reviews_order FOREIGN KEY (order_id)
        REFERENCES orders (order_id) ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Live source: products pulled from FakeStoreAPI on a schedule
-- Kept separate from `products` — different catalog, different ID space.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS live_products (
    source_product_id INT UNSIGNED NOT NULL,
    title               VARCHAR(255) NOT NULL,
    price                DECIMAL(10,2) NOT NULL,
    category              VARCHAR(100) NULL,
    description            TEXT NULL,
    image_url              VARCHAR(500) NULL,
    rating_rate             DECIMAL(3,2) NULL,
    rating_count             INT UNSIGNED NULL,
    fetched_at                DATETIME NOT NULL,
    PRIMARY KEY (source_product_id, fetched_at),
    INDEX idx_live_products_category (category),
    INDEX idx_live_products_fetched (fetched_at)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------
-- Operational: pipeline_runs — one row per ETL stage execution
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    pipeline_name     VARCHAR(100) NOT NULL,
    source_name        VARCHAR(50) NOT NULL,      -- 'olist_csv' or 'fakestore_api'
    stage               VARCHAR(20) NOT NULL,       -- 'extract' | 'transform' | 'load'
    started_at           DATETIME NOT NULL,
    finished_at           DATETIME NULL,
    status                 ENUM('running','success','failed') NOT NULL DEFAULT 'running',
    rows_processed          INT UNSIGNED NULL,
    error_message            TEXT NULL,
    INDEX idx_runs_started (started_at),
    INDEX idx_runs_status (status)
) ENGINE=InnoDB;
