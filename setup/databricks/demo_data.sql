-- =============================================================================
-- Databricks Vibe MCP — Demo Data Setup
-- =============================================================================
-- Run this entire file in the Databricks SQL Editor before starting the demo.
-- It creates the schema, tables, and sample data needed for the end-to-end demo.
--
-- HOW TO RUN:
--   1. Open Databricks workspace → left sidebar → SQL → SQL Editor
--   2. Paste the entire contents of this file
--   3. Click "Run all" (or run each statement with Shift+Enter)
--   4. Verify with: SHOW TABLES IN analytics;
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Step 1: Create the schema
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS analytics;


-- -----------------------------------------------------------------------------
-- Step 2: Customers table
-- Drop and recreate for a clean demo state.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS analytics.customers;

CREATE TABLE analytics.customers (
  customer_id BIGINT,
  name        STRING,
  email       STRING,
  country     STRING,
  created_at  TIMESTAMP
);

INSERT INTO analytics.customers VALUES
  (1,  'Alice Johnson',    'alice.johnson@example.com',   'US', TIMESTAMP '2023-01-10 09:00:00'),
  (2,  'Bob Martinez',     'bob.martinez@example.com',    'UK', TIMESTAMP '2023-02-14 11:30:00'),
  (3,  'Carol Chen',       'carol.chen@example.com',      'DE', TIMESTAMP '2023-03-05 08:15:00'),
  (4,  'David Kim',        'david.kim@example.com',       'FR', TIMESTAMP '2023-04-20 14:45:00'),
  (5,  'Emma Watson',      'emma.watson@example.com',     'JP', TIMESTAMP '2023-05-08 10:00:00'),
  (6,  'Frank Liu',        'frank.liu@example.com',       'AU', TIMESTAMP '2023-06-17 16:20:00'),
  (7,  'Grace Park',       'grace.park@example.com',      'CA', TIMESTAMP '2023-07-22 09:30:00'),
  (8,  'Hiro Tanaka',      'hiro.tanaka@example.com',     'JP', TIMESTAMP '2023-08-03 13:10:00'),
  (9,  'Isabelle Dubois',  'isabelle.dubois@example.com', 'FR', TIMESTAMP '2023-09-11 07:50:00'),
  (10, 'Jack Smith',       'jack.smith@example.com',      'US', TIMESTAMP '2023-10-25 15:00:00');


-- -----------------------------------------------------------------------------
-- Step 3: Orders table
-- 50 rows — no NULLs, no duplicates (required for quality checks to pass).
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS analytics.orders;

CREATE TABLE analytics.orders (
  order_id    BIGINT,
  customer_id BIGINT,
  amount      DOUBLE,
  status      STRING,
  created_at  TIMESTAMP
);

INSERT INTO analytics.orders VALUES
  (1001,  3,  149.99, 'completed', TIMESTAMP '2024-01-02 08:12:00'),
  (1002,  7,  599.00, 'completed', TIMESTAMP '2024-01-05 10:34:00'),
  (1003,  1,   29.99, 'pending',   TIMESTAMP '2024-01-08 14:22:00'),
  (1004,  9,  249.50, 'completed', TIMESTAMP '2024-01-11 09:05:00'),
  (1005,  4, 1200.00, 'shipped',   TIMESTAMP '2024-01-14 16:48:00'),
  (1006,  2,   89.95, 'completed', TIMESTAMP '2024-01-17 11:30:00'),
  (1007,  6,  399.99, 'completed', TIMESTAMP '2024-01-20 13:15:00'),
  (1008, 10,   55.00, 'cancelled', TIMESTAMP '2024-01-23 08:00:00'),
  (1009,  5,  750.00, 'shipped',   TIMESTAMP '2024-01-26 17:20:00'),
  (1010,  8,  110.25, 'completed', TIMESTAMP '2024-01-29 10:10:00'),
  (1011,  1,  320.00, 'completed', TIMESTAMP '2024-02-02 09:45:00'),
  (1012,  3,   45.00, 'completed', TIMESTAMP '2024-02-05 14:00:00'),
  (1013,  7, 2500.00, 'shipped',   TIMESTAMP '2024-02-08 11:30:00'),
  (1014,  2,  180.00, 'pending',   TIMESTAMP '2024-02-11 15:55:00'),
  (1015,  9,   75.50, 'completed', TIMESTAMP '2024-02-14 08:40:00'),
  (1016,  4,  640.00, 'completed', TIMESTAMP '2024-02-17 12:25:00'),
  (1017,  6,   22.99, 'cancelled', TIMESTAMP '2024-02-20 10:05:00'),
  (1018, 10,  500.00, 'completed', TIMESTAMP '2024-02-23 16:10:00'),
  (1019,  5,  199.99, 'completed', TIMESTAMP '2024-02-26 09:00:00'),
  (1020,  8,  875.00, 'shipped',   TIMESTAMP '2024-02-29 14:35:00'),
  (1021,  2,   33.00, 'completed', TIMESTAMP '2024-03-03 11:20:00'),
  (1022,  1,  460.00, 'completed', TIMESTAMP '2024-03-06 08:50:00'),
  (1023,  7,  125.75, 'pending',   TIMESTAMP '2024-03-09 13:40:00'),
  (1024,  3, 1800.00, 'completed', TIMESTAMP '2024-03-12 16:00:00'),
  (1025,  9,   68.00, 'completed', TIMESTAMP '2024-03-15 10:15:00'),
  (1026,  5,  290.00, 'shipped',   TIMESTAMP '2024-03-18 09:30:00'),
  (1027,  4,   15.99, 'completed', TIMESTAMP '2024-03-21 14:10:00'),
  (1028, 10,  950.00, 'completed', TIMESTAMP '2024-03-24 11:45:00'),
  (1029,  6,  375.00, 'cancelled', TIMESTAMP '2024-03-27 08:20:00'),
  (1030,  8,  215.50, 'completed', TIMESTAMP '2024-03-30 15:30:00'),
  (1031,  1,   92.00, 'completed', TIMESTAMP '2024-04-03 10:00:00'),
  (1032,  3,  530.00, 'shipped',   TIMESTAMP '2024-04-06 13:25:00'),
  (1033,  7,  175.00, 'completed', TIMESTAMP '2024-04-09 08:55:00'),
  (1034,  2,   41.99, 'pending',   TIMESTAMP '2024-04-12 16:40:00'),
  (1035,  9,  700.00, 'completed', TIMESTAMP '2024-04-15 11:05:00'),
  (1036,  4,  310.00, 'completed', TIMESTAMP '2024-04-18 09:15:00'),
  (1037,  6, 1450.00, 'shipped',   TIMESTAMP '2024-04-21 14:50:00'),
  (1038, 10,   88.00, 'completed', TIMESTAMP '2024-04-24 10:35:00'),
  (1039,  5,  225.00, 'completed', TIMESTAMP '2024-04-27 08:00:00'),
  (1040,  8,   17.50, 'cancelled', TIMESTAMP '2024-04-30 15:20:00'),
  (1041,  2,  490.00, 'completed', TIMESTAMP '2024-05-03 11:50:00'),
  (1042,  1,  135.00, 'completed', TIMESTAMP '2024-05-06 09:40:00'),
  (1043,  7,  820.00, 'shipped',   TIMESTAMP '2024-05-09 14:15:00'),
  (1044,  3,   63.75, 'completed', TIMESTAMP '2024-05-12 10:30:00'),
  (1045,  9,  375.00, 'completed', TIMESTAMP '2024-05-15 08:10:00'),
  (1046,  4,  275.00, 'pending',   TIMESTAMP '2024-05-18 13:00:00'),
  (1047,  6,   99.99, 'completed', TIMESTAMP '2024-05-21 16:25:00'),
  (1048, 10, 1100.00, 'completed', TIMESTAMP '2024-05-24 09:50:00'),
  (1049,  5,  555.00, 'shipped',   TIMESTAMP '2024-05-27 11:10:00'),
  (1050,  8,  420.00, 'completed', TIMESTAMP '2024-05-30 15:45:00');


-- -----------------------------------------------------------------------------
-- Step 4: Verify
-- -----------------------------------------------------------------------------
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM analytics.customers
UNION ALL
SELECT 'orders',                  COUNT(*)               FROM analytics.orders;

-- Expected output:
--   customers | 10
--   orders    | 50
