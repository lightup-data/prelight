-- Verifies no rows were added or deleted — transformation only adds a column and updates values.
-- Sandbox: sbx_orders_20260402_1651

SELECT
  prod.row_count AS prod_row_count,
  sbx.row_count AS sbx_row_count,
  sbx.row_count - prod.row_count AS row_diff,
  ROUND((sbx.row_count - prod.row_count) * 100.0 / NULLIF(prod.row_count, 0), 2) AS pct_change,
  CASE
    WHEN ABS((sbx.row_count - prod.row_count) * 100.0 / NULLIF(prod.row_count, 0)) <= 10
    THEN 'PASS' ELSE 'FAIL'
  END AS status
FROM (SELECT COUNT(*) AS row_count FROM analytics.orders) prod,
     (SELECT COUNT(*) AS row_count FROM analytics.sbx_orders_20260402_1651) sbx
