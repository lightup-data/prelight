-- Checks that order_id remains unique after the transformation. Returns 0 rows = PASS.
-- Sandbox: sbx_orders_20260403_0005

SELECT order_id AS duplicate_value, COUNT(*) AS occurrences FROM analytics.sbx_orders_20260403_0005 GROUP BY order_id HAVING COUNT(*) > 1 ORDER BY occurrences DESC
