-- Checks that order_id remains unique in the sandbox after the transformation — no duplicate keys introduced.
-- Sandbox: sbx_orders_20260406_1219

SELECT order_id AS duplicate_value, COUNT(*) AS occurrences FROM analytics.sbx_orders_20260406_1219 GROUP BY order_id HAVING COUNT(*) > 1 ORDER BY occurrences DESC
