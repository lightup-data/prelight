-- Verifies every row has a non-null value in the new region column.
-- Sandbox: sbx_orders_20260406_1219

SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status FROM analytics.sbx_orders_20260406_1219 WHERE region IS NULL
