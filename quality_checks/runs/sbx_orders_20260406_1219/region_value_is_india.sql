-- Verifies all rows have region = 'INDIA' and no other values were introduced.
-- Sandbox: sbx_orders_20260406_1219

SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status FROM analytics.sbx_orders_20260406_1219 WHERE region <> 'INDIA'
