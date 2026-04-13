-- Verifies the sandbox has the same number of rows as production — no rows should be added or dropped by an ALTER+UPDATE.
-- Sandbox: sbx_orders_20260406_1219

SELECT prod.row_count AS prod_row_count, sbx.row_count AS sbx_row_count, sbx.row_count - prod.row_count AS row_diff, CASE WHEN sbx.row_count = prod.row_count THEN 'PASS' ELSE 'FAIL' END AS status FROM (SELECT COUNT(*) AS row_count FROM analytics.orders) prod, (SELECT COUNT(*) AS row_count FROM analytics.sbx_orders_20260406_1219) sbx
