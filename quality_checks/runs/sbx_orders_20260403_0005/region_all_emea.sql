-- Confirms every row has region = 'EMEA' with no nulls or unexpected values. Returns 0 rows = PASS.
-- Sandbox: sbx_orders_20260403_0005

SELECT order_id, region FROM analytics.sbx_orders_20260403_0005 WHERE region IS DISTINCT FROM 'EMEA'
