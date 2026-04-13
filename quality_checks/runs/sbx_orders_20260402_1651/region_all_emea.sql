-- Verifies every row in the sandbox has region set to EMEA — returns violation rows if any are missing or differ.
-- Sandbox: sbx_orders_20260402_1651

SELECT order_id, region FROM analytics.sbx_orders_20260402_1651 WHERE region IS NULL OR region != 'EMEA'
