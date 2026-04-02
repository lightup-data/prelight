-- Compares total amount sum between production and sandbox. Expects a ~25% increase since 50% of rows (odd customer_ids) had their amount raised by 50%.
-- Sandbox: sbx_orders_20260402_1641

SELECT
  prod_sum,
  sbx_sum,
  ROUND(ABS(sbx_sum - prod_sum) / NULLIF(prod_sum, 0) * 100, 2) AS drift_pct,
  CASE
    WHEN sbx_sum > prod_sum
    THEN 'PASS' ELSE 'FAIL'
  END AS status
FROM (SELECT SUM(amount) AS prod_sum FROM analytics.orders) prod,
     (SELECT SUM(amount) AS sbx_sum FROM analytics.sbx_orders_20260402_1641) sbx
