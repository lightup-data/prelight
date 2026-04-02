-- Checks that the average amount increased after the transformation — expects sandbox avg to be higher than production avg.
-- Sandbox: sbx_orders_20260402_1641

SELECT
  prod_avg,
  sbx_avg,
  ROUND(ABS(sbx_avg - prod_avg) / NULLIF(prod_avg, 0) * 100, 2) AS drift_pct,
  CASE
    WHEN sbx_avg > prod_avg
    THEN 'PASS' ELSE 'FAIL'
  END AS status
FROM (SELECT AVG(amount) AS prod_avg FROM analytics.orders) prod,
     (SELECT AVG(amount) AS sbx_avg FROM analytics.sbx_orders_20260402_1641) sbx
