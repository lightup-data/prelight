-- Compares the SUM of a numeric column between production and sandbox.
-- Pass: total sum drift is within 10% (adjust threshold based on transformation scope).
-- Use when: transformation touches financial, metric, or aggregated numeric columns.

SELECT
  prod_sum,
  sbx_sum,
  ROUND(ABS(sbx_sum - prod_sum) / NULLIF(prod_sum, 0) * 100, 2) AS drift_pct,
  CASE
    WHEN ABS(sbx_sum - prod_sum) / NULLIF(prod_sum, 0) * 100 <= 10
    THEN 'PASS' ELSE 'FAIL'
  END AS status
FROM (SELECT SUM(numeric_column) AS prod_sum FROM schema.source_table) prod,
     (SELECT SUM(numeric_column) AS sbx_sum  FROM schema.sandbox_name)  sbx
