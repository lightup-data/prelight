-- Checks that the mean (AVG) of a numeric column has not shifted unexpectedly.
-- Pass: average drift is within 10% relative to the production average.
-- Use when: transformation updates numeric values (SET col = col * factor, rounding, etc.).

SELECT
  prod_avg,
  sbx_avg,
  ROUND(ABS(sbx_avg - prod_avg) / NULLIF(prod_avg, 0) * 100, 2) AS drift_pct,
  CASE
    WHEN ABS(sbx_avg - prod_avg) / NULLIF(prod_avg, 0) * 100 <= 10
    THEN 'PASS' ELSE 'FAIL'
  END AS status
FROM (SELECT AVG(numeric_column) AS prod_avg FROM schema.source_table) prod,
     (SELECT AVG(numeric_column) AS sbx_avg  FROM schema.sandbox_name)  sbx
