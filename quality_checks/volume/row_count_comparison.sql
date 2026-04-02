-- Compares total row count between production and sandbox.
-- Pass: row count drift is within 10% (adjust threshold to match transformation intent).
-- Use when: any transformation that may add, delete, or filter rows.

SELECT
  prod.row_count                                                                    AS prod_row_count,
  sbx.row_count                                                                     AS sbx_row_count,
  sbx.row_count - prod.row_count                                                    AS row_diff,
  ROUND((sbx.row_count - prod.row_count) * 100.0 / NULLIF(prod.row_count, 0), 2)   AS pct_change,
  CASE
    WHEN ABS((sbx.row_count - prod.row_count) * 100.0 / NULLIF(prod.row_count, 0)) <= 10
    THEN 'PASS' ELSE 'FAIL'
  END AS status
FROM (SELECT COUNT(*) AS row_count FROM schema.source_table) prod,
     (SELECT COUNT(*) AS row_count FROM schema.sandbox_name)  sbx
