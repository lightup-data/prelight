-- Checks that the percentage breakdown of a categorical column has not shifted significantly.
-- Pass: all categories stay within 5 percentage points of their production distribution.
-- Use when: transformation updates status/type values, deletes rows by category, or backfills categories.

SELECT
  COALESCE(p.val, s.val)                                                  AS category_value,
  COALESCE(p.prod_pct, 0)                                                 AS prod_pct,
  COALESCE(s.sbx_pct, 0)                                                  AS sbx_pct,
  ROUND(ABS(COALESCE(s.sbx_pct, 0) - COALESCE(p.prod_pct, 0)), 2)        AS shift_pct,
  CASE
    WHEN ABS(COALESCE(s.sbx_pct, 0) - COALESCE(p.prod_pct, 0)) <= 5
    THEN 'PASS' ELSE 'FAIL'
  END AS status
FROM
  (SELECT category_column AS val,
          ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS prod_pct
   FROM schema.source_table GROUP BY category_column) p
FULL OUTER JOIN
  (SELECT category_column AS val,
          ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS sbx_pct
   FROM schema.sandbox_name GROUP BY category_column) s
  ON p.val = s.val
ORDER BY shift_pct DESC
