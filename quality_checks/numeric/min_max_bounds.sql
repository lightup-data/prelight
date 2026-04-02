-- Checks that MIN and MAX of a numeric column stay within expected bounds after transformation.
-- Pass: sandbox min >= production min AND sandbox max <= production max * 1.1 (10% headroom).
-- Use when: transformation scales, caps, or modifies numeric values.

SELECT
  prod_min,
  prod_max,
  sbx_min,
  sbx_max,
  CASE
    WHEN sbx_min >= prod_min AND sbx_max <= prod_max * 1.1
    THEN 'PASS' ELSE 'FAIL'
  END AS status
FROM (SELECT MIN(numeric_column) AS prod_min, MAX(numeric_column) AS prod_max FROM schema.source_table) prod,
     (SELECT MIN(numeric_column) AS sbx_min,  MAX(numeric_column) AS sbx_max  FROM schema.sandbox_name)  sbx
