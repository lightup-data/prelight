-- Detects new category values in the sandbox that did not exist in production.
-- Pass: 0 rows returned (no new unexpected values introduced).
-- Use when: transformation writes to a status, type, category, or enum column.

SELECT
  s.category_value                   AS new_value,
  'not present in production'        AS issue
FROM (SELECT DISTINCT category_column AS category_value FROM schema.sandbox_name)  s
LEFT JOIN (SELECT DISTINCT category_column AS category_value FROM schema.source_table) p
  ON s.category_value = p.category_value
WHERE p.category_value IS NULL
