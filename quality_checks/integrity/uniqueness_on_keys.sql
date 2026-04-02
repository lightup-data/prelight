-- Detects duplicate values on a column that must be unique (primary key or unique constraint).
-- Pass: 0 rows returned (no duplicates introduced by the transformation).
-- Use when: transformation inserts rows or updates key/identifier columns.

SELECT
  key_column           AS duplicate_value,
  COUNT(*)              AS occurrences
FROM schema.sandbox_name
GROUP BY key_column
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
