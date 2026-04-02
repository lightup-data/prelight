-- Detects rows in the sandbox where a foreign key value does not exist in the referenced table.
-- Pass: 0 rows returned (no orphaned foreign key values after transformation).
-- Use when: transformation inserts rows, changes FK column values, or joins with another table.

SELECT
  s.fk_column          AS orphaned_fk_value,
  COUNT(*)              AS orphaned_count
FROM schema.sandbox_name s
LEFT JOIN schema.referenced_table r ON s.fk_column = r.referenced_pk_column
WHERE r.referenced_pk_column IS NULL
GROUP BY s.fk_column
ORDER BY orphaned_count DESC
