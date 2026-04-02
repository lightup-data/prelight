-- Checks that all rows present in production still exist in the sandbox after transformation.
-- Pass: 0 rows returned (no production rows unexpectedly deleted or lost).
-- Use when: transformation deletes rows, applies filters, or deduplicates.

SELECT
  p.key_column          AS missing_from_sandbox
FROM schema.source_table p
LEFT JOIN schema.sandbox_name s ON p.key_column = s.key_column
WHERE s.key_column IS NULL
LIMIT 50
