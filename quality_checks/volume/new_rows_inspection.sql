-- Shows rows present in sandbox but not in production (newly inserted rows).
-- Informational: always returns status='PASS'. Review the results to verify inserted rows look correct.
-- Use when: transformation inserts new rows (INSERT INTO ... SELECT, UNION, etc.).

SELECT s.*, 'PASS' AS status
FROM schema.sandbox_name s
LEFT JOIN schema.source_table p ON s.primary_key_column = p.primary_key_column
WHERE p.primary_key_column IS NULL
LIMIT 50
