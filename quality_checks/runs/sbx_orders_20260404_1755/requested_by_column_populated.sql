-- Verifies that all rows in the sandbox have requested_by column set to 'n/a' with no null or incorrect values
-- Sandbox: sbx_orders_20260404_1755

SELECT
  COUNT(*) AS total_rows,
  COUNT(CASE WHEN requested_by = 'n/a' THEN 1 END) AS correct_values,
  COUNT(CASE WHEN requested_by IS NULL OR requested_by != 'n/a' THEN 1 END) AS incorrect_values,
  CASE
    WHEN COUNT(CASE WHEN requested_by IS NULL OR requested_by != 'n/a' THEN 1 END) = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS status
FROM analytics.sbx_orders_20260404_1755
