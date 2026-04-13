# Migration Notes

**Branch:** `migration/add-region-column-to-orders-20260406-1219`  
**Date:** 2026-04-06T12:21:38.442943+00:00

---

## `analytics.orders`

**Sandbox:** `sbx_orders_20260406_1219`  
**Quality Run:** `15838d5b-94e5-4d81-9040-073f70ad2ac5` — ✅ All checks passed  

### SQL Applied

```sql
ALTER TABLE analytics.orders ADD COLUMN region VARCHAR DEFAULT 'INDIA'; UPDATE analytics.orders SET region = 'INDIA';
```

### Quality Checks

**✅ row_count_comparison — PASS**
  - Result: prod_row_count=50, sbx_row_count=50, row_diff=0, status=PASS
**✅ uniqueness_on_order_id — PASS**
  - Result: (no violations found)
**✅ region_no_nulls — PASS**
  - Result: status=PASS
**✅ region_value_is_india — PASS**
  - Result: status=PASS
