---
table: orders
schema: analytics
last_updated: 2026-04-02T16:47:11.952863+00:00
---

# orders

## Purpose

The orders table records every customer purchase in the analytics schema. It tracks order amounts, statuses, and timestamps alongside customer references. A 50% amount increase was applied to all orders with odd customer_ids.

## Columns

| Column | Type | Description |
|--------|------|-------------|
| order_id | BIGINT | Reference to the associated order. |
| customer_id | BIGINT | Reference to the associated customer. |
| amount | DOUBLE | Monetary value in the record's currency. |
| status | VARCHAR | Current status of the record. |
| created_at | TIMESTAMP | Timestamp when the record was created. |

## Metrics

> _Define key metrics computable from this table. Example:_
> - **Revenue**: `SUM(amount) WHERE status = 'completed'`
