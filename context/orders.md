---
table: orders
schema: analytics
last_updated: 2026-04-04T17:57:01.032008+00:00
---

> ⚠️ Auto-generated from sandbox migration. Fill in the prompts below before merging — it takes 2 minutes and saves hours later.

# orders

## Purpose

> _What is the `orders` table built for overall? What business question does it answer? (Migration description: "orders" — expand on the broader purpose of this table below.)_

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
