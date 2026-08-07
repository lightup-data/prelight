-- Migration: 2026-04-06T12:19:41.091174+00:00
-- Table:     analytics.orders
-- Sandbox:   sbx_orders_20260406_1219
-- Branch:    migration/add-region-column-to-orders-20260406-1219

ALTER TABLE analytics.orders ADD COLUMN region VARCHAR DEFAULT 'INDIA'; UPDATE analytics.orders SET region = 'INDIA';
