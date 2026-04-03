-- Migration: 2026-04-03T00:05:50.272429+00:00
-- Table:     analytics.orders
-- Sandbox:   sbx_orders_20260403_0005
-- Branch:    migration/add-region-column-to-orders-20260403-0005

ALTER TABLE analytics.orders ADD COLUMN region VARCHAR DEFAULT 'EMEA'; UPDATE analytics.orders SET region = 'EMEA';
