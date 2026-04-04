-- Migration: 2026-04-04T17:56:28.341858+00:00
-- Table:     analytics.orders
-- Sandbox:   sbx_orders_20260404_1755
-- Branch:    migration/add-requested-by-column-to-orders-20260404-1755

ALTER TABLE analytics.orders ADD COLUMN requested_by VARCHAR;
UPDATE analytics.orders SET requested_by = 'n/a';
