-- Migration: 2026-04-02T16:47:11.952863+00:00
-- Table:     analytics.orders
-- Sandbox:   sbx_orders_20260402_1641
-- Quality:   c1936eb4-c57e-4913-b648-6abd465fd1f3
-- Increase amount 50% for odd customer_id orders

UPDATE analytics.orders SET amount = amount * 1.5 WHERE customer_id % 2 = 1;
