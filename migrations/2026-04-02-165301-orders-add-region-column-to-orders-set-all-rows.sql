-- Migration: 2026-04-02T16:53:01.237598+00:00
-- Table:     analytics.orders
-- Sandbox:   sbx_orders_20260402_1651
-- Quality:   307b5aad-4e6d-486e-bcd5-0326ecacd697
-- Add region column to orders, set all rows to EMEA

UPDATE analytics.orders SET region = 'EMEA';
