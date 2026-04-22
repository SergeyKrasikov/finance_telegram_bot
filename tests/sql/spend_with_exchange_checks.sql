-- checks for insert_spend_with_exchange_v2 and conversion invariants
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/spend_with_exchange_checks.sql

BEGIN;

-- Clean and fixture setup
DELETE FROM allocation_postings WHERE user_id = 900201;
DELETE FROM allocation_node_groups
WHERE node_id IN (
    SELECT id
    FROM allocation_nodes
    WHERE user_id = 900201 OR legacy_category_id IN (900211, 900212)
);
DELETE FROM allocation_nodes WHERE user_id = 900201 OR legacy_category_id IN (900211, 900212);
DELETE FROM cash_flow WHERE users_id = 900201;
DELETE FROM users WHERE id = 900201;
DELETE FROM categories WHERE id IN (900211, 900212);
DELETE FROM category_groups WHERE id IN (9, 14);
DELETE FROM exchange_rates WHERE currency IN ('USD', 'RUB', 'USDT');

INSERT INTO users(id, nickname) VALUES (900201, 'spend_fx');

INSERT INTO category_groups(id, "name", description)
VALUES (9, 'reserve_group', ''), (14, 'all_group', '');

INSERT INTO categories(id, "name", "percent")
VALUES
  (900211, 'Reserve FX', 0.00),
  (900212, 'Travel', 0.00);

INSERT INTO allocation_nodes(id, user_id, slug, "name", description, node_kind, legacy_category_id, visible, include_in_report, active)
VALUES
  (900221, 900201, 'reserve_fx', 'Reserve FX', 'test reserve node', 'expense', 900211, true, true, true),
  (900222, 900201, 'travel', 'Travel', 'test spend node', 'expense', 900212, true, true, true);

INSERT INTO allocation_node_groups(node_id, legacy_group_id, active)
VALUES
  (900221, 9, true),
  (900221, 14, true),
  (900222, 14, true);

-- Different timestamps: conversion must use latest independent rates
INSERT INTO exchange_rates("datetime", currency, rate)
VALUES
  (now() - interval '2 hour', 'USD', 1),
  (now() - interval '1 hour', 'RUB', 80),
  (now() - interval '10 minutes', 'USDT', 1);

-- action
SELECT public.insert_spend_with_exchange_v2(900201, 'Travel', 100::numeric, 'usdt', 'fx test');

-- assertions
DO $$
DECLARE
    flow_rows int;
    ledger_rows int;
    linked_legacy_rows int;
    spend_rows int;
    auto_rows int;
    rub_spend numeric;
    travel_balance numeric;
BEGIN
    SELECT count(*) INTO flow_rows
    FROM cash_flow
    WHERE users_id = 900201;

    IF flow_rows <> 0 THEN
        RAISE EXCEPTION 'Expected no cash_flow rows for ledger-only auto exchange spend, got %', flow_rows;
    END IF;

    SELECT count(*) INTO ledger_rows
    FROM allocation_postings
    WHERE user_id = 900201;

    IF ledger_rows <> 3 THEN
        RAISE EXCEPTION 'Expected 3 ledger rows, got %', ledger_rows;
    END IF;

    SELECT count(*) INTO linked_legacy_rows
    FROM allocation_postings
    WHERE user_id = 900201
      AND metadata ? 'legacy_cash_flow_id';

    IF linked_legacy_rows <> 0 THEN
        RAISE EXCEPTION 'Expected no legacy_cash_flow_id for ledger-only auto exchange spend, got %', linked_legacy_rows;
    END IF;

    SELECT count(*) INTO spend_rows
    FROM allocation_postings
    WHERE user_id = 900201
      AND from_node_id = 900222
      AND to_node_id IS NULL
      AND currency = 'USDT'
      AND value = 100
      AND metadata->>'kind' = 'transaction'
      AND metadata->>'subkind' = 'spend';

    IF spend_rows <> 1 THEN
        RAISE EXCEPTION 'Expected 1 spend row in USDT=100, got %', spend_rows;
    END IF;

    SELECT count(*) INTO auto_rows
    FROM allocation_postings
    WHERE user_id = 900201
      AND metadata->>'kind' = 'exchange'
      AND metadata->>'subkind' = 'auto';

    IF auto_rows <> 2 THEN
        RAISE EXCEPTION 'Expected 2 auto exchange rows, got %', auto_rows;
    END IF;

    SELECT value INTO rub_spend
    FROM allocation_postings
    WHERE user_id = 900201
      AND from_node_id = 900222
      AND to_node_id = 900221
      AND currency = 'RUB'
    ORDER BY id DESC
    LIMIT 1;

    IF rub_spend IS NULL OR abs(rub_spend - 8000) > 1e-9 THEN
        RAISE EXCEPTION 'Expected RUB conversion value 8000, got %', rub_spend;
    END IF;

    SELECT public.get_category_balance_v2(900201, 900212, 'RUB') INTO travel_balance;
    IF travel_balance IS NULL OR abs(travel_balance + 8000) > 1e-9 THEN
        RAISE EXCEPTION 'Expected Travel ledger balance = -8000, got %', travel_balance;
    END IF;
END $$;

-- Scenario 2: different timestamps for RUB and USDT, still should work with latest independent rates
DELETE FROM allocation_postings WHERE user_id = 900201;
DELETE FROM exchange_rates WHERE currency IN ('USD', 'RUB', 'USDT');

INSERT INTO exchange_rates("datetime", currency, rate) VALUES
  (now() - interval '2 day', 'USD', 1),
  (now() - interval '2 day', 'RUB', 70),
  (now() - interval '1 day', 'RUB', 90),
  (now(), 'USDT', 1);

SELECT public.insert_spend_with_exchange_v2(900201, 'Travel', 100::numeric, 'USDT', 'fx test 2');

DO $$
DECLARE
    rub_spend numeric;
    cash_flow_rows int;
BEGIN
    SELECT value INTO rub_spend
    FROM allocation_postings
    WHERE user_id = 900201
      AND from_node_id = 900222
      AND to_node_id = 900221
      AND currency = 'RUB'
    ORDER BY id DESC
    LIMIT 1;

    IF rub_spend IS NULL OR abs(rub_spend - 9000) > 1e-9 THEN
        RAISE EXCEPTION 'Expected RUB conversion value 9000 with independent rates, got %', rub_spend;
    END IF;

    SELECT count(*) INTO cash_flow_rows
    FROM cash_flow
    WHERE users_id = 900201;

    IF cash_flow_rows <> 0 THEN
        RAISE EXCEPTION 'Expected no cash_flow rows after second ledger-only scenario, got %', cash_flow_rows;
    END IF;
END $$;

ROLLBACK;
