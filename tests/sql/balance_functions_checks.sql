-- checks for get_group_balance / get_all_balances / get_category_balance_with_currency
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/balance_functions_checks.sql

BEGIN;

-- deterministic cleanup
DELETE FROM allocation_postings WHERE user_id IN (902101, 902102);
DELETE FROM allocation_node_groups
WHERE node_id IN (
    SELECT id
    FROM allocation_nodes
    WHERE user_id IN (902101, 902102)
       OR legacy_category_id IN (902211, 902212)
);
DELETE FROM allocation_nodes WHERE user_id IN (902101, 902102) OR legacy_category_id IN (902211, 902212);
DELETE FROM user_group_memberships WHERE user_id IN (902101, 902102);
DELETE FROM user_groups WHERE slug = 'balance_test_group';
DELETE FROM users WHERE id IN (902101, 902102);
DELETE FROM categories WHERE id IN (902211, 902212);
DELETE FROM category_groups WHERE id IN (9808, 9814);
DELETE FROM exchange_rates WHERE currency IN ('USD', 'RUB', 'ETH');

-- users in same runtime household group
INSERT INTO users(id, nickname) VALUES
  (902101, 'bal_u1'),
  (902102, 'bal_u2');

INSERT INTO user_groups(slug, "name", description)
VALUES ('balance_test_group', 'balance test group', 'fixture');

INSERT INTO user_group_memberships(user_id, user_group_id)
SELECT 902101, id FROM user_groups WHERE slug = 'balance_test_group';

INSERT INTO user_group_memberships(user_id, user_group_id)
SELECT 902102, id FROM user_groups WHERE slug = 'balance_test_group';

-- category groups now map through allocation_node_groups for canonical balance helpers
INSERT INTO category_groups(id, "name", description) VALUES
  (9808, 'test_spend_group', ''),
  (9814, 'test_all_group', '');

INSERT INTO categories(id, "name", "percent") VALUES
  (902211, 'FoodTest', 0.00),
  (902212, 'CryptoTest', 0.00);

-- rates (currency per 1 USD)
INSERT INTO exchange_rates("datetime", currency, rate) VALUES
  (now(), 'USD', 1),
  (now(), 'RUB', 100),
  (now(), 'ETH', 0.0005);

INSERT INTO allocation_nodes(
    id,
    user_id,
    slug,
    "name",
    description,
    node_kind,
    legacy_category_id,
    visible,
    include_in_report,
    active
)
VALUES
    (9022111, 902101, 'bal_food_node', 'FoodTest', 'food test node', 'expense', 902211, true, true, true),
    (9022121, 902101, 'bal_crypto_node', 'CryptoTest', 'crypto test node', 'expense', 902212, true, true, true);

INSERT INTO allocation_node_groups(node_id, legacy_group_id, active) VALUES
    (9022111, 9808, true),
    (9022111, 9814, true),
    (9022121, 9808, true),
    (9022121, 9814, true);

-- Food balance inputs (shared across users in same runtime household)
INSERT INTO allocation_postings(user_id, to_node_id, value, currency, description, metadata)
VALUES
    (902101, 9022111, 100, 'USD', 'food income usd', jsonb_build_object('kind', 'fixture', 'origin', 'balance_test'));

INSERT INTO allocation_postings(user_id, from_node_id, value, currency, description, metadata)
VALUES
    (902102, 9022111, 2000, 'RUB', 'food spend rub', jsonb_build_object('kind', 'fixture', 'origin', 'balance_test'));

-- Crypto balance inputs
INSERT INTO allocation_postings(user_id, to_node_id, value, currency, description, metadata)
VALUES
    (902101, 9022121, 0.02, 'ETH', 'crypto income eth', jsonb_build_object('kind', 'fixture', 'origin', 'balance_test'));

INSERT INTO allocation_postings(user_id, from_node_id, value, currency, description, metadata)
VALUES
    (902101, 9022121, 500, 'RUB', 'crypto spend rub', jsonb_build_object('kind', 'fixture', 'origin', 'balance_test'));

DO $$
DECLARE
    food_balance numeric;
    crypto_balance numeric;
    food_remains numeric;
    group_balance numeric;
    all_sum numeric;
    food_usd numeric;
    food_rub numeric;
BEGIN
    -- single category balances in RUB
    SELECT public.get_category_balance(902101, 902211, 'RUB') INTO food_balance;
    SELECT public.get_category_balance(902101, 902212, 'RUB') INTO crypto_balance;

    IF food_balance IS NULL OR abs(food_balance - 8000) > 1e-9 THEN
        RAISE EXCEPTION 'Expected FoodTest RUB balance 8000, got %', food_balance;
    END IF;

    IF crypto_balance IS NULL OR abs(crypto_balance - 3500) > 1e-9 THEN
        RAISE EXCEPTION 'Expected CryptoTest RUB balance 3500, got %', crypto_balance;
    END IF;

    SELECT public.get_remains(902101, 'FoodTest') INTO food_remains;
    IF food_remains IS NULL OR abs(food_remains - 8000) > 1e-9 THEN
        RAISE EXCEPTION 'Expected FoodTest remains 8000, got %', food_remains;
    END IF;

    -- group balance should equal sum of categories
    SELECT balance INTO group_balance FROM public.get_group_balance(902101, 9808) LIMIT 1;
    IF group_balance IS NULL OR abs(group_balance - 11500) > 1e-9 THEN
        RAISE EXCEPTION 'Expected group balance 11500, got %', group_balance;
    END IF;

    SELECT COALESCE(SUM(balance), 0) INTO all_sum
    FROM public.get_all_balances(902101, 9808);

    IF abs(all_sum - 11500) > 1e-9 THEN
        RAISE EXCEPTION 'Expected sum(get_all_balances)=11500, got %', all_sum;
    END IF;

    -- currency split for FoodTest
    SELECT value INTO food_usd
    FROM public.get_category_balance_with_currency(902101, 902211)
    WHERE currency = 'USD'
    LIMIT 1;

    SELECT value INTO food_rub
    FROM public.get_category_balance_with_currency(902101, 902211)
    WHERE currency = 'RUB'
    LIMIT 1;

    IF food_usd IS NULL OR abs(food_usd - 100) > 1e-9 THEN
        RAISE EXCEPTION 'Expected FoodTest USD split +100, got %', food_usd;
    END IF;

    IF food_rub IS NULL OR abs(food_rub + 2000) > 1e-9 THEN
        RAISE EXCEPTION 'Expected FoodTest RUB split -2000, got %', food_rub;
    END IF;
END $$;

ROLLBACK;
