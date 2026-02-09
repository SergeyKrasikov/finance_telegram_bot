-- checks for get_group_balance / get_all_balances / get_category_balance_with_currency
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/balance_functions_checks.sql

BEGIN;

-- deterministic cleanup
DELETE FROM cash_flow WHERE users_id IN (902101, 902102);
DELETE FROM categories_category_groups WHERE users_id IN (902101, 902102);
DELETE FROM users_groups WHERE users_id IN (902101, 902102);
DELETE FROM users WHERE id IN (902101, 902102);
DELETE FROM categories WHERE id IN (902211, 902212);
DELETE FROM category_groups WHERE id IN (9808, 9814);
DELETE FROM exchange_rates WHERE currency IN ('USD', 'RUB', 'ETH');

-- users in same family group (get_users_id should include both)
INSERT INTO users(id, nickname) VALUES
  (902101, 'bal_u1'),
  (902102, 'bal_u2');

INSERT INTO users_groups(users_id, users_groups) VALUES
  (902101, 9201),
  (902102, 9201);

-- category groups
INSERT INTO category_groups(id, "name", description) VALUES
  (9808, 'test_spend_group', ''),
  (9814, 'test_all_group', '');

-- categories
INSERT INTO categories(id, "name", "percent") VALUES
  (902211, 'FoodTest', 0.00),
  (902212, 'CryptoTest', 0.00);

-- map categories to user1 groups (functions use user1 mapping for categories list)
INSERT INTO categories_category_groups(categories_id, category_groyps_id, users_id) VALUES
  (902211, 9808, 902101),
  (902212, 9808, 902101),
  (902211, 9814, 902101),
  (902212, 9814, 902101);

-- rates (currency per 1 USD)
INSERT INTO exchange_rates("datetime", currency, rate) VALUES
  (now(), 'USD', 1),
  (now(), 'RUB', 100),
  (now(), 'ETH', 0.0005);

-- Food balance inputs (shared across users in same users_group)
INSERT INTO cash_flow(users_id, category_id_to, value, currency, description)
VALUES (902101, 902211, 100, 'USD', 'food income usd');

INSERT INTO cash_flow(users_id, category_id_from, value, currency, description)
VALUES (902102, 902211, 2000, 'RUB', 'food spend rub');

-- Crypto balance inputs
INSERT INTO cash_flow(users_id, category_id_to, value, currency, description)
VALUES (902101, 902212, 0.02, 'ETH', 'crypto income eth');

INSERT INTO cash_flow(users_id, category_id_from, value, currency, description)
VALUES (902101, 902212, 500, 'RUB', 'crypto spend rub');

DO $$
DECLARE
    food_balance numeric;
    crypto_balance numeric;
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
