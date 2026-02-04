-- checks for insert_spend_with_exchange and conversion invariants
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/spend_with_exchange_checks.sql

BEGIN;

-- Clean and fixture setup
DELETE FROM cash_flow WHERE users_id = 900201;
DELETE FROM categories_category_groups WHERE users_id = 900201;
DELETE FROM users_groups WHERE users_id = 900201;
DELETE FROM users WHERE id = 900201;
DELETE FROM categories WHERE id IN (900211, 900212);
DELETE FROM category_groups WHERE id IN (9, 14);
DELETE FROM exchange_rates WHERE currency IN ('USD', 'RUB', 'USDT');

INSERT INTO users(id, nickname) VALUES (900201, 'spend_fx_user');
INSERT INTO users_groups(users_id, users_groups) VALUES (900201, 8201);

INSERT INTO category_groups(id, "name", description)
VALUES (9, 'reserve_group', ''), (14, 'all_group', '');

INSERT INTO categories(id, "name", "percent")
VALUES
  (900211, 'Reserve FX', 0.00),
  (900212, 'Travel', 0.00);

INSERT INTO categories_category_groups(categories_id, category_groyps_id, users_id)
VALUES
  (900211, 9, 900201),
  (900211, 14, 900201),
  (900212, 14, 900201);

-- Same timestamp for RUB and USDT because insert_spend_with_exchange joins rates by datetime
WITH ts AS (SELECT now() AS t)
INSERT INTO exchange_rates("datetime", currency, rate)
SELECT t, 'USD', 1 FROM ts
UNION ALL
SELECT t, 'RUB', 80 FROM ts
UNION ALL
SELECT t, 'USDT', 1 FROM ts;

-- action
SELECT public.insert_spend_with_exchange(900201, 'Travel', 100::numeric, 'USDT', 'fx test');

-- assertions
DO $$
DECLARE
    flow_rows int;
    spend_rows int;
    auto_rows int;
    rub_spend numeric;
    travel_remains numeric;
BEGIN
    SELECT count(*) INTO flow_rows
    FROM cash_flow
    WHERE users_id = 900201;

    IF flow_rows <> 3 THEN
        RAISE EXCEPTION 'Expected 3 cash_flow rows, got %', flow_rows;
    END IF;

    SELECT count(*) INTO spend_rows
    FROM cash_flow
    WHERE users_id = 900201
      AND category_id_from = 900212
      AND category_id_to IS NULL
      AND currency = 'USDT'
      AND value = 100;

    IF spend_rows <> 1 THEN
        RAISE EXCEPTION 'Expected 1 spend row in USDT=100, got %', spend_rows;
    END IF;

    SELECT count(*) INTO auto_rows
    FROM cash_flow
    WHERE users_id = 900201
      AND description LIKE 'auto exchange%';

    IF auto_rows <> 2 THEN
        RAISE EXCEPTION 'Expected 2 auto exchange rows, got %', auto_rows;
    END IF;

    SELECT value INTO rub_spend
    FROM cash_flow
    WHERE users_id = 900201
      AND category_id_from = 900212
      AND category_id_to = 900211
      AND currency = 'RUB'
    ORDER BY id DESC
    LIMIT 1;

    IF rub_spend IS NULL OR abs(rub_spend - 8000) > 1e-9 THEN
        RAISE EXCEPTION 'Expected RUB conversion value 8000, got %', rub_spend;
    END IF;

    SELECT public.get_remains(900201, 'Travel') INTO travel_remains;
    IF travel_remains IS NULL OR abs(travel_remains + 8000) > 1e-9 THEN
        RAISE EXCEPTION 'Expected Travel remains = -8000, got %', travel_remains;
    END IF;
END $$;

ROLLBACK;
