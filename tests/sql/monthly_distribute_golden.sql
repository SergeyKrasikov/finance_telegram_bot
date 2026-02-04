-- Golden test for monthly_distribute business logic
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/monthly_distribute_golden.sql

BEGIN;

-- Clean possible leftovers for deterministic fixture
DELETE FROM cash_flow WHERE users_id IN (900101, 900102);
DELETE FROM categories_category_groups WHERE users_id IN (900101, 900102);
DELETE FROM users_groups WHERE users_id IN (900101, 900102);
DELETE FROM users WHERE id IN (900101, 900102);
DELETE FROM categories WHERE id IN (6,7,9,13,15,101,102,103,104,106);
DELETE FROM category_groups WHERE id IN (1,2,3,4,6,7,8,9,11,12,13);
DELETE FROM exchange_rates WHERE currency IN ('USD','RUB');

-- Users and relation
INSERT INTO users(id, nickname) VALUES
    (900101, 'u1'),
    (900102, 'u2');

INSERT INTO users_groups(users_id, users_groups) VALUES
    (900101, 77),
    (900102, 77);

-- Required groups
INSERT INTO category_groups(id, "name", description) VALUES
    (1, 'g1', ''),
    (2, 'g2', ''),
    (3, 'g3', ''),
    (4, 'g4', ''),
    (6, 'g6', ''),
    (7, 'g7', ''),
    (8, 'g8', ''),
    (9, 'g9', ''),
    (11, 'g11', ''),
    (12, 'g12', ''),
    (13, 'g13', '');

-- Required categories
INSERT INTO categories(id, "name", "percent") VALUES
    (6, 'free_money', 1.00),
    (7, 'gifts', 1.00),
    (9, 'reserve', 1.00),
    (13, 'month_income', 1.00),
    (15, 'family_anchor', 0.50),
    (101, 'g1_target', 1.00),
    (102, 'g2_target', 1.00),
    (103, 'g3_target', 1.00),
    (104, 'g4_target', 1.00),
    (106, 'g6_second', 1.00);

-- Group mappings for user 900101
INSERT INTO categories_category_groups(categories_id, category_groyps_id, users_id) VALUES
    (13, 13, 900101),
    (7, 7, 900101),
    (6, 6, 900101),
    (101, 1, 900101),
    (102, 2, 900101),
    (104, 4, 900101);

-- Group mappings for user 900102
INSERT INTO categories_category_groups(categories_id, category_groyps_id, users_id) VALUES
    (103, 3, 900102),
    (106, 6, 900102);

-- Rates (RUB target conversion)
INSERT INTO exchange_rates("datetime", currency, rate) VALUES
    (now(), 'USD', 1),
    (now(), 'RUB', 1);

-- Current balances for monthly_distribute
-- income category 13 => 1000 RUB
INSERT INTO cash_flow(users_id, category_id_to, value, currency, description)
VALUES (900101, 13, 1000, 'RUB', 'fixture income');

-- free money category 6 => 200 RUB
INSERT INTO cash_flow(users_id, category_id_to, value, currency, description)
VALUES (900101, 6, 200, 'RUB', 'fixture free money');

-- Last month earnings/spend for reporting
INSERT INTO cash_flow(users_id, "datetime", category_id_to, value, currency, description)
VALUES (900101, date_trunc('month', now()) - interval '1 month' + interval '1 day', 13, 111, 'RUB', 'fixture month earnings');

INSERT INTO cash_flow(users_id, "datetime", category_id_from, value, currency, description)
VALUES (900101, date_trunc('month', now()) - interval '1 month' + interval '2 days', 13, 22, 'RUB', 'fixture month spend');

DO $$
DECLARE
    out jsonb;
BEGIN
    out := monthly_distribute(900101, 13);

    IF (out->>'user_id')::bigint <> 900101 THEN
        RAISE EXCEPTION 'Expected user_id=900101, got %', out->>'user_id';
    END IF;

    IF (out->>'second_user_id')::bigint <> 900102 THEN
        RAISE EXCEPTION 'Expected second_user_id=900102, got %', out->>'second_user_id';
    END IF;

    IF abs((out->>'семейный_взнос')::numeric - 500) > 1e-9 THEN
        RAISE EXCEPTION 'Expected семейный_взнос=500, got %', out->>'семейный_взнос';
    END IF;

    IF abs((out->>'общие_категории')::numeric - 500) > 1e-9 THEN
        RAISE EXCEPTION 'Expected общие_категории=500, got %', out->>'общие_категории';
    END IF;

    IF abs((out->>'investition')::numeric - 100) > 1e-9 THEN
        RAISE EXCEPTION 'Expected investition=100, got %', out->>'investition';
    END IF;

    IF abs((out->>'month_earnings')::numeric - 111) > 1e-9 THEN
        RAISE EXCEPTION 'Expected month_earnings=111, got %', out->>'month_earnings';
    END IF;

    IF abs((out->>'month_spend')::numeric - 22) > 1e-9 THEN
        RAISE EXCEPTION 'Expected month_spend=22, got %', out->>'month_spend';
    END IF;
END $$;

ROLLBACK;
