-- negative checks for insert_spend_with_exchange preconditions
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/spend_with_exchange_negative_checks.sql

BEGIN;

DELETE FROM cash_flow WHERE users_id = 903001;
DELETE FROM categories_category_groups WHERE users_id = 903001;
DELETE FROM users_groups WHERE users_id = 903001;
DELETE FROM users WHERE id = 903001;
DELETE FROM categories WHERE id IN (903011, 903012);
DELETE FROM category_groups WHERE id IN (9, 14);
DELETE FROM exchange_rates WHERE currency IN ('USD', 'RUB', 'USDT');

INSERT INTO users(id, nickname) VALUES (903001, 'neg_fx');
INSERT INTO users_groups(users_id, users_groups) VALUES (903001, 9301);

INSERT INTO category_groups(id, "name", description) VALUES
  (9, 'reserve_group', ''),
  (14, 'all_group', '');

INSERT INTO categories(id, "name", "percent") VALUES
  (903011, 'ReserveNeg', 0.00),
  (903012, 'SpendNeg', 0.00);

DO $$
BEGIN
    -- 1) Missing rates should fail
    BEGIN
        PERFORM public.insert_spend_with_exchange(903001, 'SpendNeg', 10::numeric, 'USDT', 'neg1');
        RAISE EXCEPTION 'Expected failure for missing rates';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('Exchange rates for' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;

    -- Prepare rates for next checks
    WITH ts AS (SELECT now() AS t)
    INSERT INTO exchange_rates("datetime", currency, rate)
    SELECT t, 'USD', 1 FROM ts
    UNION ALL
    SELECT t, 'RUB', 80 FROM ts
    UNION ALL
    SELECT t, 'USDT', 1 FROM ts;

    -- 2) Missing reserve category (group 9) should fail
    INSERT INTO categories_category_groups(categories_id, category_groyps_id, users_id)
    VALUES (903012, 14, 903001);

    BEGIN
        PERFORM public.insert_spend_with_exchange(903001, 'SpendNeg', 10::numeric, 'USDT', 'neg2');
        RAISE EXCEPTION 'Expected failure for missing reserve category';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('Reserve category' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;

    -- 3) Missing spend category in group 14 should fail
    INSERT INTO categories_category_groups(categories_id, category_groyps_id, users_id)
    VALUES (903011, 9, 903001);

    BEGIN
        PERFORM public.insert_spend_with_exchange(903001, 'UnknownCategory', 10::numeric, 'USDT', 'neg3');
        RAISE EXCEPTION 'Expected failure for missing category in group 14';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('not found in group 14' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;

    -- 4) Non-positive value should fail
    BEGIN
        PERFORM public.insert_spend_with_exchange(903001, 'SpendNeg', 0::numeric, 'USDT', 'neg4');
        RAISE EXCEPTION 'Expected failure for non-positive spend value';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('must be greater than zero' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;
END $$;

ROLLBACK;
