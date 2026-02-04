-- checks for currency code length compatibility (USDT/FDUSD/etc)
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/currency_code_length_checks.sql

BEGIN;

-- Minimal fixture
DELETE FROM cash_flow WHERE users_id = 905001;
DELETE FROM categories_category_groups WHERE users_id = 905001;
DELETE FROM users_groups WHERE users_id = 905001;
DELETE FROM users WHERE id = 905001;
DELETE FROM categories WHERE id = 905011;
DELETE FROM exchange_rates WHERE currency IN ('USD', 'USDT', 'FDUSD');

INSERT INTO users(id, nickname) VALUES (905001, 'lenchk');
INSERT INTO categories(id, "name", "percent") VALUES (905011, 'LenCheck', 1.00);
INSERT INTO categories_category_groups(categories_id, category_groyps_id, users_id)
VALUES (905011, 14, 905001);

DO $$
DECLARE
    cf_len int;
    er_len int;
    msg text;
BEGIN
    SELECT character_maximum_length
      INTO cf_len
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = 'cash_flow'
       AND column_name = 'currency';

    SELECT character_maximum_length
      INTO er_len
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = 'exchange_rates'
       AND column_name = 'currency';

    IF cf_len < 16 OR er_len < 16 THEN
        RAISE EXCEPTION 'Currency columns must be varchar(16)+, got cash_flow=% exchange_rates=%', cf_len, er_len;
    END IF;

    INSERT INTO exchange_rates("datetime", currency, rate) VALUES (now(), 'USD', 1);

    msg := public.exchange(905001, 905011, 100::numeric, 'USD', 99::numeric, 'USDT');
    IF msg NOT LIKE 'Курс:%USDT%' THEN
        RAISE EXCEPTION 'Unexpected exchange message for USDT: %', msg;
    END IF;

    msg := public.exchange(905001, 905011, 100::numeric, 'USD', 98::numeric, 'FDUSD');
    IF msg NOT LIKE 'Курс:%FDUSD%' THEN
        RAISE EXCEPTION 'Unexpected exchange message for FDUSD: %', msg;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM cash_flow
        WHERE users_id = 905001
          AND currency IN ('USDT', 'FDUSD')
    ) THEN
        RAISE EXCEPTION 'Expected cash_flow rows with long currency codes';
    END IF;
END $$;

ROLLBACK;
