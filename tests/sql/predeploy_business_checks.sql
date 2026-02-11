-- predeploy business checks for exchange and formatting
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/predeploy_business_checks.sql

BEGIN;

-- Test fixtures
DELETE FROM cash_flow WHERE users_id = 900001;
DELETE FROM exchange_rates WHERE currency IN ('USD', 'USDT', 'ETH', 'RUB', 'AAA', 'BBB');
DELETE FROM categories WHERE id = 900001;
DELETE FROM users WHERE id = 900001;

INSERT INTO users(id, nickname) VALUES (900001, 'testuser');
INSERT INTO categories(id, "name", "percent") VALUES (900001, 'Test Wallet', 0.00);

INSERT INTO exchange_rates("datetime", currency, rate) VALUES (now(), 'USD', 1);

-- 1) USD -> USDT: updates USDT (anchor USD=1)
SELECT public.exchange(900001, 900001, 100::numeric, 'USD', 99::numeric, 'USDT');
DO $$
DECLARE r numeric;
BEGIN
    SELECT rate INTO r FROM exchange_rates WHERE currency = 'USDT' ORDER BY datetime DESC LIMIT 1;
    IF r IS NULL OR abs(r - 0.99) > 1e-12 THEN
        RAISE EXCEPTION 'Test failed: expected USDT rate 0.99, got %', r;
    END IF;
END $$;

-- 2) RUB -> USDT: when receiving stablecoin, update non-stable side (RUB)
SELECT public.exchange(900001, 900001, 80::numeric, 'RUB', 1::numeric, 'USDT');
DO $$
DECLARE rub_rate numeric;
DECLARE usdt_rate numeric;
BEGIN
    SELECT rate INTO rub_rate FROM exchange_rates WHERE currency = 'RUB' ORDER BY datetime DESC LIMIT 1;
    SELECT rate INTO usdt_rate FROM exchange_rates WHERE currency = 'USDT' ORDER BY datetime DESC LIMIT 1;

    IF rub_rate IS NULL OR abs(rub_rate - 79.2) > 1e-12 THEN
        RAISE EXCEPTION 'Test failed: expected RUB rate 79.2, got %', rub_rate;
    END IF;

    IF usdt_rate IS NULL OR abs(usdt_rate - 0.99) > 1e-12 THEN
        RAISE EXCEPTION 'Test failed: expected USDT unchanged at 0.99, got %', usdt_rate;
    END IF;
END $$;

-- 3) USDT -> ETH: paying stablecoin updates received non-stable (ETH)
SELECT public.exchange(900001, 900001, 1::numeric, 'USDT', 0.0004::numeric, 'ETH');
DO $$
DECLARE eth_rate numeric;
BEGIN
    SELECT rate INTO eth_rate FROM exchange_rates WHERE currency = 'ETH' ORDER BY datetime DESC LIMIT 1;
    IF eth_rate IS NULL OR abs(eth_rate - 0.000396) > 1e-12 THEN
        RAISE EXCEPTION 'Test failed: expected ETH rate 0.000396, got %', eth_rate;
    END IF;
END $$;

-- 4) ETH -> RUB: no USD/stable in target, update received currency (RUB)
SELECT public.exchange(900001, 900001, 0.0005::numeric, 'ETH', 100::numeric, 'RUB');
DO $$
DECLARE rub_rate numeric;
BEGIN
    SELECT rate INTO rub_rate FROM exchange_rates WHERE currency = 'RUB' ORDER BY datetime DESC LIMIT 1;
    IF rub_rate IS NULL OR abs(rub_rate - 79.2) > 1e-9 THEN
        RAISE EXCEPTION 'Test failed: expected RUB rate 79.2 after ETH->RUB, got %', rub_rate;
    END IF;
END $$;

-- 5) Unknown pair should fail
DO $$
BEGIN
    BEGIN
        PERFORM public.exchange(900001, 900001, 1::numeric, 'AAA', 2::numeric, 'BBB');
        RAISE EXCEPTION 'Test failed: expected exception for unknown pair AAA/BBB';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('unknown' IN LOWER(SQLERRM)) = 0 THEN
                RAISE;
            END IF;
    END;
END $$;

-- 6) SQL formatting check used by functions: large rounded, small full
DO $$
DECLARE f_large text;
DECLARE f_small text;
BEGIN
    SELECT CASE
        WHEN ABS(v) >= 1 THEN REPLACE(TO_CHAR(v, 'FM999,999,999,999,999,999,990.00'), ',', ' ')
        WHEN v::text LIKE '%.%' THEN RTRIM(TRIM(TRAILING '0' FROM v::text), '.')
        ELSE v::text
    END INTO f_large
    FROM (VALUES (12345.678::numeric)) t(v);

    SELECT CASE
        WHEN ABS(v) >= 1 THEN REPLACE(TO_CHAR(v, 'FM999,999,999,999,999,999,990.00'), ',', ' ')
        WHEN v::text LIKE '%.%' THEN RTRIM(TRIM(TRAILING '0' FROM v::text), '.')
        ELSE v::text
    END INTO f_small
    FROM (VALUES (0.0001234500::numeric)) t(v);

    IF f_large <> '12 345.68' THEN
        RAISE EXCEPTION 'Test failed: expected 12 345.68, got %', f_large;
    END IF;
    IF f_small <> '0.00012345' THEN
        RAISE EXCEPTION 'Test failed: expected 0.00012345, got %', f_small;
    END IF;
END $$;

-- 7) get_last_transaction returns varchar value (no type mismatch)
DO $$
DECLARE v text;
DECLARE t text;
BEGIN
    SELECT value, pg_typeof(value)::text
    INTO v, t
    FROM get_last_transaction(900001, 1)
    LIMIT 1;

    IF t <> 'character varying' THEN
        RAISE EXCEPTION 'Test failed: expected value type character varying, got %', t;
    END IF;
END $$;

ROLLBACK;
