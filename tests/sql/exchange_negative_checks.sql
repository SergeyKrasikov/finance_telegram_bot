-- negative checks for exchange preconditions and error branches
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/exchange_negative_checks.sql

BEGIN;

DELETE FROM cash_flow WHERE users_id = 904001;
DELETE FROM users WHERE id = 904001;
DELETE FROM categories WHERE id = 904011;
DELETE FROM exchange_rates WHERE currency IN ('AAA', 'BBB', 'USDT', 'ETH', 'RUB', 'USD');

INSERT INTO users(id, nickname) VALUES (904001, 'neg_ex_u');
INSERT INTO categories(id, "name", "percent") VALUES (904011, 'NegExWallet', 0.00);

DO $$
DECLARE
    before_usdt_count int;
    after_usdt_count int;
BEGIN
    -- 1) non-positive values
    BEGIN
        PERFORM public.exchange(904001, 904011, 0::numeric, 'USD', 1::numeric, 'USDT');
        RAISE EXCEPTION 'Expected failure for non-positive values';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('must be greater than zero' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;

    -- 2) both rates unknown
    BEGIN
        PERFORM public.exchange(904001, 904011, 1::numeric, 'AAA', 2::numeric, 'BBB');
        RAISE EXCEPTION 'Expected failure for unknown pair AAA/BBB';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('Rates for' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;

    -- baseline known rates
    INSERT INTO exchange_rates("datetime", currency, rate) VALUES
      (now(), 'USD', 1),
      (now(), 'RUB', 100);

    -- 3) paying stablecoin without stable rate
    before_usdt_count := (SELECT count(*) FROM exchange_rates WHERE currency = 'USDT');
    BEGIN
        PERFORM public.exchange(904001, 904011, 1::numeric, 'USDT', 100::numeric, 'RUB');
        RAISE EXCEPTION 'Expected failure for unknown stablecoin rate (out)';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('Stablecoin rate is unknown' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;
    after_usdt_count := (SELECT count(*) FROM exchange_rates WHERE currency = 'USDT');
    IF before_usdt_count <> after_usdt_count THEN
        RAISE EXCEPTION 'USDT rate rows changed on failed exchange';
    END IF;

    -- 4) receiving stablecoin without stable rate
    BEGIN
        PERFORM public.exchange(904001, 904011, 100::numeric, 'RUB', 1::numeric, 'USDT');
        RAISE EXCEPTION 'Expected failure for unknown stablecoin rate (in)';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('Stablecoin rate is unknown' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;

    -- 5) no stable/USD pair, missing out-rate but in-rate exists
    BEGIN
        PERFORM public.exchange(904001, 904011, 1::numeric, 'ETH', 100::numeric, 'RUB');
        RAISE EXCEPTION 'Expected failure for unknown out-rate ETH';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('Rate for' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;

    IF (SELECT count(*) FROM cash_flow WHERE users_id = 904001) <> 0 THEN
        RAISE EXCEPTION 'Failed exchange should not create cash_flow rows';
    END IF;
END $$;

ROLLBACK;
