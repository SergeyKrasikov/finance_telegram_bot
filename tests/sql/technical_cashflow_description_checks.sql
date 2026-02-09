-- checks for public.is_technical_cashflow_description
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/technical_cashflow_description_checks.sql

BEGIN;

DO $$
BEGIN
    IF NOT public.is_technical_cashflow_description('exchange to 5 USDT') THEN
        RAISE EXCEPTION 'Expected exchange row to be technical';
    END IF;

    IF NOT public.is_technical_cashflow_description('auto exchange 100 RUB to 1 USDT') THEN
        RAISE EXCEPTION 'Expected auto exchange row to be technical';
    END IF;

    IF NOT public.is_technical_cashflow_description('monthly distribute') THEN
        RAISE EXCEPTION 'Expected monthly distribute row to be technical';
    END IF;

    IF NOT public.is_technical_cashflow_description('internal: reserve move') THEN
        RAISE EXCEPTION 'Expected internal flag to be technical';
    END IF;

    IF public.is_technical_cashflow_description('salary from client') THEN
        RAISE EXCEPTION 'Expected regular income row to be non-technical';
    END IF;

    IF public.is_technical_cashflow_description(NULL) THEN
        RAISE EXCEPTION 'Expected NULL description to be non-technical';
    END IF;
END $$;

ROLLBACK;
