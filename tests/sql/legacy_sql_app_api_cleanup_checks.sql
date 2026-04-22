-- checks that legacy app-facing SQL functions were removed after runtime cutover
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/legacy_sql_app_api_cleanup_checks.sql

BEGIN;

DO $$
BEGIN
    IF to_regprocedure('public.get_last_transaction(bigint,integer)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy get_last_transaction(...) should be removed';
    END IF;

    IF to_regprocedure('public.insert_in_cash_flow(bigint,timestamp,integer,integer,integer,varchar,text)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy insert_in_cash_flow(...) should be removed';
    END IF;

    IF to_regprocedure('public.insert_spend(bigint,varchar,numeric,varchar,text)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy insert_spend(...) should be removed';
    END IF;

    IF to_regprocedure('public.insert_revenue(bigint,varchar,numeric,varchar,text)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy insert_revenue(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_categories_name(bigint,integer)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy get_categories_name(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_group_balance(bigint,integer)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy get_group_balance(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_remains(bigint,character)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy get_remains(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_all_balances(bigint,integer)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy get_all_balances(...) should be removed';
    END IF;

    IF to_regprocedure('public.exchange(bigint,integer,numeric,varchar,numeric,varchar)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy exchange(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_category_id_from_name(varchar)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy get_category_id_from_name(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_category_balance_with_currency(bigint,integer)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy get_category_balance_with_currency(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_currency()') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy get_currency() should be removed';
    END IF;

    IF to_regprocedure('public.insert_spend_with_exchange(bigint,varchar,numeric,varchar,text)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy insert_spend_with_exchange(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_last_transaction_v2(bigint,integer)') IS NULL THEN
        RAISE EXCEPTION 'Expected get_last_transaction_v2(...) to remain available';
    END IF;

    IF to_regprocedure('public.insert_spend_v2(bigint,varchar,numeric,varchar,text)') IS NULL THEN
        RAISE EXCEPTION 'Expected insert_spend_v2(...) to remain available';
    END IF;

    IF to_regprocedure('public.insert_revenue_v2(bigint,varchar,numeric,varchar,text)') IS NULL THEN
        RAISE EXCEPTION 'Expected insert_revenue_v2(...) to remain available';
    END IF;

    IF to_regprocedure('public.insert_spend_with_exchange_v2(bigint,varchar,numeric,varchar,text)') IS NULL THEN
        RAISE EXCEPTION 'Expected insert_spend_with_exchange_v2(...) to remain available';
    END IF;

    IF to_regprocedure('public.exchange_v2(bigint,integer,numeric,varchar,numeric,varchar)') IS NULL THEN
        RAISE EXCEPTION 'Expected exchange_v2(...) to remain available';
    END IF;
END $$;

ROLLBACK;
