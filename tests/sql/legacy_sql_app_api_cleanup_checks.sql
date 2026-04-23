-- checks that transitional/legacy SQL app APIs are absent after runtime cutover
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/legacy_sql_app_api_cleanup_checks.sql

BEGIN;

DO $$
BEGIN
    IF to_regprocedure('public.monthly_distribute(bigint,integer)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy monthly_distribute(...) should be removed';
    END IF;

    IF to_regprocedure('public.distribute_to_group(bigint,integer,integer,numeric,varchar)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy distribute_to_group(...) should be removed';
    END IF;

    IF to_regprocedure('public.transact_from_group_to_category(bigint,integer,integer)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy transact_from_group_to_category(...) should be removed';
    END IF;

    IF to_regprocedure('public.insert_in_cash_flow(bigint,timestamp,integer,integer,integer,varchar,text)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy insert_in_cash_flow(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_categories_id(bigint,integer)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy get_categories_id(...) should be removed';
    END IF;

    IF to_regprocedure('public.mirror_cash_flow_row_to_allocation_postings(bigint,text,text,text,jsonb)') IS NOT NULL THEN
        RAISE EXCEPTION 'Dual-write mirror_cash_flow_row_to_allocation_postings(...) should be removed';
    END IF;

    IF to_regprocedure('public.insert_monthly_compat_investition_second(bigint,numeric,varchar,text)') IS NOT NULL THEN
        RAISE EXCEPTION 'Legacy monthly compatibility cash_flow insert should be removed';
    END IF;

    IF to_regprocedure('public.get_category_balance_v2(bigint,integer,varchar)') IS NOT NULL THEN
        RAISE EXCEPTION 'Transitional get_category_balance_v2(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_last_transaction_v2(bigint,integer)') IS NOT NULL THEN
        RAISE EXCEPTION 'Transitional get_last_transaction_v2(...) should be removed';
    END IF;

    IF to_regprocedure('public.insert_spend_v2(bigint,varchar,numeric,varchar,text)') IS NOT NULL THEN
        RAISE EXCEPTION 'Transitional insert_spend_v2(...) should be removed';
    END IF;

    IF to_regprocedure('public.insert_revenue_v2(bigint,varchar,numeric,varchar,text)') IS NOT NULL THEN
        RAISE EXCEPTION 'Transitional insert_revenue_v2(...) should be removed';
    END IF;

    IF to_regprocedure('public.insert_spend_with_exchange_v2(bigint,varchar,numeric,varchar,text)') IS NOT NULL THEN
        RAISE EXCEPTION 'Transitional insert_spend_with_exchange_v2(...) should be removed';
    END IF;

    IF to_regprocedure('public.exchange_v2(bigint,integer,numeric,varchar,numeric,varchar)') IS NOT NULL THEN
        RAISE EXCEPTION 'Transitional exchange_v2(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_categories_name_v2(bigint,integer)') IS NOT NULL THEN
        RAISE EXCEPTION 'Transitional get_categories_name_v2(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_group_balance_v2(bigint,integer)') IS NOT NULL THEN
        RAISE EXCEPTION 'Transitional get_group_balance_v2(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_remains_v2(bigint,character)') IS NOT NULL
       OR to_regprocedure('public.get_remains_v2(bigint,varchar)') IS NOT NULL THEN
        RAISE EXCEPTION 'Transitional get_remains_v2(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_all_balances_v2(bigint,integer)') IS NOT NULL THEN
        RAISE EXCEPTION 'Transitional get_all_balances_v2(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_category_id_from_name_v2(bigint,varchar)') IS NOT NULL THEN
        RAISE EXCEPTION 'Transitional get_category_id_from_name_v2(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_category_balance_with_currency_v2(bigint,integer)') IS NOT NULL THEN
        RAISE EXCEPTION 'Transitional get_category_balance_with_currency_v2(...) should be removed';
    END IF;

    IF to_regprocedure('public.get_currency_v2()') IS NOT NULL THEN
        RAISE EXCEPTION 'Transitional get_currency_v2() should be removed';
    END IF;

    IF to_regprocedure('public.get_last_transaction(bigint,integer)') IS NULL THEN
        RAISE EXCEPTION 'Expected canonical get_last_transaction(...) to remain available';
    END IF;

    IF to_regprocedure('public.get_category_balance(bigint,integer,varchar)') IS NULL THEN
        RAISE EXCEPTION 'Expected canonical get_category_balance(...) to remain available';
    END IF;

    IF to_regprocedure('public.get_categories_name(bigint,integer)') IS NULL THEN
        RAISE EXCEPTION 'Expected canonical get_categories_name(...) to remain available';
    END IF;

    IF to_regprocedure('public.get_category_id_from_name(bigint,varchar)') IS NULL THEN
        RAISE EXCEPTION 'Expected canonical get_category_id_from_name(...) to remain available';
    END IF;

    IF to_regprocedure('public.get_group_balance(bigint,integer)') IS NULL THEN
        RAISE EXCEPTION 'Expected canonical get_group_balance(...) to remain available';
    END IF;

    IF to_regprocedure('public.get_remains(bigint,varchar)') IS NULL THEN
        RAISE EXCEPTION 'Expected canonical get_remains(...) to remain available';
    END IF;

    IF to_regprocedure('public.get_all_balances(bigint,integer)') IS NULL THEN
        RAISE EXCEPTION 'Expected canonical get_all_balances(...) to remain available';
    END IF;

    IF to_regprocedure('public.get_category_balance_with_currency(bigint,integer)') IS NULL THEN
        RAISE EXCEPTION 'Expected canonical get_category_balance_with_currency(...) to remain available';
    END IF;

    IF to_regprocedure('public.get_currency()') IS NULL THEN
        RAISE EXCEPTION 'Expected canonical get_currency() to remain available';
    END IF;

    IF to_regprocedure('public.insert_spend(bigint,varchar,numeric,varchar,text)') IS NULL THEN
        RAISE EXCEPTION 'Expected canonical insert_spend(...) to remain available';
    END IF;

    IF to_regprocedure('public.insert_revenue(bigint,varchar,numeric,varchar,text)') IS NULL THEN
        RAISE EXCEPTION 'Expected canonical insert_revenue(...) to remain available';
    END IF;

    IF to_regprocedure('public.insert_spend_with_exchange(bigint,varchar,numeric,varchar,text)') IS NULL THEN
        RAISE EXCEPTION 'Expected canonical insert_spend_with_exchange(...) to remain available';
    END IF;

    IF to_regprocedure('public.exchange(bigint,integer,numeric,varchar,numeric,varchar)') IS NULL THEN
        RAISE EXCEPTION 'Expected canonical exchange(...) to remain available';
    END IF;
END $$;

ROLLBACK;
