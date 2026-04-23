-- guards that live runtime functions do not depend on legacy tables
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/runtime_legacy_boundary_checks.sql

BEGIN;

DO $$
DECLARE
    exchange_def text;
    spend_def text;
    revenue_def text;
    spend_fx_def text;
    daily_def text;
    history_def text;
    category_balance_def text;
    categories_def text;
    category_id_def text;
    balances_def text;
    group_balance_def text;
    remains_def text;
    category_currency_def text;
    currency_def text;
    delete_def text;
    monthly_entry_def text;
    monthly_cascade_def text;
BEGIN
    SELECT pg_get_functiondef('public.exchange(bigint,integer,numeric,varchar,numeric,varchar)'::regprocedure)
    INTO exchange_def;

    SELECT pg_get_functiondef('public.insert_spend(bigint,varchar,numeric,varchar,text)'::regprocedure)
    INTO spend_def;

    SELECT pg_get_functiondef('public.insert_revenue(bigint,varchar,numeric,varchar,text)'::regprocedure)
    INTO revenue_def;

    SELECT pg_get_functiondef('public.insert_spend_with_exchange(bigint,varchar,numeric,varchar,text)'::regprocedure)
    INTO spend_fx_def;

    SELECT pg_get_functiondef('public.get_daily_transactions(bigint)'::regprocedure)
    INTO daily_def;

    SELECT pg_get_functiondef('public.get_last_transaction(bigint,integer)'::regprocedure)
    INTO history_def;

    SELECT pg_get_functiondef('public.get_category_balance(bigint,integer,varchar)'::regprocedure)
    INTO category_balance_def;

    SELECT pg_get_functiondef('public.get_categories_name(bigint,integer)'::regprocedure)
    INTO categories_def;

    SELECT pg_get_functiondef('public.get_category_id_from_name(bigint,varchar)'::regprocedure)
    INTO category_id_def;

    SELECT pg_get_functiondef('public.get_all_balances(bigint,integer)'::regprocedure)
    INTO balances_def;

    SELECT pg_get_functiondef('public.get_group_balance(bigint,integer)'::regprocedure)
    INTO group_balance_def;

    SELECT pg_get_functiondef('public.get_remains(bigint,varchar)'::regprocedure)
    INTO remains_def;

    SELECT pg_get_functiondef('public.get_category_balance_with_currency(bigint,integer)'::regprocedure)
    INTO category_currency_def;

    SELECT pg_get_functiondef('public.get_currency()'::regprocedure)
    INTO currency_def;

    SELECT pg_get_functiondef('public.delete_transaction(bigint[])'::regprocedure)
    INTO delete_def;

    SELECT pg_get_functiondef('public.monthly()'::regprocedure)
    INTO monthly_entry_def;

    SELECT pg_get_functiondef('public.monthly_distribute_cascade(bigint)'::regprocedure)
    INTO monthly_cascade_def;

    IF POSITION('cash_flow' IN exchange_def) > 0 THEN
        RAISE EXCEPTION 'exchange() should not reference cash_flow';
    END IF;

    IF POSITION('cash_flow' IN spend_def) > 0 THEN
        RAISE EXCEPTION 'insert_spend() should not reference cash_flow';
    END IF;

    IF POSITION('cash_flow' IN revenue_def) > 0 THEN
        RAISE EXCEPTION 'insert_revenue() should not reference cash_flow';
    END IF;

    IF POSITION('cash_flow' IN spend_fx_def) > 0 THEN
        RAISE EXCEPTION 'insert_spend_with_exchange() should not reference cash_flow';
    END IF;

    IF POSITION('cash_flow' IN daily_def) > 0 THEN
        RAISE EXCEPTION 'get_daily_transactions() should not reference cash_flow';
    END IF;

    IF POSITION('cash_flow' IN history_def) > 0 THEN
        RAISE EXCEPTION 'get_last_transaction() should not reference cash_flow';
    END IF;

    IF POSITION('cash_flow' IN balances_def) > 0 THEN
        RAISE EXCEPTION 'get_all_balances() should not reference cash_flow';
    END IF;

    IF POSITION('cash_flow' IN category_balance_def) > 0
       OR POSITION('cash_flow' IN categories_def) > 0
       OR POSITION('cash_flow' IN category_id_def) > 0
       OR POSITION('cash_flow' IN group_balance_def) > 0
       OR POSITION('cash_flow' IN remains_def) > 0
       OR POSITION('cash_flow' IN category_currency_def) > 0
       OR POSITION('cash_flow' IN currency_def) > 0 THEN
        RAISE EXCEPTION 'Canonical read helpers should not reference cash_flow';
    END IF;

    IF POSITION('DELETE FROM public.cash_flow' IN delete_def) > 0
       OR POSITION('DELETE FROM cash_flow' IN delete_def) > 0 THEN
        RAISE EXCEPTION 'delete_transaction() should not delete from cash_flow';
    END IF;

    IF POSITION('categories_category_groups' IN exchange_def) > 0
       OR POSITION('categories_category_groups' IN spend_def) > 0
       OR POSITION('categories_category_groups' IN revenue_def) > 0
       OR POSITION('categories_category_groups' IN spend_fx_def) > 0
       OR POSITION('categories_category_groups' IN daily_def) > 0
       OR POSITION('categories_category_groups' IN history_def) > 0
       OR POSITION('categories_category_groups' IN category_balance_def) > 0
       OR POSITION('categories_category_groups' IN categories_def) > 0
       OR POSITION('categories_category_groups' IN category_id_def) > 0
       OR POSITION('categories_category_groups' IN balances_def) > 0
       OR POSITION('categories_category_groups' IN group_balance_def) > 0
       OR POSITION('categories_category_groups' IN remains_def) > 0
       OR POSITION('categories_category_groups' IN category_currency_def) > 0
       OR POSITION('categories_category_groups' IN currency_def) > 0
       OR POSITION('categories_category_groups' IN monthly_entry_def) > 0
       OR POSITION('categories_category_groups' IN monthly_cascade_def) > 0 THEN
        RAISE EXCEPTION 'Live runtime function still references categories_category_groups';
    END IF;

    IF POSITION('users_groups' IN exchange_def) > 0
       OR POSITION('users_groups' IN spend_def) > 0
       OR POSITION('users_groups' IN revenue_def) > 0
       OR POSITION('users_groups' IN spend_fx_def) > 0
       OR POSITION('users_groups' IN daily_def) > 0
       OR POSITION('users_groups' IN history_def) > 0
       OR POSITION('users_groups' IN category_balance_def) > 0
       OR POSITION('users_groups' IN categories_def) > 0
       OR POSITION('users_groups' IN category_id_def) > 0
       OR POSITION('users_groups' IN balances_def) > 0
       OR POSITION('users_groups' IN group_balance_def) > 0
       OR POSITION('users_groups' IN remains_def) > 0
       OR POSITION('users_groups' IN category_currency_def) > 0
       OR POSITION('users_groups' IN currency_def) > 0
       OR POSITION('users_groups' IN monthly_entry_def) > 0
       OR POSITION('users_groups' IN monthly_cascade_def) > 0 THEN
        RAISE EXCEPTION 'Live runtime function still references users_groups';
    END IF;
END $$;

ROLLBACK;
