-- Deploy: exchange logic + unified numeric formatting
-- Date: 2026-02-03

BEGIN;

-- 1) USD anchor for fresh/existing DB (idempotent)
INSERT INTO exchange_rates ("datetime", currency, rate)
SELECT now(), 'USD', 1
WHERE NOT EXISTS (
    SELECT 1 FROM exchange_rates WHERE currency = 'USD'
);

-- 2) get_remains: fixed quoted column name c."name"
CREATE OR REPLACE FUNCTION public.get_remains(_user_id bigint, _category CHARACTER)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
begin
return (
    select coalesce(
        get_category_balance(
            _user_id,
            (
                select c.id
                from categories c
                join categories_category_groups ccg on c.id = ccg.categories_id
                where ccg.category_groyps_id = 14
                  and ccg.users_id = _user_id
                  and c."name" = _category
            )
        ),
        0
    )
);
end
$function$;

-- 3) get_last_transaction: unified formatting
CREATE OR REPLACE FUNCTION get_last_transaction(_user_id bigint, _num int)
RETURNS TABLE (
    id bigint,
    datetime timestamp,
    "from" varchar(100),
    "to" varchar(100),
    value varchar,
    currency varchar(3),
    description text
)
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY (
        SELECT
            cf.id,
            cf.datetime,
            c."name" AS "from",
            c2."name" AS "to",
            CASE
                WHEN ABS(cf.value) >= 1 THEN REPLACE(TO_CHAR(cf.value, 'FM999,999,999,999,999,999,990.00'), ',', ' ')
                WHEN cf.value::text LIKE '%.%' THEN RTRIM(TRIM(TRAILING '0' FROM cf.value::text), '.')
                ELSE cf.value::text
            END AS value,
            cf.currency,
            cf.description
        FROM (
            SELECT
                cf_sub.id,
                cf_sub.datetime,
                cf_sub.category_id_from,
                cf_sub.category_id_to,
                cf_sub.value,
                cf_sub.currency,
                cf_sub.description,
                dense_rank() OVER (ORDER BY cf_sub.datetime DESC) AS "rank"
            FROM cash_flow cf_sub
            WHERE users_id = _user_id
        ) cf
        LEFT JOIN categories c ON cf.category_id_from = c.id
        LEFT JOIN categories c2 ON cf.category_id_to = c2.id
        WHERE "rank" = _num
    );
END;
$function$;

-- 4) get_daily_transactions: unified formatting
CREATE OR REPLACE FUNCTION public.get_daily_transactions(_user_id bigint)
RETURNS TABLE(transact text)
LANGUAGE sql
AS $function$
SELECT CONCAT_WS(' ',
    c."name",
    COALESCE(c2."name", '-'),
    CASE
        WHEN ABS(cf.value) >= 1 THEN REPLACE(TO_CHAR(cf.value, 'FM999,999,999,999,999,999,990.00'), ',', ' ')
        WHEN cf.value::text LIKE '%.%' THEN RTRIM(TRIM(TRAILING '0' FROM cf.value::text), '.')
        ELSE cf.value::text
    END,
    cf.currency
) AS transact
FROM cash_flow cf
LEFT JOIN categories c ON cf.category_id_from = c.id
LEFT JOIN categories c2 ON cf.category_id_to = c2.id
WHERE date_trunc('day', cf.datetime) = date_trunc('day', now())
  AND users_id = _user_id
ORDER BY cf.datetime;
$function$;

-- 5) get_category_balance_with_currency: fixed output column name currency
CREATE OR REPLACE FUNCTION public.get_category_balance_with_currency(_user_id bigint, _category_id integer)
 RETURNS TABLE (value numeric, currency varchar)
 LANGUAGE plpgsql
AS $function$
BEGIN
RETURN query (
SELECT
    sum(cf.value) AS value, currency
FROM
    (
    SELECT
        cash_flow.value,
        currency
    FROM
        cash_flow
    WHERE
        category_id_to = _category_id
        AND users_id IN (SELECT get_users_id(_user_id))
UNION ALL
    SELECT
        -cash_flow.value,
        currency
    FROM
        cash_flow
    WHERE
        category_id_from = _category_id
        AND users_id IN (SELECT get_users_id(_user_id))
    ) cf
GROUP BY currency);
END
$function$;

-- 6) exchange: hierarchy USD -> stablecoins -> other currencies
CREATE OR REPLACE FUNCTION public.exchange(
    _users_id bigint,
    _category_id int,
    _value_out numeric,
    _currency_out character VARYING,
    _value_in numeric,
    _currency_in character varying
)
RETURNS text
LANGUAGE plpgsql
AS $function$
declare
    _rate_out numeric;
    _rate_in numeric;
    _rate_out_current numeric;
    _rate_in_current numeric;
    _rate_out_text text;
    _rate_in_text text;
    _stable_currencies text[] := array[
        'USDT','USDC','DAI','BUSD','TUSD','USDP','GUSD','USDN','FRAX','USDD','FDUSD','USDE','SUSD','PYUSD'
    ];
    _is_stable_out boolean;
    _is_stable_in boolean;
    _ts timestamp := now();
begin
    if _value_out <= 0 or _value_in <= 0 then
        raise exception 'Exchange values must be greater than zero';
    end if;

    select rate into _rate_out
    from exchange_rates
    where currency = _currency_out
    order by datetime desc
    limit 1;

    select rate into _rate_in
    from exchange_rates
    where currency = _currency_in
    order by datetime desc
    limit 1;

    if _currency_out = 'USD' then
        _rate_out := 1;
    end if;
    if _currency_in = 'USD' then
        _rate_in := 1;
    end if;

    _is_stable_out := _currency_out = ANY(_stable_currencies);
    _is_stable_in := _currency_in = ANY(_stable_currencies);

    if _rate_out is null and _rate_in is null then
        raise exception 'Rates for % and % are unknown. Exchange via USD first', _currency_out, _currency_in;
    end if;

    -- USD is anchor: update the other currency
    if _currency_out = 'USD' then
        _rate_out := 1;
        _rate_in := _rate_out * (_value_in / _value_out);
        insert into exchange_rates(datetime, currency, rate)
        values(_ts, _currency_in, _rate_in);

    elsif _currency_in = 'USD' then
        _rate_in := 1;
        _rate_out := _rate_in * (_value_out / _value_in);
        insert into exchange_rates(datetime, currency, rate)
        values(_ts, _currency_out, _rate_out);

    -- if receiving stablecoin -> update non-stable side
    elsif _is_stable_in then
        if _currency_out = 'USD' then
            -- USD is anchor and unchanged
            null;
        else
            if _rate_in is null then
                raise exception 'Stablecoin rate is unknown. Exchange stablecoin with USD first';
            end if;
            _rate_out := _rate_in * (_value_out / _value_in);
            insert into exchange_rates(datetime, currency, rate)
            values(_ts, _currency_out, _rate_out);
        end if;

    -- if paying by stablecoin -> update received non-stable currency
    elsif _is_stable_out then
        if _rate_out is null then
            raise exception 'Stablecoin rate is unknown. Exchange stablecoin with USD first';
        end if;
        _rate_in := _rate_out * (_value_in / _value_out);
        insert into exchange_rates(datetime, currency, rate)
        values(_ts, _currency_in, _rate_in);

    -- no USD/stables: update received currency by paid currency
    else
        if _rate_out is null then
            raise exception 'Rate for % is unknown. Exchange via USD or stablecoin first', _currency_out;
        end if;
        _rate_in := _rate_out * (_value_in / _value_out);
        insert into exchange_rates(datetime, currency, rate)
        values(_ts, _currency_in, _rate_in);
    end if;

    insert into cash_flow(users_id, category_id_from, value, currency, description)
    values(_users_id, _category_id, _value_out, _currency_out, concat('exchange to ', _value_in, ' ', _currency_in));

    insert into cash_flow(users_id, category_id_to, value, currency, description)
    values(_users_id, _category_id, _value_in, _currency_in, concat('exchange from ', _value_out, ' ', _currency_out));

    if _currency_out = 'USD' then
        _rate_out_current := 1;
    else
        _rate_out_current := coalesce(_rate_out, (select rate from exchange_rates where currency = _currency_out order by datetime desc limit 1));
    end if;

    if _currency_in = 'USD' then
        _rate_in_current := 1;
    else
        _rate_in_current := coalesce(_rate_in, (select rate from exchange_rates where currency = _currency_in order by datetime desc limit 1));
    end if;

    _rate_out_text := case
        when abs(_rate_out_current) >= 1 then replace(to_char(_rate_out_current, 'FM999,999,999,999,999,999,990.00'), ',', ' ')
        when _rate_out_current::text like '%.%' then rtrim(trim(trailing '0' from _rate_out_current::text), '.')
        else _rate_out_current::text
    end;

    _rate_in_text := case
        when abs(_rate_in_current) >= 1 then replace(to_char(_rate_in_current, 'FM999,999,999,999,999,999,990.00'), ',', ' ')
        when _rate_in_current::text like '%.%' then rtrim(trim(trailing '0' from _rate_in_current::text), '.')
        else _rate_in_current::text
    end;

    return format('Курс: %s=%s, %s=%s (за 1 USD)',
                  _currency_out, _rate_out_text,
                  _currency_in, _rate_in_text);
end
$function$;

COMMIT;

-- ============================================================
-- Verification block (run manually, read-only + safe checks)
-- ============================================================

-- A) Check signatures
-- SELECT pg_get_function_result('public.get_category_balance_with_currency(bigint, integer)'::regprocedure);
-- SELECT pg_get_function_result('public.exchange(bigint, integer, numeric, character varying, numeric, character varying)'::regprocedure);

-- B) Check USD anchor exists
-- SELECT currency, rate, datetime
-- FROM exchange_rates
-- WHERE currency = 'USD'
-- ORDER BY datetime DESC
-- LIMIT 1;

-- C) Check formatting logic (no data mutation)
-- SELECT
--   CASE
--     WHEN ABS(v) >= 1 THEN REPLACE(TO_CHAR(v, 'FM999,999,999,999,999,999,990.00'), ',', ' ')
--     WHEN v::text LIKE '%.%' THEN RTRIM(TRIM(TRAILING '0' FROM v::text), '.')
--     ELSE v::text
--   END AS formatted
-- FROM (VALUES (12345.678::numeric), (0.0001234500::numeric), (1.5::numeric)) t(v);

-- D) Exchange smoke tests (run in transaction and ROLLBACK)
-- BEGIN;
-- SELECT public.exchange(<USER_ID>, <CATEGORY_ID>, 100::numeric, 'USD', 99.1::numeric, 'USDT');
-- SELECT public.exchange(<USER_ID>, <CATEGORY_ID>, 1000::numeric, 'USDT', 0.031::numeric, 'ETH');
-- SELECT public.exchange(<USER_ID>, <CATEGORY_ID>, 100000::numeric, 'RUB', 1000::numeric, 'USDT');
-- ROLLBACK;
