-- Recreate strategy: drop old signatures first so signature changes are applied cleanly.
DROP FUNCTION IF EXISTS public.monthly();
DROP FUNCTION IF EXISTS public.monthly_distribute(bigint, integer);
DROP FUNCTION IF EXISTS public.distribute_to_group(bigint, integer, integer, numeric, varchar);
DROP FUNCTION IF EXISTS public.transact_from_group_to_category(bigint, integer, integer);
DROP FUNCTION IF EXISTS public.exchange(bigint, integer, numeric, varchar, numeric, varchar);
DROP FUNCTION IF EXISTS public.exchange_v2(bigint, integer, numeric, varchar, numeric, varchar);
DROP FUNCTION IF EXISTS public.insert_spend_with_exchange(bigint, varchar, numeric, varchar, text);
DROP FUNCTION IF EXISTS public.insert_spend_with_exchange_v2(bigint, varchar, numeric, varchar, text);
DROP FUNCTION IF EXISTS public.insert_revenue(bigint, varchar, numeric, varchar, text);
DROP FUNCTION IF EXISTS public.insert_revenue_v2(bigint, varchar, numeric, varchar, text);
DROP FUNCTION IF EXISTS public.insert_spend(bigint, varchar, numeric, varchar, text);
DROP FUNCTION IF EXISTS public.insert_spend_v2(bigint, varchar, numeric, varchar, text);
DROP FUNCTION IF EXISTS public.insert_in_cash_flow(bigint, timestamp, integer, integer, integer, varchar, text);
DROP FUNCTION IF EXISTS public.get_daily_transactions(bigint);
DROP FUNCTION IF EXISTS public.get_daily_allocation_transactions(bigint);
DROP FUNCTION IF EXISTS public.get_last_transaction(bigint, integer);
DROP FUNCTION IF EXISTS public.get_last_transaction_v2(bigint, integer);
DROP FUNCTION IF EXISTS public.get_last_allocation_postings(bigint, integer);
DROP FUNCTION IF EXISTS public.delete_transaction(bigint[]);
DROP FUNCTION IF EXISTS public.get_group_balance(bigint, integer);
DROP FUNCTION IF EXISTS public.get_group_balance_v2(bigint, integer);
DROP FUNCTION IF EXISTS public.get_all_balances(bigint, integer);
DROP FUNCTION IF EXISTS public.get_all_balances_v2(bigint, integer);
DROP FUNCTION IF EXISTS public.get_remains(bigint, character);
DROP FUNCTION IF EXISTS public.get_remains_v2(bigint, character);
DROP FUNCTION IF EXISTS public.get_category_balance_with_currency(bigint, integer);
DROP FUNCTION IF EXISTS public.get_category_balance_with_currency_v2(bigint, integer);
DROP FUNCTION IF EXISTS public.get_category_balance(bigint, integer, varchar);
DROP FUNCTION IF EXISTS public.get_category_balance_v2(bigint, integer, varchar);
DROP FUNCTION IF EXISTS public.get_allocation_node_balance(bigint, bigint, varchar);
DROP FUNCTION IF EXISTS public.get_allocation_node_balance_by_slug(bigint, text, varchar);
DROP FUNCTION IF EXISTS public.ensure_allocation_compatibility_node(bigint, integer);
DROP FUNCTION IF EXISTS public.mirror_cash_flow_row_to_allocation_postings(bigint, text, text, text, jsonb);
DROP FUNCTION IF EXISTS public.get_categories_name(bigint, integer);
DROP FUNCTION IF EXISTS public.get_categories_name_v2(bigint, integer);
DROP FUNCTION IF EXISTS public.get_category_id_from_name(varchar);
DROP FUNCTION IF EXISTS public.get_category_id_from_name_v2(bigint, varchar);
DROP FUNCTION IF EXISTS public.find_allocation_category_node_id_by_name(bigint, varchar);
DROP FUNCTION IF EXISTS public.get_categories_id(bigint, integer);
DROP FUNCTION IF EXISTS public.get_currency();
DROP FUNCTION IF EXISTS public.get_currency_v2();
DROP FUNCTION IF EXISTS public.get_all_users_id();
DROP FUNCTION IF EXISTS public.is_technical_cashflow_description(text);
DROP FUNCTION IF EXISTS public.get_users_id(bigint);
DROP FUNCTION IF EXISTS public.find_allocation_node_id(bigint, text);
DROP FUNCTION IF EXISTS public.find_allocation_remainder_legacy_category_id(bigint, text);
DROP FUNCTION IF EXISTS public.get_group_percent_sum(bigint, integer);
DROP FUNCTION IF EXISTS public.distribute_with_allocation_fallback(bigint, text, integer, integer, numeric, varchar, text);
DROP FUNCTION IF EXISTS public.transact_group_to_allocation_fallback(bigint, integer, text, integer, varchar, text);
DROP FUNCTION IF EXISTS public.reserve_negative_personal_expenses_to_allocation_fallback(bigint, text, text, integer, numeric, varchar, text);
DROP FUNCTION IF EXISTS public.insert_monthly_compat_investition_second(bigint, numeric, varchar, text);
DROP FUNCTION IF EXISTS public.validate_allocation_routes(bigint);
DROP FUNCTION IF EXISTS public.allocation_distribute_recursive(bigint, bigint, numeric, varchar, integer, text, bigint[]);
DROP FUNCTION IF EXISTS public.allocation_distribute(bigint, bigint, numeric, varchar, integer, text);
DROP FUNCTION IF EXISTS public.monthly_distribute_allocation(bigint, bigint, integer, varchar, text);
DROP FUNCTION IF EXISTS public.monthly_allocation_report_metrics(bigint, bigint, jsonb);
DROP FUNCTION IF EXISTS public.monthly_distribute_cascade(bigint, integer);

-- принимает user_id и возвращает id всех пользьвателей из группы  
create or replace function get_users_id(_user_id bigint)
 returns table (user_id bigint)
 language plpgsql
as $function$
begin
return query (select ug2.users_id from users_groups ug1 join users_groups ug2 using(users_groups) where ug1.users_id = _user_id);
		end
$function$;


-- LEGACY cash_flow-backed category balance helper.
-- App read-paths use get_category_balance_v2(...); keep this for reference/compare/rollback.
CREATE OR REPLACE FUNCTION public.get_category_balance(
    _user_id bigint,
    _category_id integer,
    _currency CHARACTER VARYING DEFAULT 'RUB'::CHARACTER VARYING
) RETURNS NUMERIC
LANGUAGE plpgsql
AS $function$
DECLARE
    result NUMERIC;
BEGIN
    WITH _exchange_rates AS (
        SELECT DISTINCT ON (currency)
            currency,
            rate
        FROM
            exchange_rates
        ORDER BY
            currency, datetime DESC
    ),
    cash_flow_data AS (
        SELECT
            CASE
                WHEN category_id_to = _category_id THEN value
                ELSE -value
            END AS value,
            currency
        FROM
            cash_flow
        WHERE
            (_category_id IN (category_id_to, category_id_from))
            AND users_id IN (SELECT get_users_id(_user_id))
    )
    SELECT
        SUM(cf.value / (src_rate.rate / target_rate.rate))
    INTO result
    FROM
        cash_flow_data cf
    JOIN _exchange_rates src_rate
        ON src_rate.currency = cf.currency
    JOIN _exchange_rates target_rate
        ON target_rate.currency = _currency;

    RETURN result;
END;
$function$;


-- Ledger-backed category balance helper.
-- Mirrors get_category_balance(...) semantics while reading allocation_postings.
CREATE OR REPLACE FUNCTION public.get_category_balance_v2(
    _user_id bigint,
    _category_id integer,
    _currency CHARACTER VARYING DEFAULT 'RUB'::CHARACTER VARYING
) RETURNS NUMERIC
LANGUAGE plpgsql
AS $function$
DECLARE
    result NUMERIC;
BEGIN
    WITH _exchange_rates AS (
        SELECT DISTINCT ON (currency)
            currency,
            rate
        FROM public.exchange_rates
        ORDER BY currency, datetime DESC
    ),
    posting_data AS (
        SELECT
            CASE
                WHEN to_node.legacy_category_id = _category_id THEN ap.value
                ELSE -ap.value
            END AS value,
            ap.currency
        FROM public.allocation_postings ap
        LEFT JOIN public.allocation_nodes from_node
          ON from_node.id = ap.from_node_id
        LEFT JOIN public.allocation_nodes to_node
          ON to_node.id = ap.to_node_id
        WHERE ap.user_id IN (SELECT get_users_id(_user_id))
          AND _category_id IN (
              from_node.legacy_category_id,
              to_node.legacy_category_id
          )
    )
    SELECT
        SUM(p.value / (src_rate.rate / target_rate.rate))
    INTO result
    FROM posting_data p
    JOIN _exchange_rates src_rate
      ON src_rate.currency = p.currency
    JOIN _exchange_rates target_rate
      ON target_rate.currency = _currency;

    RETURN result;
END;
$function$;


-- Возвращает баланс allocation-ноды по новому graph-native ledger.
-- Используется как read-helper во время перехода с cash_flow на allocation_postings.
CREATE OR REPLACE FUNCTION public.get_allocation_node_balance(
    _user_id bigint,
    _node_id bigint,
    _currency CHARACTER VARYING DEFAULT 'RUB'::CHARACTER VARYING
) RETURNS NUMERIC
LANGUAGE plpgsql
AS $function$
DECLARE
    result NUMERIC;
BEGIN
    WITH _exchange_rates AS (
        SELECT DISTINCT ON (currency)
            currency,
            rate
        FROM exchange_rates
        ORDER BY currency, datetime DESC
    ),
    posting_data AS (
        SELECT
            CASE
                WHEN to_node_id = _node_id THEN value
                ELSE -value
            END AS value,
            currency
        FROM public.allocation_postings ap
        WHERE (_node_id IN (ap.to_node_id, ap.from_node_id))
          AND ap.user_id IN (SELECT get_users_id(_user_id))
    )
    SELECT
        SUM(p.value / (src_rate.rate / target_rate.rate))
    INTO result
    FROM posting_data p
    JOIN _exchange_rates src_rate
      ON src_rate.currency = p.currency
    JOIN _exchange_rates target_rate
      ON target_rate.currency = _currency;

    RETURN result;
END;
$function$;


-- Возвращает баланс allocation-ноды по slug.
-- Сначала ищет user-owned ноду, затем group-owned ноду среди активных membership пользователя.
CREATE OR REPLACE FUNCTION public.get_allocation_node_balance_by_slug(
    _user_id bigint,
    _slug text,
    _currency CHARACTER VARYING DEFAULT 'RUB'::CHARACTER VARYING
) RETURNS NUMERIC
LANGUAGE sql
STABLE
AS $function$
    WITH target_node AS (
        SELECT an.id
        FROM public.allocation_nodes an
        WHERE an.active
          AND an.slug = _slug
          AND (
              an.user_id = _user_id
              OR an.user_group_id IN (
                  SELECT ugm.user_group_id
                  FROM public.user_group_memberships ugm
                  WHERE ugm.user_id = _user_id
                    AND ugm.active
              )
          )
        ORDER BY
            CASE WHEN an.user_id IS NOT NULL THEN 0 ELSE 1 END,
            an.id
        LIMIT 1
    )
    SELECT public.get_allocation_node_balance(_user_id, id, _currency)
    FROM target_node;
$function$;


-- Ensures a user-owned compatibility node exists for a legacy category so
-- legacy cash_flow can always be mirrored/backfilled into allocation_postings.
CREATE OR REPLACE FUNCTION public.ensure_allocation_compatibility_node(
    _user_id bigint,
    _legacy_category_id integer
) RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
    _node_id bigint;
    _category_name text;
BEGIN
    IF _legacy_category_id IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT an.id
    INTO _node_id
    FROM public.allocation_nodes an
    WHERE an.active
      AND an.user_id = _user_id
      AND an.legacy_category_id = _legacy_category_id
    ORDER BY an.id
    LIMIT 1;

    IF _node_id IS NOT NULL THEN
        RETURN _node_id;
    END IF;

    SELECT c."name"
    INTO _category_name
    FROM public.categories c
    WHERE c.id = _legacy_category_id;

    IF _category_name IS NULL THEN
        RETURN NULL;
    END IF;

    INSERT INTO public.allocation_nodes (
        user_id,
        slug,
        "name",
        description,
        node_kind,
        legacy_category_id,
        visible,
        include_in_report,
        active
    )
    VALUES (
        _user_id,
        CONCAT('legacy_bridge_cat_', _legacy_category_id),
        _category_name,
        CONCAT('Compatibility bridge for legacy category ', _legacy_category_id),
        'both',
        _legacy_category_id,
        false,
        false,
        true
    )
    ON CONFLICT (user_id, slug) WHERE user_id IS NOT NULL
    DO UPDATE SET
        legacy_category_id = EXCLUDED.legacy_category_id,
        active = true
    RETURNING id INTO _node_id;

    RETURN _node_id;
END;
$function$;


-- Mirror one legacy cash_flow row into allocation_postings when matching allocation nodes exist.
-- Used by manual runtime write-paths while cash_flow remains the compatibility ledger.
CREATE OR REPLACE FUNCTION public.mirror_cash_flow_row_to_allocation_postings(
    _cash_flow_id bigint,
    _kind text,
    _subkind text,
    _origin text,
    _extra_metadata jsonb DEFAULT '{}'::jsonb
) RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
    _cf public.cash_flow%ROWTYPE;
    _from_node_id bigint;
    _to_node_id bigint;
BEGIN
    SELECT *
    INTO _cf
    FROM public.cash_flow
    WHERE id = _cash_flow_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'cash_flow row % not found', _cash_flow_id;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM public.allocation_postings ap
        WHERE ap.metadata->>'legacy_cash_flow_id' = _cash_flow_id::text
    ) THEN
        RETURN;
    END IF;

    IF COALESCE(_cf.value, 0) <= 0 THEN
        RETURN;
    END IF;

    SELECT an.id
    INTO _from_node_id
    FROM public.allocation_nodes an
    WHERE an.active
      AND an.legacy_category_id = _cf.category_id_from
      AND (
          an.user_id = _cf.users_id
          OR an.user_group_id IN (
              SELECT ugm.user_group_id
              FROM public.user_group_memberships ugm
              WHERE ugm.user_id = _cf.users_id
                AND ugm.active
          )
      )
    ORDER BY
        CASE WHEN an.user_id = _cf.users_id THEN 0 ELSE 1 END,
        an.id
    LIMIT 1;

    IF _from_node_id IS NULL THEN
        _from_node_id := public.ensure_allocation_compatibility_node(
            _cf.users_id,
            _cf.category_id_from
        );
    END IF;

    SELECT an.id
    INTO _to_node_id
    FROM public.allocation_nodes an
    WHERE an.active
      AND an.legacy_category_id = _cf.category_id_to
      AND (
          an.user_id = _cf.users_id
          OR an.user_group_id IN (
              SELECT ugm.user_group_id
              FROM public.user_group_memberships ugm
              WHERE ugm.user_id = _cf.users_id
                AND ugm.active
          )
      )
    ORDER BY
        CASE WHEN an.user_id = _cf.users_id THEN 0 ELSE 1 END,
        an.id
    LIMIT 1;

    IF _to_node_id IS NULL THEN
        _to_node_id := public.ensure_allocation_compatibility_node(
            _cf.users_id,
            _cf.category_id_to
        );
    END IF;

    IF _from_node_id IS NULL AND _to_node_id IS NULL THEN
        RETURN;
    END IF;

    INSERT INTO public.allocation_postings(
        "datetime",
        user_id,
        from_node_id,
        to_node_id,
        value,
        currency,
        description,
        metadata
    )
    VALUES (
        _cf.datetime,
        _cf.users_id,
        _from_node_id,
        _to_node_id,
        _cf.value,
        _cf.currency,
        _cf.description,
        jsonb_strip_nulls(
            jsonb_build_object(
                'kind', _kind,
                'subkind', _subkind,
                'origin', _origin,
                'legacy_cash_flow_id', _cf.id,
                'legacy_category_id_from', _cf.category_id_from,
                'legacy_category_id_to', _cf.category_id_to
            ) || COALESCE(_extra_metadata, '{}'::jsonb)
        )
    );
END;
$function$;


-- Возвращает id активной allocation-ноды пользователя по slug.
-- Используется только в переходной monthly-логике:
-- по slug ищем новую root-ноду, если она уже собрана для конкретной ветки.
CREATE OR REPLACE FUNCTION public.find_allocation_node_id(_user_id bigint, _slug text)
 RETURNS bigint
 LANGUAGE sql
 STABLE
AS $function$
    SELECT an.id
    FROM public.allocation_nodes an
    WHERE an.user_id = _user_id
      AND an.slug = _slug
      AND an.active
    ORDER BY an.id
    LIMIT 1;
$function$;


-- Возвращает legacy_category_id remainder-leaf ноды, подключенной к source slug пользователя.
-- Используется в переходной monthly-логике, чтобы брать free-category уже из allocation-графа.
CREATE OR REPLACE FUNCTION public.find_allocation_remainder_legacy_category_id(_user_id bigint, _source_slug text)
 RETURNS integer
 LANGUAGE sql
 STABLE
AS $function$
    SELECT target.legacy_category_id
    FROM public.allocation_nodes source
    JOIN public.allocation_routes ar
      ON ar.source_node_id = source.id
     AND ar.active
     AND ar.percent = 1
    JOIN public.allocation_nodes target
      ON target.id = ar.target_node_id
    WHERE source.user_id = _user_id
      AND source.slug = _source_slug
      AND source.active
      AND target.active
    ORDER BY ar.id
    LIMIT 1;
$function$;


-- Сумма legacy-процентов внутри старой category_group.
-- Нужна только на переходном этапе, чтобы остаток считался так же,
-- как раньше в distribute_to_group(), даже если сами проводки уже пишет новый каскад.
CREATE OR REPLACE FUNCTION public.get_group_percent_sum(_user_id bigint, _group_id integer)
 RETURNS numeric
 LANGUAGE sql
 STABLE
AS $function$
    SELECT COALESCE(SUM(c.percent), 0)
    FROM public.categories c
    JOIN public.categories_category_groups ccg
      ON ccg.categories_id = c.id
    WHERE ccg.users_id = _user_id
      AND ccg.category_groyps_id = _group_id;
$function$;


-- Legacy compatibility: some local/test mappings historically counted
-- investition_second into the caller's investment leaf as well.
-- Keep this narrow and mapping-driven so normal users are not affected.
CREATE OR REPLACE FUNCTION public.insert_monthly_compat_investition_second(
    _user_id bigint,
    _amount numeric,
    _currency varchar DEFAULT 'RUB',
    _description text DEFAULT 'monthly distribute'
)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    _investment_category_id integer;
BEGIN
    IF _amount IS NULL OR _amount <= 0 THEN
        RETURN;
    END IF;

    _investment_category_id := (
        SELECT get_categories_id(_user_id, 1)
    );

    IF _investment_category_id IS NULL THEN
        RETURN;
    END IF;

    -- Only apply the compatibility posting when the investment leaf is also
    -- attached to legacy group 15. This matches the known dirty local mapping
    -- and avoids changing the normal production path.
    IF NOT EXISTS (
        SELECT 1
        FROM public.categories_category_groups ccg
        WHERE ccg.users_id = _user_id
          AND ccg.categories_id = _investment_category_id
          AND ccg.category_groyps_id = 15
    ) THEN
        RETURN;
    END IF;

    INSERT INTO public.cash_flow(
        users_id,
        category_id_from,
        category_id_to,
        value,
        currency,
        description
    )
    VALUES (
        _user_id,
        15,
        _investment_category_id,
        _amount,
        _currency,
        COALESCE(_description, 'monthly distribute')
    );
END;
$function$;


-- принимает user_id, группу категорий по которым распределить и id категории откуда поступили деньги, распределяет деньги по указанной группе и cумму для распределения, возвращает остаток  
create or replace function distribute_to_group(
    _user_id bigint, 
    _group_id int, 
    _income_category_id int, 
    _income_value numeric, 
    _currency varchar default 'RUB'
) returns numeric
language plpgsql
as $function$
declare 
    _reminder numeric;
    _total_percent numeric;
begin
    -- Проверка входных параметров
    if _income_value <= 0 then
        return 0;
    end if;

    -- Проверка существования группы для пользователя
    if not exists (
        select 1 
        from categories_category_groups 
        where category_groyps_id = _group_id and users_id = _user_id
    ) then
        raise exception 'Group ID % does not exist for user %', _group_id, _user_id;
    end if;

    -- Суммарный процент по группе для последующего расчета остатка
    select coalesce(sum("percent"), 0)
      into _total_percent
    from categories c
    join categories_category_groups ccg on c.id = ccg.categories_id
    where ccg.category_groyps_id = _group_id and users_id = _user_id;

    -- Вставка распределения
    insert into cash_flow (users_id, category_id_from, category_id_to, value, currency, description)
    select ccg.users_id, _income_category_id, c.id, _income_value * c."percent", _currency, 'monthly distribute'
    from categories c
    join categories_category_groups ccg on c.id = ccg.categories_id
    where ccg.category_groyps_id = _group_id and users_id = _user_id and _income_value > 0;

    -- Расчет остатка
    _reminder := _income_value * (1 - _total_percent);
    raise notice 'Distributed % to group % for user %', _income_value, _group_id, _user_id;

    return _reminder;

exception
    when others then
        raise notice 'Error occurred while distributing income for user %: %', _user_id, sqlerrm;
        return null;
end
$function$;

-- Определяет внутренние тех.операции, которые не должны попадать в month_earnings/month_spend.
-- Поддерживает как текущие префиксы, так и явный флаг в description: "internal:"
CREATE OR REPLACE FUNCTION public.is_technical_cashflow_description(_description text)
 RETURNS boolean
 LANGUAGE sql
 IMMUTABLE
AS $function$
SELECT CASE
    WHEN _description IS NULL THEN false
    ELSE lower(_description) LIKE ANY (ARRAY[
        'exchange %',
        'auto exchange %',
        'monthly distribute%',
        'internal:%'
    ])
END;
$function$;


-- Переходная версия monthly_distribute():
-- повторяет legacy monthly_distribute() по расчетам и JSON,
-- но постепенно переводит подготовительные шаги и ветки на allocation-граф.
-- Важно:
-- 1) функция должна оставаться эквивалентной legacy monthly_distribute();
-- 2) менять её нужно по одной ветке и после каждого изменения прогонять compare SQL;
-- 3) legacy monthly_distribute() сохраняется ниже как reference/rollback и не должна вызываться из public.monthly().
CREATE OR REPLACE FUNCTION public.monthly_distribute_cascade(_user_id bigint, _income_category integer)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    _sum_value numeric(10,2);
    _free_money numeric(10,2);
    _second_member_id bigint;
    _sum_earnings numeric;
    _sum_spend numeric;
    _monthly_income_root_id bigint;
    _extra_income_root_id bigint;
    _free_to_gifts_root_id bigint;
    _free_category_id integer;
    _reserve_root_id bigint;
    _salary_primary_root_id bigint;
    _source record;
    _balance numeric;
    _reserve_amount numeric;
    _report_rows jsonb := '[]'::jsonb;
    _branch_report jsonb := '[]'::jsonb;
    _report_metrics jsonb := '{}'::jsonb;
BEGIN
    -- Шаг 1. Allocation-only подготовка monthly incomes (group 11 -> monthly_income_sources).
    _monthly_income_root_id := public.find_allocation_node_id(_user_id, 'monthly_income_sources');

    IF _monthly_income_root_id IS NULL THEN
        RAISE EXCEPTION
            'monthly_income_sources allocation root is required for user %',
            _user_id;
    END IF;

    FOR _source IN
        SELECT
            ccg.categories_id AS category_id_from,
            public.get_category_balance(_user_id, ccg.categories_id, 'RUB') AS balance
        FROM public.categories_category_groups ccg
        WHERE ccg.users_id = _user_id
          AND ccg.category_groyps_id = 11
        ORDER BY ccg.categories_id
    LOOP
        IF COALESCE(_source.balance, 0) <= 0 THEN
            CONTINUE;
        END IF;

        PERFORM public.allocation_distribute(
            _user_id::bigint,
            _monthly_income_root_id::bigint,
            _source.balance::numeric,
            'RUB'::varchar,
            _source.category_id_from::integer,
            'monthly distribute'::text
        );
    END LOOP;

    -- Шаг 2. Allocation-only подготовка extra incomes (group 12 -> extra_income_sources).
    _extra_income_root_id := public.find_allocation_node_id(_user_id, 'extra_income_sources');

    IF _extra_income_root_id IS NULL THEN
        RAISE EXCEPTION
            'extra_income_sources allocation root is required for user %',
            _user_id;
    END IF;

    FOR _source IN
        SELECT
            ccg.categories_id AS category_id_from,
            public.get_category_balance(_user_id, ccg.categories_id, 'RUB') AS balance
        FROM public.categories_category_groups ccg
        WHERE ccg.users_id = _user_id
          AND ccg.category_groyps_id = 12
        ORDER BY ccg.categories_id
    LOOP
        IF COALESCE(_source.balance, 0) <= 0 THEN
            CONTINUE;
        END IF;

        PERFORM public.allocation_distribute(
            _user_id::bigint,
            _extra_income_root_id::bigint,
            _source.balance::numeric,
            'RUB'::varchar,
            _source.category_id_from::integer,
            'monthly distribute'::text
        );
    END LOOP;

    _free_category_id := public.find_allocation_remainder_legacy_category_id(_user_id, 'self_distribution');

    IF _free_category_id IS NULL THEN
        RAISE EXCEPTION
            'self_distribution remainder leaf with legacy_category_id is required for user %',
            _user_id;
    END IF;

    _free_money := public.get_category_balance(_user_id, _free_category_id, 'RUB');

    -- Шаг 2.5. Allocation-only перевод free money в gifts bucket.
    _free_to_gifts_root_id := public.find_allocation_node_id(_user_id, 'free_to_gifts');

    IF _free_to_gifts_root_id IS NULL THEN
        RAISE EXCEPTION
            'free_to_gifts allocation root is required for user %',
            _user_id;
    END IF;

    IF COALESCE(_free_money, 0) > 0 THEN
        PERFORM public.allocation_distribute(
            _user_id::bigint,
            _free_to_gifts_root_id::bigint,
            _free_money::numeric,
            'RUB'::varchar,
            _free_category_id,
            'monthly distribute'::text
        );
    END IF;

    -- Шаг 3. Allocation-only reserve для отрицательных personal-spend категорий.
    _reserve_root_id := public.find_allocation_node_id(_user_id, 'debt_reserve');

    IF _reserve_root_id IS NULL THEN
        RAISE EXCEPTION
            'debt_reserve allocation root is required for user %',
            _user_id;
    END IF;

    FOR _source IN
        WITH canonical_reserve_sources AS (
            SELECT v.user_id, v.category_id
            FROM (
                VALUES
                    (249716305::bigint, 2::integer),
                    (249716305::bigint, 8::integer),
                    (249716305::bigint, 9::integer),
                    (249716305::bigint, 11::integer),
                    (943915310::bigint, 17::integer),
                    (943915310::bigint, 18::integer),
                    (943915310::bigint, 20::integer),
                    (943915310::bigint, 21::integer),
                    (943915310::bigint, 26::integer)
            ) AS v(user_id, category_id)
        )
        SELECT category_id
        FROM canonical_reserve_sources
        WHERE user_id = _user_id

        UNION

        SELECT DISTINCT spend_ccg.categories_id AS category_id
        FROM public.categories_category_groups spend_ccg
        JOIN public.categories_category_groups personal_ccg
          ON personal_ccg.users_id = spend_ccg.users_id
         AND personal_ccg.categories_id = spend_ccg.categories_id
        WHERE spend_ccg.users_id = _user_id
          AND _user_id NOT IN (249716305, 943915310)
          AND spend_ccg.category_groyps_id = 8
          AND personal_ccg.category_groyps_id = 15
        ORDER BY category_id
    LOOP
        _balance := public.get_category_balance(_user_id, _source.category_id, 'RUB');

        IF COALESCE(_balance, 0) >= 0 THEN
            CONTINUE;
        END IF;

        _reserve_amount := ABS(_balance) * 0.01;

        IF _reserve_amount <= 0 THEN
            CONTINUE;
        END IF;

        PERFORM public.allocation_distribute(
            _user_id::bigint,
            _reserve_root_id::bigint,
            _reserve_amount::numeric,
            'RUB'::varchar,
            _source.category_id::integer,
            'monthly distribute'::text
        );
    END LOOP;

    _sum_value := (
        SELECT get_category_balance(_user_id, _income_category, 'RUB')
    );

    _salary_primary_root_id := public.find_allocation_node_id(_user_id, 'salary_primary');

    IF _salary_primary_root_id IS NULL THEN
        RAISE EXCEPTION
            'salary_primary allocation root is required for user %',
            _user_id;
    END IF;

    IF _sum_value > 0 THEN
        WITH distributed AS (
            SELECT *
            FROM public.allocation_distribute(
                _user_id::bigint,
                _salary_primary_root_id::bigint,
                _sum_value::numeric,
                'RUB'::varchar,
                _income_category::integer,
                'monthly distribute'::text
            )
        )
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'owner_user_id', owner_user_id,
                    'owner_user_group_id', owner_user_group_id,
                    'node_id', report_node_id,
                    'slug', report_node_slug,
                    'name', report_node_name,
                    'amount', report_amount
                )
                ORDER BY
                    owner_user_id NULLS LAST,
                    owner_user_group_id NULLS LAST,
                    report_node_name
            ),
            '[]'::jsonb
        )
        INTO _branch_report
        FROM distributed;

        _report_rows := _report_rows || COALESCE(_branch_report, '[]'::jsonb);
    END IF;

    _second_member_id := (
        SELECT user_id
        FROM get_users_id(_user_id)
        WHERE user_id != _user_id
    );

    -- Шаг 3. Итоговые отчётные суммы теперь собираются из report rows.
    -- Важно: основной каскад запускается ровно один раз от salary_primary.
    -- Дальнейшая логика "инвестиции -> семейный взнос -> остаток -> partner split -> leafs"
    -- должна быть выражена самим allocation-графом, без дополнительных ручных вызовов
    -- salary_secondary/family_split/free_pool из orchestration-функции.
    _report_metrics := public.monthly_allocation_report_metrics(
        _user_id,
        _second_member_id,
        _report_rows
    );

    _sum_earnings := (
        SELECT COALESCE(SUM(value), 0)
        FROM public.allocation_postings
        WHERE user_id = _user_id
          AND from_node_id IS NULL
          AND NOT public.is_technical_cashflow_description(description)
          AND date_trunc('month', datetime) = date_trunc('month', now()) - INTERVAL '1 month'
    );

    _sum_spend := (
        SELECT COALESCE(SUM(value), 0)
        FROM public.allocation_postings
        WHERE user_id = _user_id
          AND to_node_id IS NULL
          AND NOT public.is_technical_cashflow_description(description)
          AND date_trunc('month', datetime) = date_trunc('month', now()) - INTERVAL '1 month'
    );

    RETURN jsonb_build_object(
        'user_id', _user_id,
        'общие_категории', COALESCE((_report_metrics ->> 'общие_категории')::numeric, 0),
        'second_user_id', _second_member_id,
        'семейный_взнос', COALESCE((_report_metrics ->> 'семейный_взнос')::numeric, 0),
        'second_user_pay', COALESCE((_report_metrics ->> 'second_user_pay')::numeric, 0),
        'investition', COALESCE((_report_metrics ->> 'investition')::numeric, 0),
        'investition_second', COALESCE((_report_metrics ->> 'investition_second')::numeric, 0),
        'month_earnings', _sum_earnings,
        'month_spend', _sum_spend
    );
END;
$function$;


-- Проверяет, что у исходной ноды не более одного маршрута "остаток"
-- и что сумма процентных маршрутов не превышает 100%.
-- percent = 1 трактуется как remainder route, а не как "перевести 100%".
CREATE OR REPLACE FUNCTION public.validate_allocation_routes(_source_node_id bigint)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    _remainder_count integer;
    _percent_sum numeric;
BEGIN
    SELECT
        COUNT(*) FILTER (WHERE percent = 1),
        COALESCE(SUM(percent) FILTER (WHERE percent < 1), 0)
    INTO
        _remainder_count,
        _percent_sum
    FROM public.allocation_routes
    WHERE source_node_id = _source_node_id
      AND active;

    IF _remainder_count > 1 THEN
        RAISE EXCEPTION
            'Allocation node % has % remainder routes; expected at most one',
            _source_node_id,
            _remainder_count;
    END IF;

    IF _percent_sum > 1 THEN
        RAISE EXCEPTION
            'Allocation node % has percent sum % > 1',
            _source_node_id,
            _percent_sum;
    END IF;
END;
$function$;


-- Рекурсивно распределяет сумму по allocation_routes.
-- Пишет реальные проводки в allocation_postings только на листьях.
-- На переходном этапе mixed-node запрещена:
-- нода либо промежуточная и имеет детей, либо лист и пишет в ledger.
CREATE OR REPLACE FUNCTION public.allocation_distribute_recursive(
    _executor_user_id bigint,
    _source_node_id bigint,
    _amount numeric,
    _currency varchar DEFAULT 'RUB',
    _category_id_from integer DEFAULT NULL,
    _description text DEFAULT 'allocation cascade',
    _path bigint[] DEFAULT ARRAY[]::bigint[]
)
 RETURNS TABLE(
    owner_user_id bigint,
    owner_user_group_id bigint,
    report_node_id bigint,
    report_node_slug varchar,
    report_node_name varchar,
    report_amount numeric
 )
 LANGUAGE plpgsql
AS $function$
DECLARE
    _node public.allocation_nodes%ROWTYPE;
    _target_node public.allocation_nodes%ROWTYPE;
    _route public.allocation_routes%ROWTYPE;
    _route_count integer;
    _remaining numeric;
    _child_amount numeric;
    _posting_user_id bigint;
    _posting_from_node_id bigint;
    _next_executor_user_id bigint;
    _next_category_id_from integer;
BEGIN
    IF _amount IS NULL OR _amount <= 0 THEN
        RETURN;
    END IF;

    IF _source_node_id = ANY(_path) THEN
        RAISE EXCEPTION 'Cycle detected in allocation graph at node %', _source_node_id;
    END IF;

    SELECT *
    INTO _node
    FROM public.allocation_nodes
    WHERE id = _source_node_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Allocation node % not found', _source_node_id;
    END IF;

    IF NOT _node.active THEN
        RAISE EXCEPTION 'Allocation node % (%) is inactive', _node.id, _node.slug;
    END IF;

    SELECT COUNT(*)
    INTO _route_count
    FROM public.allocation_routes
    WHERE source_node_id = _source_node_id
      AND active;

    -- Явно запрещаем ноду, которая одновременно является и маршрутизатором, и leaf-точкой записи.
    IF _route_count > 0 AND _node.legacy_category_id IS NOT NULL THEN
        RAISE EXCEPTION
            'Allocation node % (%) has both outgoing routes and legacy_category_id; mixed nodes are not supported',
            _node.id,
            _node.slug;
    END IF;

    -- Report-сумма считается по входящей сумме в ноду.
    -- Это позволяет включать в отчет как промежуточные report-ноды, так и конечные листья.
    IF _node.include_in_report THEN
        owner_user_id := _node.user_id;
        owner_user_group_id := _node.user_group_id;
        report_node_id := _node.id;
        report_node_slug := _node.slug;
        report_node_name := _node.name;
        report_amount := _amount;
        RETURN NEXT;
    END IF;

    -- Лист определяется отсутствием исходящих активных маршрутов.
    IF _route_count = 0 THEN
        _posting_user_id := COALESCE(_node.user_id, _executor_user_id);
        _posting_from_node_id := CASE
            WHEN COALESCE(array_length(_path, 1), 0) > 0
                THEN _path[array_length(_path, 1)]
            ELSE NULL
        END;

        INSERT INTO public.allocation_postings(
            user_id,
            from_node_id,
            to_node_id,
            value,
            currency,
            description,
            metadata
        )
        VALUES (
            _posting_user_id,
            _posting_from_node_id,
            _node.id,
            _amount,
            _currency,
            COALESCE(_description, 'allocation cascade'),
            jsonb_strip_nulls(
                jsonb_build_object(
                    'kind', 'monthly',
                    'subkind', 'leaf_posting',
                    'origin', 'allocation_runtime',
                    'legacy_category_id_from', _category_id_from,
                    'legacy_category_id_to', _node.legacy_category_id,
                    'leaf_slug', _node.slug
                )
            )
        );

        RETURN;
    END IF;

    PERFORM public.validate_allocation_routes(_source_node_id);

    _remaining := _amount;

    FOR _route IN
        SELECT *
        FROM public.allocation_routes
        WHERE source_node_id = _source_node_id
          AND active
        ORDER BY
            CASE WHEN percent = 1 THEN 1 ELSE 0 END,
            percent,
            id
    LOOP
        -- Маршрут с percent = 1 забирает весь остаток после процентных веток.
        IF _route.percent = 1 THEN
            _child_amount := _remaining;
        ELSE
            _child_amount := _amount * _route.percent;
            _remaining := _remaining - _child_amount;
        END IF;

        IF _child_amount IS NULL OR _child_amount <= 0 THEN
            CONTINUE;
        END IF;

        SELECT *
        INTO _target_node
        FROM public.allocation_nodes
        WHERE id = _route.target_node_id;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'Allocation target node % not found', _route.target_node_id;
        END IF;

        -- При переходе в partner-ветку меняем "владельца" downstream-проводок на целевого пользователя.
        -- Это важно для shared/group-owned leaf-nodes: они должны писаться от имени текущей ветки,
        -- а не всегда от исходного executor_user_id.
        _next_executor_user_id := COALESCE(_target_node.user_id, _executor_user_id);

        -- В old monthly_distribute() partner-ветка всегда шла из family contribution category 15.
        -- Сохраняем legacy source-category в metadata при входе в family_contribution_in.
        IF _target_node.slug = 'family_contribution_in' THEN
            _next_category_id_from := 15;
        ELSE
            _next_category_id_from := _category_id_from;
        END IF;

        RETURN QUERY
        SELECT *
        FROM public.allocation_distribute_recursive(
            _next_executor_user_id,
            _route.target_node_id,
            _child_amount,
            _currency,
            _next_category_id_from,
            _description,
            _path || _source_node_id
        );
    END LOOP;
END;
$function$;


-- Публичная функция распределения:
-- 1) проверяет доступ исполнителя к исходной ноде;
-- 2) запускает рекурсивное распределение;
-- 3) агрегирует строки отчета по нодам.
-- Это low-level entrypoint нового движка.
-- Месячная логика выше должна использовать именно его, а не писать напрямую в cash_flow.
CREATE OR REPLACE FUNCTION public.allocation_distribute(
    _executor_user_id bigint,
    _source_node_id bigint,
    _amount numeric,
    _currency varchar DEFAULT 'RUB',
    _category_id_from integer DEFAULT NULL,
    _description text DEFAULT 'allocation cascade'
)
 RETURNS TABLE(
    owner_user_id bigint,
    owner_user_group_id bigint,
    report_node_id bigint,
    report_node_slug varchar,
    report_node_name varchar,
    report_amount numeric
 )
 LANGUAGE plpgsql
AS $function$
DECLARE
    _source_node public.allocation_nodes%ROWTYPE;
BEGIN
    IF _amount IS NULL OR _amount <= 0 THEN
        RAISE EXCEPTION 'Allocation amount must be greater than zero';
    END IF;

    SELECT *
    INTO _source_node
    FROM public.allocation_nodes
    WHERE id = _source_node_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Allocation source node % not found', _source_node_id;
    END IF;

    IF NOT _source_node.active THEN
        RAISE EXCEPTION 'Allocation source node % (%) is inactive', _source_node.id, _source_node.slug;
    END IF;

    IF _source_node.user_id IS NOT NULL AND _source_node.user_id <> _executor_user_id THEN
        RAISE EXCEPTION
            'Executor user % cannot start distribution from node % owned by user %',
            _executor_user_id,
            _source_node.id,
            _source_node.user_id;
    END IF;

    IF _source_node.user_group_id IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM public.user_group_memberships ugm
           WHERE ugm.user_id = _executor_user_id
             AND ugm.user_group_id = _source_node.user_group_id
             AND ugm.active
       ) THEN
        RAISE EXCEPTION
            'Executor user % is not an active member of group % for source node %',
            _executor_user_id,
            _source_node.user_group_id,
            _source_node.id;
    END IF;

    RETURN QUERY
    SELECT
        distributed.owner_user_id,
        distributed.owner_user_group_id,
        distributed.report_node_id,
        distributed.report_node_slug,
        distributed.report_node_name,
        SUM(distributed.report_amount) AS report_amount
    FROM public.allocation_distribute_recursive(
        _executor_user_id,
        _source_node_id,
        _amount,
        _currency,
        _category_id_from,
        _description,
        ARRAY[]::bigint[]
    ) distributed
    GROUP BY
        distributed.owner_user_id,
        distributed.owner_user_group_id,
        distributed.report_node_id,
        distributed.report_node_slug,
        distributed.report_node_name
    ORDER BY
        distributed.owner_user_id NULLS LAST,
        distributed.owner_user_group_id NULLS LAST,
        distributed.report_node_name;
END;
$function$;


-- Внутренний allocation entrypoint для monthly cascade.
-- Legacy monthly_distribute() сохраняется отдельно как reference/rollback.
CREATE OR REPLACE FUNCTION public.monthly_distribute_allocation(
    _executor_user_id bigint,
    _source_node_id bigint,
    _category_id_from integer,
    _currency varchar DEFAULT 'RUB',
    _description text DEFAULT 'monthly distribute allocation'
)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    _source_amount numeric;
    _sum_earnings numeric;
    _sum_spend numeric;
    _report jsonb := '[]'::jsonb;
BEGIN
    IF _category_id_from IS NULL THEN
        RAISE EXCEPTION 'monthly_distribute_allocation requires source category_id_from';
    END IF;

    _source_amount := COALESCE(
        public.get_category_balance_v2(_executor_user_id, _category_id_from, _currency),
        0
    );

    _sum_earnings := (
        SELECT COALESCE(SUM(value), 0)
        FROM public.allocation_postings
        WHERE user_id = _executor_user_id
          AND from_node_id IS NULL
          AND NOT public.is_technical_cashflow_description(description)
          AND date_trunc('month', datetime) = date_trunc('month', now()) - INTERVAL '1 month'
    );

    _sum_spend := (
        SELECT COALESCE(SUM(value), 0)
        FROM public.allocation_postings
        WHERE user_id = _executor_user_id
          AND to_node_id IS NULL
          AND NOT public.is_technical_cashflow_description(description)
          AND date_trunc('month', datetime) = date_trunc('month', now()) - INTERVAL '1 month'
    );

    IF _source_amount > 0 THEN
        WITH distributed AS (
            SELECT *
            FROM public.allocation_distribute(
                _executor_user_id,
                _source_node_id,
                _source_amount,
                _currency,
                _category_id_from,
                _description
            )
        )
        SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'owner_user_id', owner_user_id,
                    'owner_user_group_id', owner_user_group_id,
                    'node_id', report_node_id,
                    'slug', report_node_slug,
                    'name', report_node_name,
                    'amount', report_amount
                )
                ORDER BY
                    owner_user_id NULLS LAST,
                    owner_user_group_id NULLS LAST,
                    report_node_name
            ),
            '[]'::jsonb
        )
        INTO _report
        FROM distributed;
    END IF;

    RETURN jsonb_build_object(
        'user_id', _executor_user_id,
        'source_node_id', _source_node_id,
        'source_category_id', _category_id_from,
        'source_amount', _source_amount,
        'currency', _currency,
        'month_earnings', COALESCE(_sum_earnings, 0),
        'month_spend', COALESCE(_sum_spend, 0),
        'report', _report
    );
END;
$function$;


-- Агрегирует report rows месячного каскада в итоговый JSON monthly-метрик.
-- Для Telegram-отчёта сохраняем split-контракт старой monthly_distribute():
-- 1) `общие_категории` — доля текущего пользователя;
-- 2) `second_user_pay` — доля партнёра, которая потом будет досуммирована в Python-агрегации;
-- 3) `investition_second` — инвестиции партнёра из семейного взноса.
-- Пока граф не несёт отдельный report-node для partner-common, второй кусок общих категорий
-- восстанавливаем как остаток partner-ветки:
-- family_contribution - invest_partner - все partner-owned leaf-суммы.
CREATE OR REPLACE FUNCTION public.monthly_allocation_report_metrics(
    _user_id bigint,
    _second_user_id bigint,
    _report_rows jsonb
)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    _family_contribution numeric := 0;
    _common_total numeric := 0;
    _invest_self numeric := 0;
    _invest_partner numeric := 0;
    _partner_leaf_total numeric := 0;
    _partner_common numeric := 0;
    _self_common numeric := 0;
BEGIN
    WITH rows AS (
        SELECT
            NULLIF(item ->> 'owner_user_id', '')::bigint AS owner_user_id,
            NULLIF(item ->> 'owner_user_group_id', '')::bigint AS owner_user_group_id,
            item ->> 'slug' AS slug,
            COALESCE((item ->> 'amount')::numeric, 0) AS amount
        FROM jsonb_array_elements(COALESCE(_report_rows, '[]'::jsonb)) AS item
    ),
    common_slugs AS (
        SELECT DISTINCT an.slug
        FROM public.allocation_nodes an
        WHERE an.active
          AND an.user_group_id IS NOT NULL
          AND an.slug ~ '^cat_[0-9]+$'
    ),
    second_invest_slugs AS (
        SELECT DISTINCT target.slug
        FROM public.allocation_nodes source
        JOIN public.allocation_routes ar
          ON ar.source_node_id = source.id
         AND ar.active
        JOIN public.allocation_nodes target
          ON target.id = ar.target_node_id
        WHERE source.user_id = _second_user_id
          AND source.active
          AND source.slug IN ('invest_self_report', 'invest_partner_report')
          AND target.active
          AND target.slug ~ '^cat_[0-9]+$'
    )
    SELECT
        COALESCE(SUM(amount) FILTER (
            WHERE owner_user_id = _user_id
              AND slug IN ('family_contribution_out', 'family_contribution_report')
        ), 0),
        COALESCE(SUM(amount) FILTER (
            WHERE owner_user_group_id IS NOT NULL
              AND slug IN (SELECT slug FROM common_slugs)
        ), 0),
        COALESCE(SUM(amount) FILTER (
            WHERE owner_user_id = _user_id
              AND slug = 'invest_self_report'
        ), 0),
        COALESCE(SUM(amount) FILTER (
            WHERE owner_user_id = _second_user_id
              AND slug IN ('invest_partner_report', 'invest_partner_incoming_report')
        ), 0),
        COALESCE(SUM(amount) FILTER (
            WHERE owner_user_id = _second_user_id
              AND slug ~ '^cat_[0-9]+$'
              AND slug NOT IN (SELECT slug FROM common_slugs)
              AND slug NOT IN (SELECT slug FROM second_invest_slugs)
        ), 0)
    INTO
        _family_contribution,
        _common_total,
        _invest_self,
        _invest_partner,
        _partner_leaf_total
    FROM rows;

    _partner_common := LEAST(
        _common_total,
        GREATEST(_family_contribution - _invest_partner - _partner_leaf_total, 0)
    );
    _self_common := GREATEST(_common_total - _partner_common, 0);

    RETURN jsonb_build_object(
        'семейный_взнос', _family_contribution,
        'общие_категории', _self_common,
        'second_user_pay', _partner_common,
        'investition', _invest_self,
        'investition_second', _invest_partner
    );
END;
$function$;


-- LEGACY: старая monthly-функция.
-- Сохраняется в базе только как reference/rollback для compare и аварийного отката.
-- Новый public.monthly() её больше не вызывает.
-- принимает id пользователя и id категории прихода и распределяет по всем категориям								
CREATE OR REPLACE FUNCTION public.monthly_distribute(_user_id bigint, _income_category integer)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    _sum_value numeric(10,2);
    _sum_value_second numeric(10,2);
    _value_for_second_member numeric;
    _free_money numeric(10,2);
    _second_member_id bigint;
    _second_member_free_money numeric;
    _general_categories numeric;
    _general_categories_second numeric;
    _sum_earnings NUMERIC;
    _sum_spend NUMERIC;
BEGIN
    PERFORM transact_from_group_to_category(_user_id, 11, (SELECT get_categories_id(_user_id, 13)));    -- Переводим месячные доходы в одну категорию
    PERFORM transact_from_group_to_category(_user_id, 12, (SELECT get_categories_id(_user_id, 7)));    -- Переводим другие доходы в категорию "подарки себе"
    _free_money := (SELECT get_category_balance(_user_id, (SELECT get_categories_id(_user_id, 6)), 'RUB'));    -- Получение остатка свободных денег
    PERFORM distribute_to_group(_user_id, 7, (SELECT get_categories_id(_user_id, 6)), _free_money, 'RUB');    -- Перевод части остатка на подарки себе
    INSERT INTO cash_flow (users_id, category_id_from, category_id_to, value, currency, description)    -- Увеличение резерва на 1% за счет должников
    SELECT _user_id, id, 
           (SELECT get_categories_id(_user_id, 9)), 
           ABS("sum") * 0.01, 'RUB', 'monthly distribute'
    FROM (
        SELECT c.id, get_category_balance(_user_id, c.id, 'RUB') AS "sum"
        FROM categories c
        JOIN categories_category_groups ccg ON c.id = ccg.categories_id
        WHERE ccg.users_id = _user_id AND ccg.category_groyps_id = 8
    ) debts
    WHERE "sum" < 0;
    _sum_value := (SELECT get_category_balance(_user_id, _income_category, 'RUB'));    -- Сумма дохода за месяц
    _value_for_second_member := _sum_value * (SELECT "percent" FROM categories WHERE id = 15);    -- Расчет семейного взноса
    _sum_value_second := distribute_to_group(_user_id, 1, _income_category, _sum_value, 'RUB');    -- Перевод денег на НЗ 
    _free_money := distribute_to_group(_user_id, 2, _income_category, _sum_value - _value_for_second_member, 'RUB');   -- Распределение свободных денег
    _second_member_id := (SELECT user_id FROM get_users_id(_user_id) WHERE user_id != _user_id);    -- Получение ID второго пользователя
    _second_member_free_money := distribute_to_group(_second_member_id, 3, 15, _value_for_second_member, 'RUB');    -- Распределение денег для второго пользователя
    PERFORM distribute_to_group(_user_id, 6, _income_category, _free_money - _sum_value * 0.1, 'RUB');    -- Внесение свободных денег в резерв
    PERFORM distribute_to_group(_second_member_id, 6, 15, _second_member_free_money, 'RUB');   -- Внесение свободных денег в резерв второго пользователя
    _general_categories := (SELECT SUM((_sum_value - _value_for_second_member) * c."percent")    -- Подсчет общих категорий
                            FROM categories c
                            JOIN categories_category_groups ccg ON c.id = ccg.categories_id
                            WHERE ccg.category_groyps_id = 4);

    _general_categories_second := (SELECT SUM(_value_for_second_member * c."percent")
                                   FROM categories c
                                   JOIN categories_category_groups ccg ON c.id = ccg.categories_id
                                   WHERE ccg.category_groyps_id = 4);
    _sum_earnings := (SELECT COALESCE(SUM(value), 0)   -- Подсчет доходов за месяц
                      FROM cash_flow
                      WHERE users_id = _user_id
                      AND category_id_from IS NULL
                      AND NOT public.is_technical_cashflow_description(description)
                      AND date_trunc('month', datetime) = date_trunc('month', now()) - INTERVAL '1 month');
	_sum_spend := (SELECT COALESCE(SUM(value), 0)  -- Подсчет расходов за месяц
                   FROM cash_flow
                   WHERE users_id = _user_id
                   AND category_id_to IS NULL
                   AND NOT public.is_technical_cashflow_description(description)
                   AND date_trunc('month', datetime) = date_trunc('month', now()) - INTERVAL '1 month');
    RETURN jsonb_build_object(   -- Возвращение результата
        'user_id', _user_id,
        'общие_категории', _general_categories,
        'second_user_id', _second_member_id,
        'семейный_взнос', _value_for_second_member,
        'second_user_pay', _general_categories_second,
        'investition', _sum_value * 0.1,
        'investition_second', _value_for_second_member * 0.1,
        'month_earnings', _sum_earnings,
        'month_spend', _sum_spend
    );
END;
$function$;

-- принимает user_id, id группы и id категории и переводит все деньги с группы на категорию 
create or replace function transact_from_group_to_category(_user_id bigint, _group_id int, _category_id int)
 returns text
 language plpgsql
as $function$
begin
	insert into cash_flow (users_id, category_id_from, category_id_to, value, currency, description)
	select users_id, categories_id, _category_id, balance, 'RUB', 'monthly distribute'  
	from 
		(select users_id, categories_id, get_category_balance(_user_id, categories_id) as balance 
		 from categories_category_groups ccg 
		 where users_id = _user_id and category_groyps_id = _group_id) sub 
	where balance > 0;

return 'OK';
		end
$function$;



-- принимает user_id и id группы и возвращает id всех категорий группы
CREATE OR REPLACE FUNCTION public.get_categories_id(_user_id bigint, _groyps_id integer)
 RETURNS TABLE(categories_id integer)
 LANGUAGE plpgsql
AS $function$
begin
return query (select ccg.categories_id from categories_category_groups ccg where ccg.users_id = _user_id and ccg.category_groyps_id = _groyps_id);
		end
$function$
;



-- LEGACY cash_flow-backed history helper.
-- App read-paths use get_last_transaction_v2(...); keep this for reference/compare/rollback.
CREATE OR REPLACE FUNCTION get_last_transaction(_user_id bigint, _num int)
RETURNS TABLE (
    id bigint,
    datetime timestamp,
    "from" varchar(100),
    "to" varchar(100),
    value varchar,  -- Изменён тип на varchar для представления форматированного значения
    currency varchar(16),
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
            CAST(
                CASE
                    WHEN ABS(cf.value) >= 1 THEN REPLACE(TO_CHAR(cf.value, 'FM999,999,999,999,999,999,990.00'), ',', ' ')
                    WHEN cf.value::text LIKE '%.%' THEN RTRIM(TRIM(TRAILING '0' FROM cf.value::text), '.')
                    ELSE cf.value::text
                END
            AS varchar) AS value,
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
            FROM 
                cash_flow cf_sub 
            WHERE 
                users_id = _user_id
        ) cf 
        LEFT JOIN categories c ON cf.category_id_from = c.id 
        LEFT JOIN categories c2 ON cf.category_id_to = c2.id 
        WHERE 
            "rank" = _num
    );
END;
$function$;


-- Ledger-backed candidate for /history. Kept separate until delete flow is migrated.
CREATE OR REPLACE FUNCTION public.get_last_transaction_v2(_user_id bigint, _num int)
RETURNS TABLE (
    id bigint,
    datetime timestamp,
    "from" varchar(100),
    "to" varchar(100),
    value varchar,
    currency varchar(16),
    description text
)
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY (
        SELECT
            ap.id,
            ap.datetime,
            src."name" AS "from",
            dst."name" AS "to",
            CAST(
                CASE
                    WHEN ABS(ap.value) >= 1 THEN REPLACE(TO_CHAR(ap.value, 'FM999,999,999,999,999,999,990.00'), ',', ' ')
                    WHEN ap.value::text LIKE '%.%' THEN RTRIM(TRIM(TRAILING '0' FROM ap.value::text), '.')
                    ELSE ap.value::text
                END
            AS varchar) AS value,
            ap.currency,
            ap.description
        FROM (
            SELECT
                ap_sub.*,
                dense_rank() OVER (ORDER BY ap_sub.datetime DESC) AS "rank"
            FROM public.allocation_postings ap_sub
            WHERE ap_sub.user_id = _user_id
        ) ap
        LEFT JOIN public.allocation_nodes src
          ON src.id = ap.from_node_id
        LEFT JOIN public.allocation_nodes dst
          ON dst.id = ap.to_node_id
        WHERE ap."rank" = _num
        ORDER BY ap.id
    );
END;
$function$;


-- Read-only helper for observing the new allocation ledger without switching /history yet.
CREATE OR REPLACE FUNCTION public.get_last_allocation_postings(_user_id bigint, _num int)
RETURNS TABLE (
    id bigint,
    datetime timestamp,
    "from" varchar(100),
    "to" varchar(100),
    value varchar,
    currency varchar(16),
    description text,
    kind text,
    subkind text,
    origin text
)
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY (
        SELECT
            ap.id,
            ap.datetime,
            src."name" AS "from",
            dst."name" AS "to",
            CAST(
                CASE
                    WHEN ABS(ap.value) >= 1 THEN REPLACE(TO_CHAR(ap.value, 'FM999,999,999,999,999,999,990.00'), ',', ' ')
                    WHEN ap.value::text LIKE '%.%' THEN RTRIM(TRIM(TRAILING '0' FROM ap.value::text), '.')
                    ELSE ap.value::text
                END
            AS varchar) AS value,
            ap.currency,
            ap.description,
            ap.metadata->>'kind' AS kind,
            ap.metadata->>'subkind' AS subkind,
            ap.metadata->>'origin' AS origin
        FROM (
            SELECT
                ap_sub.*,
                dense_rank() OVER (ORDER BY ap_sub.datetime DESC) AS "rank"
            FROM public.allocation_postings ap_sub
            WHERE ap_sub.user_id = _user_id
        ) ap
        LEFT JOIN public.allocation_nodes src
          ON src.id = ap.from_node_id
        LEFT JOIN public.allocation_nodes dst
          ON dst.id = ap.to_node_id
        WHERE ap."rank" = _num
        ORDER BY ap.id
    );
END;
$function$;

-- Принимает поля транзакции и записывает в таблицу cash_flow, обязательным полем является users_id
CREATE OR REPLACE FUNCTION public.insert_in_cash_flow(_users_id bigint,
													  _datetime timestamp default now(),
													  _category_id_from integer default null, 
													  _category_id_to integer default null,
													  _value integer default 0, 
													  _currency varchar default 'RUB',
													  _description text default null)
RETURNS text
LANGUAGE plpgsql
AS $function$
declare
    _cash_flow_id bigint;
begin 
	insert into cash_flow(users_id, datetime, category_id_from, category_id_to, value, currency, description)
		   values(_users_id, _datetime, _category_id_from, _category_id_to, _value, _currency, _description)
        returning id into _cash_flow_id;

    perform public.mirror_cash_flow_row_to_allocation_postings(
        _cash_flow_id,
        'cash_flow_insert',
        'generic',
        'app'
    );
    return 'OK';
end
$function$
;


-- LEGACY cash_flow-primary spend write helper.
-- App write-paths use insert_spend_v2(...); keep this for reference/compare/rollback.
CREATE OR REPLACE FUNCTION public.insert_spend(_users_id bigint, _category_name_from character varying, _value numeric DEFAULT 0, _currency character varying DEFAULT 'RUB'::character varying, _description text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
    _cash_flow_id bigint;
begin 
	insert into cash_flow (users_id, category_id_from, value, currency, description) 
                select _users_id, c.id, _value, _currency as currency, _description
                from categories c
                join categories_category_groups ccg on c.id = ccg.categories_id
                where ccg.category_groyps_id = 14 and ccg.users_id = _users_id
                and c."name"=_category_name_from
        returning id into _cash_flow_id;

    perform public.mirror_cash_flow_row_to_allocation_postings(
        _cash_flow_id,
        'transaction',
        'spend',
        'app'
    );
    return 'OK';
end
$function$
;

-- Allocation-primary spend write helper.
-- Runtime writes only allocation_postings; legacy cash_flow stays as historical/backfill source.
CREATE OR REPLACE FUNCTION public.insert_spend_v2(_users_id bigint, _category_name_from character varying, _value numeric DEFAULT 0, _currency character varying DEFAULT 'RUB'::character varying, _description text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    _node_id bigint;
    _legacy_category_id integer;
BEGIN
    IF _value <= 0 THEN
        RAISE EXCEPTION 'Spend value must be greater than zero';
    END IF;

    _node_id := public.find_allocation_category_node_id_by_name(_users_id, _category_name_from);

    IF _node_id IS NULL THEN
        RAISE EXCEPTION 'Allocation category node not found for user %, category %', _users_id, _category_name_from;
    END IF;

    SELECT legacy_category_id
    INTO _legacy_category_id
    FROM public.allocation_nodes
    WHERE id = _node_id;

    INSERT INTO public.allocation_postings (
        user_id,
        from_node_id,
        to_node_id,
        value,
        currency,
        description,
        metadata
    )
    VALUES (
        _users_id,
        _node_id,
        NULL,
        _value,
        _currency,
        _description,
        jsonb_strip_nulls(
            jsonb_build_object(
                'kind', 'transaction',
                'subkind', 'spend',
                'origin', 'app',
                'legacy_category_id_from', _legacy_category_id
            )
        )
    );

    RETURN 'OK';
END
$function$
;

-- LEGACY cash_flow-primary revenue write helper.
-- App write-paths use insert_revenue_v2(...); keep this for reference/compare/rollback.
CREATE OR REPLACE FUNCTION public.insert_revenue(_users_id bigint, _category_to character varying, _value numeric DEFAULT 0, _currency character varying DEFAULT 'RUB'::character varying, _description text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
    _cash_flow_id bigint;
begin 
	insert into cash_flow (users_id, category_id_to, value, currency, description) 
                select _users_id, c.id, _value, _currency, _description
                from categories c
                join categories_category_groups ccg on c.id = ccg.categories_id
                where ccg.category_groyps_id = 14 and ccg.users_id = _users_id
                and c."name"=_category_to
        returning id into _cash_flow_id;

    perform public.mirror_cash_flow_row_to_allocation_postings(
        _cash_flow_id,
        'transaction',
        'revenue',
        'app'
    );
    return 'OK';
end
$function$
;

-- Allocation-primary revenue write helper.
-- Runtime writes only allocation_postings; legacy cash_flow stays as historical/backfill source.
CREATE OR REPLACE FUNCTION public.insert_revenue_v2(_users_id bigint, _category_to character varying, _value numeric DEFAULT 0, _currency character varying DEFAULT 'RUB'::character varying, _description text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    _node_id bigint;
    _legacy_category_id integer;
BEGIN
    IF _value <= 0 THEN
        RAISE EXCEPTION 'Revenue value must be greater than zero';
    END IF;

    _node_id := public.find_allocation_category_node_id_by_name(_users_id, _category_to);

    IF _node_id IS NULL THEN
        RAISE EXCEPTION 'Allocation category node not found for user %, category %', _users_id, _category_to;
    END IF;

    SELECT legacy_category_id
    INTO _legacy_category_id
    FROM public.allocation_nodes
    WHERE id = _node_id;

    INSERT INTO public.allocation_postings (
        user_id,
        from_node_id,
        to_node_id,
        value,
        currency,
        description,
        metadata
    )
    VALUES (
        _users_id,
        NULL,
        _node_id,
        _value,
        _currency,
        _description,
        jsonb_strip_nulls(
            jsonb_build_object(
                'kind', 'transaction',
                'subkind', 'revenue',
                'origin', 'app',
                'legacy_category_id_to', _legacy_category_id
            )
        )
    );

    RETURN 'OK';
END
$function$
;



-- LEGACY categories_category_groups-backed category lookup.
-- App read-paths use get_categories_name_v2(...); keep this for reference/compare/rollback.
CREATE OR REPLACE FUNCTION public.get_categories_name(_user_id bigint, _groyps_id integer)
 RETURNS TABLE("name" varchar)
 LANGUAGE plpgsql
AS $function$
begin
return query (select c."name" from public.categories c where c.id in (select public.get_categories_id(_user_id, _groyps_id)));
		end
$function$
;

-- Allocation-backed category lookup for UI category lists.
CREATE OR REPLACE FUNCTION public.get_categories_name_v2(_user_id bigint, _groyps_id integer)
 RETURNS TABLE("name" varchar)
 LANGUAGE sql
 STABLE
AS $function$
    SELECT DISTINCT an."name"
    FROM public.allocation_nodes an
    JOIN public.allocation_node_groups ang
      ON ang.node_id = an.id
     AND ang.active
    WHERE an.active
      AND an.legacy_category_id IS NOT NULL
      AND ang.legacy_group_id = _groyps_id
      AND (
          an.user_id = _user_id
          OR an.user_group_id IN (
              SELECT ugm.user_group_id
              FROM public.user_group_memberships ugm
              WHERE ugm.user_id = _user_id
                AND ugm.active
          )
      )
    ORDER BY an."name";
$function$
;


-- Ledger-aware delete helper used by /history.
-- Input ids are allocation_postings.id values. If a ledger row mirrors legacy cash_flow,
-- the linked cash_flow row is deleted as well to prevent future backfill resurrection.
CREATE OR REPLACE FUNCTION public.delete_transaction(_transactions_id bigint[])
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    _legacy_cash_flow_ids bigint[];
BEGIN
    SELECT COALESCE(
        ARRAY_AGG((ap.metadata->>'legacy_cash_flow_id')::bigint)
            FILTER (
                WHERE ap.metadata ? 'legacy_cash_flow_id'
                  AND ap.metadata->>'legacy_cash_flow_id' ~ '^[0-9]+$'
            ),
        ARRAY[]::bigint[]
    )
    INTO _legacy_cash_flow_ids
    FROM public.allocation_postings ap
    WHERE ap.id = ANY(_transactions_id);

    DELETE FROM public.allocation_postings
    WHERE id = ANY(_transactions_id);

    IF COALESCE(array_length(_legacy_cash_flow_ids, 1), 0) > 0 THEN
        DELETE FROM public.cash_flow
        WHERE id = ANY(_legacy_cash_flow_ids);
    END IF;

    RETURN 'OK';
END
$function$
;

-- принимает id пользователя и возвращает все операции за сегодня
CREATE OR REPLACE FUNCTION public.get_daily_transactions(_user_id bigint)
RETURNS TABLE(transact text)
LANGUAGE sql
AS $function$
SELECT CONCAT_WS(' ',
    src."name",
    COALESCE(dst."name", '-'),
    CASE
        WHEN ABS(ap.value) >= 1 THEN REPLACE(TO_CHAR(ap.value, 'FM999,999,999,999,999,999,990.00'), ',', ' ')
        WHEN ap.value::text LIKE '%.%' THEN RTRIM(TRIM(TRAILING '0' FROM ap.value::text), '.')
        ELSE ap.value::text
    END,
    ap.currency
) AS transact
FROM public.allocation_postings ap
LEFT JOIN public.allocation_nodes src ON ap.from_node_id = src.id
LEFT JOIN public.allocation_nodes dst ON ap.to_node_id = dst.id
WHERE date_trunc('day', ap.datetime) = date_trunc('day', now())
  AND ap.user_id = _user_id
ORDER BY ap.datetime;
$function$;


-- Read-only daily report helper backed by allocation_postings.
-- Kept as an explicit ledger-read alias after get_daily_transactions() switched to allocation_postings.
CREATE OR REPLACE FUNCTION public.get_daily_allocation_transactions(_user_id bigint)
RETURNS TABLE(transact text)
LANGUAGE sql
AS $function$
SELECT *
FROM public.get_daily_transactions(_user_id);
$function$;

-- возвращает id всех пользователей
CREATE OR REPLACE FUNCTION public.get_all_users_id()
 RETURNS TABLE(id bigint)
 LANGUAGE plpgsql
AS $function$
begin
return query (SELECT u.id FROM users u );
	end
$function$
;	

-- LEGACY cash_flow-backed group balance helper.
-- App read-paths use get_group_balance_v2(...); keep this for reference/compare/rollback.
CREATE OR REPLACE FUNCTION public.get_group_balance(_user_id bigint, _groyps_id integer)
 RETURNS TABLE(balance NUMERIC)
 LANGUAGE plpgsql
AS $function$
begin
return query (SELECT 
				sum(get_category_balance) AS balance
			  FROM (SELECT  
						get_category_balance(_user_id, get_categories_id(_user_id, _groyps_id))) sub);
end
$function$
;

-- Ledger-backed candidate for get_group_balance(...).
CREATE OR REPLACE FUNCTION public.get_group_balance_v2(_user_id bigint, _groyps_id integer)
 RETURNS TABLE(balance NUMERIC)
 LANGUAGE sql
AS $function$
    SELECT SUM(public.get_category_balance_v2(_user_id, c.categories_id, 'RUB')) AS balance
    FROM public.get_categories_id(_user_id, _groyps_id) c;
$function$
;

-- LEGACY cash_flow-backed category remains helper.
-- App read-paths use get_remains_v2(...); keep this for reference/compare/rollback.
CREATE OR REPLACE FUNCTION public.get_remains(_user_id bigint, _category CHARACTER)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
begin
return (select COALESCE (get_category_balance(_user_id,(select c.id from categories c join categories_category_groups ccg on c.id = ccg.categories_id
                    where ccg.category_groyps_id = 14 and ccg.users_id = _user_id
                    and c."name"=_category)), 0))
			  ;
		end
$function$
;   

-- Ledger-backed candidate for get_remains(...).
CREATE OR REPLACE FUNCTION public.get_remains_v2(_user_id bigint, _category CHARACTER)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN (
        SELECT COALESCE(
            public.get_category_balance_v2(
                _user_id,
                (
                    SELECT c.id
                    FROM public.categories c
                    JOIN public.categories_category_groups ccg
                      ON c.id = ccg.categories_id
                    WHERE ccg.category_groyps_id = 14
                      AND ccg.users_id = _user_id
                      AND c."name" = _category
                ),
                'RUB'
            ),
            0
        )
    );
END;
$function$
;

-- LEGACY cash_flow-backed all balances helper.
-- App read-paths use get_all_balances_v2(...); keep this for reference/compare/rollback.
CREATE OR REPLACE FUNCTION public.get_all_balances(_user_id bigint, _group_id integer)
RETURNS TABLE(category_name varchar, balance numeric(20, 2))
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT 
        c."name" AS category_name,
        COALESCE(get_category_balance(_user_id, c.id, 'RUB'), 0) AS balance
    FROM 
        public.categories c
    WHERE 
        c.id IN (SELECT public.get_categories_id(_user_id, _group_id));
END;
$function$;

-- Ledger-backed candidate for get_all_balances(...).
CREATE OR REPLACE FUNCTION public.get_all_balances_v2(_user_id bigint, _group_id integer)
RETURNS TABLE(category_name varchar, balance numeric(20, 2))
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        c."name" AS category_name,
        COALESCE(public.get_category_balance_v2(_user_id, c.id, 'RUB'), 0)::numeric(20, 2) AS balance
    FROM public.categories c
    WHERE c.id IN (SELECT public.get_categories_id(_user_id, _group_id));
END;
$function$;

-- запускает функции месячного распределения
CREATE OR REPLACE FUNCTION public.monthly()
 RETURNS TABLE (get_remains jsonb)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
        SELECT public.monthly_distribute_cascade(943915310, 37)
        UNION ALL
        SELECT public.monthly_distribute_cascade(249716305, 16);
end
$function$
;  

-- LEGACY cash_flow-primary manual exchange.
-- App write-paths use exchange_v2(...); keep this for reference/compare/rollback.
CREATE OR REPLACE FUNCTION public.exchange(_users_id bigint, _category_id int, _value_out numeric, _currency_out character VARYING, _value_in numeric, _currency_in character varying)
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
    _cash_flow_id_out bigint;
    _cash_flow_id_in bigint;
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
    -- Stablecoin updates only when exchanged with USD
    elsif _is_stable_out then
        if _rate_out is null then
            raise exception 'Stablecoin rate is unknown. Exchange stablecoin with USD first';
        end if;
        _rate_in := _rate_out * (_value_in / _value_out);
        insert into exchange_rates(datetime, currency, rate)
        values(_ts, _currency_in, _rate_in);
    elsif _is_stable_in then
        if _rate_in is null then
            raise exception 'Stablecoin rate is unknown. Exchange stablecoin with USD first';
        end if;
        _rate_out := _rate_in * (_value_out / _value_in);
        insert into exchange_rates(datetime, currency, rate)
        values(_ts, _currency_out, _rate_out);
    else
        if _rate_out is null then
            raise exception 'Rate for % is unknown. Exchange via USD or stablecoin first', _currency_out;
        end if;
        _rate_in := _rate_out * (_value_in / _value_out);
        insert into exchange_rates(datetime, currency, rate)
        values(_ts, _currency_in, _rate_in);
    end if;

    insert into cash_flow(users_id, category_id_from, value, currency, description)
           values(_users_id, _category_id, _value_out, _currency_out, concat('exchange to ', _value_in, ' ',  _currency_in))
    returning id into _cash_flow_id_out;

    perform public.mirror_cash_flow_row_to_allocation_postings(
        _cash_flow_id_out,
        'exchange',
        'manual',
        'app',
        jsonb_build_object('direction', 'out')
    );

    insert into cash_flow(users_id, category_id_to, value, currency, description)
           values(_users_id, _category_id, _value_in, _currency_in, concat('exchange from ', _value_out, ' ',  _currency_out))
    returning id into _cash_flow_id_in;

    perform public.mirror_cash_flow_row_to_allocation_postings(
        _cash_flow_id_in,
        'exchange',
        'manual',
        'app',
        jsonb_build_object('direction', 'in')
    );

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
$function$
;

-- Allocation-primary manual exchange.
-- Runtime writes only allocation_postings; legacy cash_flow stays as historical/backfill source.
CREATE OR REPLACE FUNCTION public.exchange_v2(_users_id bigint, _category_id int, _value_out numeric, _currency_out character VARYING, _value_in numeric, _currency_in character varying)
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
    _category_node_id bigint;
    _allocation_posting_id_out bigint;
    _allocation_posting_id_in bigint;
    _currency_out_norm varchar := upper(_currency_out);
    _currency_in_norm varchar := upper(_currency_in);
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

    select an.id
    into _category_node_id
    from public.allocation_nodes an
    where an.active
      and an.legacy_category_id = _category_id
      and (
          an.user_id = _users_id
          or an.user_group_id in (
              select ugm.user_group_id
              from public.user_group_memberships ugm
              where ugm.user_id = _users_id
                and ugm.active
          )
      )
    order by
        case when an.user_id = _users_id then 0 else 1 end,
        an.id
    limit 1;

    if _category_node_id is null then
        _category_node_id := public.ensure_allocation_compatibility_node(_users_id, _category_id);
    end if;

    if _category_node_id is null then
        raise exception 'Allocation category node for legacy category % not found for user %', _category_id, _users_id;
    end if;

    select rate into _rate_out
    from exchange_rates
    where currency = _currency_out_norm
    order by datetime desc
    limit 1;

    select rate into _rate_in
    from exchange_rates
    where currency = _currency_in_norm
    order by datetime desc
    limit 1;

    if _currency_out_norm = 'USD' then
        _rate_out := 1;
    end if;
    if _currency_in_norm = 'USD' then
        _rate_in := 1;
    end if;

    _is_stable_out := _currency_out_norm = ANY(_stable_currencies);
    _is_stable_in := _currency_in_norm = ANY(_stable_currencies);

    if _rate_out is null and _rate_in is null then
        raise exception 'Rates for % and % are unknown. Exchange via USD first', _currency_out_norm, _currency_in_norm;
    end if;

    -- USD is anchor: update the other currency
    if _currency_out_norm = 'USD' then
        _rate_out := 1;
        _rate_in := _rate_out * (_value_in / _value_out);
        insert into exchange_rates(datetime, currency, rate)
        values(_ts, _currency_in_norm, _rate_in);
    elsif _currency_in_norm = 'USD' then
        _rate_in := 1;
        _rate_out := _rate_in * (_value_out / _value_in);
        insert into exchange_rates(datetime, currency, rate)
        values(_ts, _currency_out_norm, _rate_out);
    -- Stablecoin updates only when exchanged with USD
    elsif _is_stable_out then
        if _rate_out is null then
            raise exception 'Stablecoin rate is unknown. Exchange stablecoin with USD first';
        end if;
        _rate_in := _rate_out * (_value_in / _value_out);
        insert into exchange_rates(datetime, currency, rate)
        values(_ts, _currency_in_norm, _rate_in);
    elsif _is_stable_in then
        if _rate_in is null then
            raise exception 'Stablecoin rate is unknown. Exchange stablecoin with USD first';
        end if;
        _rate_out := _rate_in * (_value_out / _value_in);
        insert into exchange_rates(datetime, currency, rate)
        values(_ts, _currency_out_norm, _rate_out);
    else
        if _rate_out is null then
            raise exception 'Rate for % is unknown. Exchange via USD or stablecoin first', _currency_out_norm;
        end if;
        _rate_in := _rate_out * (_value_in / _value_out);
        insert into exchange_rates(datetime, currency, rate)
        values(_ts, _currency_in_norm, _rate_in);
    end if;

    insert into public.allocation_postings(
        user_id,
        from_node_id,
        to_node_id,
        value,
        currency,
        description,
        metadata
    )
    values(
        _users_id,
        _category_node_id,
        null,
        _value_out,
        _currency_out_norm,
        concat('exchange to ', _value_in, ' ',  _currency_in_norm),
        jsonb_build_object(
            'kind', 'exchange',
            'subkind', 'manual',
            'origin', 'app',
            'direction', 'out',
            'legacy_category_id_from', _category_id
        )
    )
    returning id into _allocation_posting_id_out;

    insert into public.allocation_postings(
        user_id,
        from_node_id,
        to_node_id,
        value,
        currency,
        description,
        metadata
    )
    values(
        _users_id,
        null,
        _category_node_id,
        _value_in,
        _currency_in_norm,
        concat('exchange from ', _value_out, ' ',  _currency_out_norm),
        jsonb_build_object(
            'kind', 'exchange',
            'subkind', 'manual',
            'origin', 'app',
            'direction', 'in',
            'legacy_category_id_to', _category_id,
            'paired_posting_id', _allocation_posting_id_out
        )
    )
    returning id into _allocation_posting_id_in;

    update public.allocation_postings
    set metadata = metadata || jsonb_build_object('paired_posting_id', _allocation_posting_id_out)
    where id = _allocation_posting_id_in;

    update public.allocation_postings
    set metadata = metadata || jsonb_build_object('paired_posting_id', _allocation_posting_id_in)
    where id = _allocation_posting_id_out;

    if _currency_out_norm = 'USD' then
        _rate_out_current := 1;
    else
        _rate_out_current := coalesce(_rate_out, (select rate from exchange_rates where currency = _currency_out_norm order by datetime desc limit 1));
    end if;

    if _currency_in_norm = 'USD' then
        _rate_in_current := 1;
    else
        _rate_in_current := coalesce(_rate_in, (select rate from exchange_rates where currency = _currency_in_norm order by datetime desc limit 1));
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
              _currency_out_norm, _rate_out_text,
              _currency_in_norm, _rate_in_text);
		end
$function$
;

-- LEGACY global category lookup by name.
-- App read-paths use get_category_id_from_name_v2(...); keep this for reference/compare/rollback.
CREATE OR REPLACE FUNCTION public.get_category_id_from_name( _category_name varchar)
 RETURNS int
 LANGUAGE plpgsql
AS $function$
begin
return (SELECT id FROM categories WHERE "name" = _category_name)
			  ;
		end
$function$
;

-- Allocation-backed user-aware category lookup by display name.
CREATE OR REPLACE FUNCTION public.get_category_id_from_name_v2(_user_id bigint, _category_name varchar)
 RETURNS int
 LANGUAGE sql
 STABLE
AS $function$
    SELECT an.legacy_category_id
    FROM public.allocation_nodes an
    WHERE an.active
      AND an.legacy_category_id IS NOT NULL
      AND an."name" = _category_name
      AND (
          an.user_id = _user_id
          OR an.user_group_id IN (
              SELECT ugm.user_group_id
              FROM public.user_group_memberships ugm
              WHERE ugm.user_id = _user_id
                AND ugm.active
          )
      )
    ORDER BY
        CASE WHEN an.user_id = _user_id THEN 0 ELSE 1 END,
        an.id
    LIMIT 1;
$function$
;

-- Finds a writable user/category node by display name.
CREATE OR REPLACE FUNCTION public.find_allocation_category_node_id_by_name(_user_id bigint, _category_name varchar)
 RETURNS bigint
 LANGUAGE sql
 STABLE
AS $function$
    SELECT an.id
    FROM public.allocation_nodes an
    WHERE an.active
      AND an.legacy_category_id IS NOT NULL
      AND an."name" = _category_name
      AND (
          an.user_id = _user_id
          OR an.user_group_id IN (
              SELECT ugm.user_group_id
              FROM public.user_group_memberships ugm
              WHERE ugm.user_id = _user_id
                AND ugm.active
          )
      )
    ORDER BY
        CASE WHEN an.user_id = _user_id THEN 0 ELSE 1 END,
        an.id
    LIMIT 1;
$function$
;

-- LEGACY cash_flow-backed category balance split by currency.
-- App read-paths use get_category_balance_with_currency_v2(...); keep this for reference/compare/rollback.
CREATE OR REPLACE FUNCTION public.get_category_balance_with_currency(_user_id bigint, _category_id integer)
 RETURNS TABLE (value numeric, currency varchar)
 LANGUAGE sql
AS $function$
SELECT
    sum(cf.value) AS value,
    cf.currency
FROM
    (
    SELECT
        cash_flow.value,
        cash_flow.currency
    FROM
        cash_flow
    WHERE
        category_id_to = _category_id
        AND users_id IN (
        SELECT
            get_users_id(_user_id))
UNION ALL
SELECT
        -cash_flow.value,
        cash_flow.currency
    FROM
        cash_flow
    WHERE
        category_id_from = _category_id
        AND users_id IN (
        SELECT
            get_users_id(_user_id))
              ) cf
GROUP BY cf.currency;
$function$
;

-- Ledger-backed candidate for get_category_balance_with_currency(...).
CREATE OR REPLACE FUNCTION public.get_category_balance_with_currency_v2(_user_id bigint, _category_id integer)
 RETURNS TABLE (value numeric, currency varchar)
 LANGUAGE sql
AS $function$
SELECT
    SUM(p.value) AS value,
    p.currency
FROM (
    SELECT
        CASE
            WHEN to_node.legacy_category_id = _category_id THEN ap.value
            ELSE -ap.value
        END AS value,
        ap.currency
    FROM public.allocation_postings ap
    LEFT JOIN public.allocation_nodes from_node
      ON from_node.id = ap.from_node_id
    LEFT JOIN public.allocation_nodes to_node
      ON to_node.id = ap.to_node_id
    WHERE ap.user_id IN (SELECT get_users_id(_user_id))
      AND _category_id IN (
          from_node.legacy_category_id,
          to_node.legacy_category_id
      )
) p
GROUP BY p.currency;
$function$
;

-- LEGACY cash_flow-backed currency list.
-- App read-paths use get_currency_v2(...); keep this for reference/compare/rollback.
CREATE OR REPLACE FUNCTION public.get_currency()
 RETURNS TABLE(transact varchar)
 LANGUAGE plpgsql
AS $function$
begin
return query (
SELECT DISTINCT currency FROM cash_flow);
	end
$function$
;

-- Ledger-backed currency list.
CREATE OR REPLACE FUNCTION public.get_currency_v2()
 RETURNS TABLE(transact varchar)
 LANGUAGE sql
 STABLE
AS $function$
    SELECT DISTINCT ap.currency AS transact
    FROM public.allocation_postings ap
    ORDER BY ap.currency;
$function$
;

-- LEGACY cash_flow-primary spend with automatic exchange.
-- App write-paths use insert_spend_with_exchange_v2(...); keep this for reference/compare/rollback.
CREATE OR REPLACE FUNCTION public.insert_spend_with_exchange(_users_id bigint, _category_name_from character varying, _value numeric, _currency character varying, _description text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    _value_RUB NUMERIC(10,2);
    _reserv_id int;
    _category_id_from int;
    _rate_src numeric;
    _rate_rub numeric;
    _cash_flow_exchange_out_id bigint;
    _cash_flow_exchange_in_id bigint;
    _cash_flow_spend_id bigint;
    _currency_norm character varying := upper(_currency);
BEGIN
    IF _value <= 0 THEN
        RAISE EXCEPTION 'Spend value must be greater than zero';
    END IF;

    IF _currency_norm = 'RUB' THEN
        _value_RUB := _value;
    ELSE
        SELECT rate INTO _rate_src
        FROM exchange_rates
        WHERE currency = _currency_norm
        ORDER BY datetime DESC
        LIMIT 1;

        SELECT rate INTO _rate_rub
        FROM exchange_rates
        WHERE currency = 'RUB'
        ORDER BY datetime DESC
        LIMIT 1;

        IF _rate_src IS NULL OR _rate_rub IS NULL THEN
            RAISE EXCEPTION 'Exchange rates for % and RUB are required', _currency_norm;
        END IF;

        _value_RUB := _value / (_rate_src / _rate_rub);
    END IF;

    IF _value_RUB IS NULL THEN
        RAISE EXCEPTION 'Exchange rates for % and RUB are required', _currency_norm;
    END IF;

    _reserv_id := (SELECT get_categories_id(_users_id, 9));
    IF _reserv_id IS NULL THEN
        RAISE EXCEPTION 'Reserve category (group 9) not found for user %', _users_id;
    END IF;

    _category_id_from := (SELECT c.id
			                from categories c
			                join categories_category_groups ccg on c.id = ccg.categories_id
			                where ccg.category_groyps_id = 14 and ccg.users_id = _users_id
			                and c."name"=_category_name_from);
    IF _category_id_from IS NULL THEN
        RAISE EXCEPTION 'Category % not found in group 14 for user %', _category_name_from, _users_id;
    END IF;

    INSERT INTO cash_flow (users_id, category_id_from, category_id_to, value, currency, description)
    VALUES
        (_users_id, _reserv_id, _category_id_from, _value, _currency_norm, concat('auto exchange ', _value_RUB, ' RUB to ', _value, ' ', _currency_norm, ' ', _description))
    RETURNING id INTO _cash_flow_exchange_out_id;

    PERFORM public.mirror_cash_flow_row_to_allocation_postings(
        _cash_flow_exchange_out_id,
        'exchange',
        'auto',
        'system',
        jsonb_build_object('direction', 'out')
    );

    INSERT INTO cash_flow (users_id, category_id_from, category_id_to, value, currency, description)
    VALUES
        (_users_id, _category_id_from, _reserv_id, _value_RUB, 'RUB', concat('auto exchange ', _value, ' ', _currency_norm, ' to ', _value_RUB, ' RUB', ' ', _description))
    RETURNING id INTO _cash_flow_exchange_in_id;

    PERFORM public.mirror_cash_flow_row_to_allocation_postings(
        _cash_flow_exchange_in_id,
        'exchange',
        'auto',
        'system',
        jsonb_build_object('direction', 'in')
    );

    INSERT INTO cash_flow (users_id, category_id_from, category_id_to, value, currency, description)
    VALUES
        (_users_id, _category_id_from, NULL, _value, _currency_norm, _description)
    RETURNING id INTO _cash_flow_spend_id;

    PERFORM public.mirror_cash_flow_row_to_allocation_postings(
        _cash_flow_spend_id,
        'transaction',
        'spend',
        'app',
        jsonb_build_object('exchange_subkind', 'auto')
    );

    RETURN 'OK';
END
$function$
;

-- Allocation-primary spend write helper that requires an automatic exchange.
-- Runtime writes only allocation_postings; legacy cash_flow stays as historical/backfill source.
CREATE OR REPLACE FUNCTION public.insert_spend_with_exchange_v2(_users_id bigint, _category_name_from character varying, _value numeric, _currency character varying, _description text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    _value_RUB NUMERIC(10,2);
    _reserve_node_id bigint;
    _reserve_legacy_category_id int;
    _category_node_id bigint;
    _category_legacy_id int;
    _rate_src numeric;
    _rate_rub numeric;
    _allocation_exchange_out_id bigint;
    _allocation_exchange_in_id bigint;
    _currency_norm character varying := upper(_currency);
BEGIN
    IF _value <= 0 THEN
        RAISE EXCEPTION 'Spend value must be greater than zero';
    END IF;

    IF _currency_norm = 'RUB' THEN
        _value_RUB := _value;
    ELSE
        SELECT rate INTO _rate_src
        FROM exchange_rates
        WHERE currency = _currency_norm
        ORDER BY datetime DESC
        LIMIT 1;

        SELECT rate INTO _rate_rub
        FROM exchange_rates
        WHERE currency = 'RUB'
        ORDER BY datetime DESC
        LIMIT 1;

        IF _rate_src IS NULL OR _rate_rub IS NULL THEN
            RAISE EXCEPTION 'Exchange rates for % and RUB are required', _currency_norm;
        END IF;

        _value_RUB := _value / (_rate_src / _rate_rub);
    END IF;

    IF _value_RUB IS NULL THEN
        RAISE EXCEPTION 'Exchange rates for % and RUB are required', _currency_norm;
    END IF;

    SELECT an.id, an.legacy_category_id
    INTO _reserve_node_id, _reserve_legacy_category_id
    FROM public.allocation_nodes an
    JOIN public.allocation_node_groups ang ON ang.node_id = an.id
    WHERE an.active
      AND ang.active
      AND an.legacy_category_id IS NOT NULL
      AND ang.legacy_group_id = 9
      AND (
          an.user_id = _users_id
          OR an.user_group_id IN (
              SELECT ugm.user_group_id
              FROM public.user_group_memberships ugm
              WHERE ugm.user_id = _users_id
                AND ugm.active
          )
      )
    ORDER BY
        CASE WHEN an.user_id = _users_id THEN 0 ELSE 1 END,
        an.id
    LIMIT 1;

    IF _reserve_node_id IS NULL THEN
        RAISE EXCEPTION 'Reserve allocation category node (group 9) not found for user %', _users_id;
    END IF;

    SELECT an.id, an.legacy_category_id
    INTO _category_node_id, _category_legacy_id
    FROM public.allocation_nodes an
    JOIN public.allocation_node_groups ang ON ang.node_id = an.id
    WHERE an.active
      AND ang.active
      AND an.legacy_category_id IS NOT NULL
      AND an."name" = _category_name_from
      AND ang.legacy_group_id = 14
      AND (
          an.user_id = _users_id
          OR an.user_group_id IN (
              SELECT ugm.user_group_id
              FROM public.user_group_memberships ugm
              WHERE ugm.user_id = _users_id
                AND ugm.active
          )
      )
    ORDER BY
        CASE WHEN an.user_id = _users_id THEN 0 ELSE 1 END,
        an.id
    LIMIT 1;

    IF _category_node_id IS NULL THEN
        RAISE EXCEPTION 'Allocation category node % in group 14 not found for user %', _category_name_from, _users_id;
    END IF;

    INSERT INTO public.allocation_postings (
        user_id,
        from_node_id,
        to_node_id,
        value,
        currency,
        description,
        metadata
    )
    VALUES (
        _users_id,
        _reserve_node_id,
        _category_node_id,
        _value,
        _currency_norm,
        concat('auto exchange ', _value_RUB, ' RUB to ', _value, ' ', _currency_norm, ' ', _description),
        jsonb_strip_nulls(
            jsonb_build_object(
                'kind', 'exchange',
                'subkind', 'auto',
                'origin', 'system',
                'direction', 'out',
                'legacy_category_id_from', _reserve_legacy_category_id,
                'legacy_category_id_to', _category_legacy_id
            )
        )
    )
    RETURNING id INTO _allocation_exchange_out_id;

    INSERT INTO public.allocation_postings (
        user_id,
        from_node_id,
        to_node_id,
        value,
        currency,
        description,
        metadata
    )
    VALUES (
        _users_id,
        _category_node_id,
        _reserve_node_id,
        _value_RUB,
        'RUB',
        concat('auto exchange ', _value, ' ', _currency_norm, ' to ', _value_RUB, ' RUB', ' ', _description),
        jsonb_strip_nulls(
            jsonb_build_object(
                'kind', 'exchange',
                'subkind', 'auto',
                'origin', 'system',
                'direction', 'in',
                'legacy_category_id_from', _category_legacy_id,
                'legacy_category_id_to', _reserve_legacy_category_id
            )
        )
    )
    RETURNING id INTO _allocation_exchange_in_id;

    UPDATE public.allocation_postings
    SET metadata = jsonb_strip_nulls(
        metadata || jsonb_build_object('paired_posting_id', _allocation_exchange_in_id)
    )
    WHERE id = _allocation_exchange_out_id;

    UPDATE public.allocation_postings
    SET metadata = jsonb_strip_nulls(
        metadata || jsonb_build_object('paired_posting_id', _allocation_exchange_out_id)
    )
    WHERE id = _allocation_exchange_in_id;

    INSERT INTO public.allocation_postings (
        user_id,
        from_node_id,
        to_node_id,
        value,
        currency,
        description,
        metadata
    )
    VALUES (
        _users_id,
        _category_node_id,
        NULL,
        _value,
        _currency_norm,
        _description,
        jsonb_strip_nulls(
            jsonb_build_object(
                'kind', 'transaction',
                'subkind', 'spend',
                'origin', 'app',
                'exchange_subkind', 'auto',
                'legacy_category_id_from', _category_legacy_id,
                'exchange_out_posting_id', _allocation_exchange_out_id,
                'exchange_in_posting_id', _allocation_exchange_in_id
            )
        )
    );

    RETURN 'OK';
END
$function$
;
