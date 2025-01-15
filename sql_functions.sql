-- принимает user_id и возвращает id всех пользьвателей из группы  
create or replace function get_users_id(_user_id bigint)
 returns table (user_id bigint)
 language plpgsql
as $function$
begin
return query (select ug2.users_id from users_groups ug1 join users_groups ug2 using(users_groups) where ug1.users_id = _user_id);
		end
$function$;


-- принимает user_id, id категории, валюту в которой вернуть баланс и возвращает остаток по ней  
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

    -- Расчет остатка и вставка значений в одну операцию
    with total_percent as (
        select sum("percent") as total
        from categories c 
        join categories_category_groups ccg on c.id = ccg.categories_id
        where ccg.category_groyps_id = _group_id and users_id = _user_id
    )
    insert into cash_flow (users_id, category_id_from, category_id_to, value, currency)
    select ccg.users_id, _income_category_id, c.id, _income_value * c."percent", _currency
    from categories c
    join categories_category_groups ccg on c.id = ccg.categories_id
    where ccg.category_groyps_id = _group_id and users_id = _user_id and _income_value > 0;
    select _income_value * (1 - coalesce(total, 0)) into _reminder  -- Расчет остатка
    from total_percent;
    raise notice 'Distributed % to group % for user %', _income_value, _group_id, _user_id;

    return _reminder;

exception
    when others then
        raise notice 'Error occurred while distributing income for user %: %', _user_id, sqlerrm;
        return null;
end
$function$;



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
    INSERT INTO cash_flow (users_id, category_id_from, category_id_to, value, currency)    -- Увеличение резерва на 1% за счет должников
    SELECT _user_id, id, 
           (SELECT get_categories_id(_user_id, 9)), 
           ABS("sum") * 0.01, 'RUB'
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
    _second_member_id := (SELECT id FROM get_users_id(_user_id) WHERE id != _user_id);    -- Получение ID второго пользователя
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
                      AND date_trunc('month', datetime) = date_trunc('month', now()) - INTERVAL '1 month');
	_sum_spend := (SELECT COALESCE(SUM(value), 0)  -- Подсчет расходов за месяц
                   FROM cash_flow
                   WHERE users_id = _user_id
                   AND category_id_to IS NULL
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
	insert into cash_flow (users_id, category_id_from, category_id_to, value, currency)
	select users_id, categories_id, _category_id, balance, 'RUB'  
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



-- принимает user_id и порядковый номер транзакции начиная с конца и возвращает транзакцию  
CREATE OR REPLACE FUNCTION get_last_transaction(_user_id bigint, _num int)
RETURNS TABLE (
    id bigint,
    datetime timestamp,
    "from" varchar(100),
    "to" varchar(100),
    value varchar,  -- Изменён тип на varchar для представления форматированного значения
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
                WHEN cf.value::text LIKE '%.%' THEN 
                    RTRIM(TRIM(TRAILING '0' FROM cf.value::text), '.')
                ELSE 
                    cf.value::text
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
begin 
	insert into cash_flow(users_id, datetime, category_id_from, category_id_to, value, currency, description)
		   values(_users_id, _datetime, _category_id_from, _category_id_to, _value, _currency, _description);
return 'OK';
		end
$function$
;


-- Принимает поля трат и записывает в таблицу cash_flow
CREATE OR REPLACE FUNCTION public.insert_spend(_users_id bigint, _category_name_from character varying, _value numeric DEFAULT 0, _currency character varying DEFAULT 'RUB'::character varying, _description text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
begin 
	insert into cash_flow (users_id, category_id_from, value, currency, description) 
                select _users_id, c.id, _value, _currency as currency, _description
                from categories c
                join categories_category_groups ccg on c.id = ccg.categories_id
                where ccg.category_groyps_id = 14 and ccg.users_id = _users_id
                and c."name"=_category_name_from;
return 'OK';
		end
$function$
;

-- Принимает поля доходов и записывает в таблицу cash_flow
CREATE OR REPLACE FUNCTION public.insert_revenue(_users_id bigint, _category_to character varying, _value numeric DEFAULT 0, _currency character varying DEFAULT 'RUB'::character varying, _description text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
begin 
	insert into cash_flow (users_id, category_id_to, value, currency, description) 
                select _users_id, c.id, _value, _currency, _description
                from categories c
                join categories_category_groups ccg on c.id = ccg.categories_id
                where ccg.category_groyps_id = 14 and ccg.users_id = _users_id
                and c."name"=_category_to;
return 'OK';
		end
$function$
;



-- принимает имя юзера и id группы и возвращает имена категорий этой группы, работает используя функцию get_categories_id
CREATE OR REPLACE FUNCTION public.get_categories_name(_user_id bigint, _groyps_id integer)
 RETURNS TABLE("name" varchar)
 LANGUAGE plpgsql
AS $function$
begin
return query (select c."name" from public.categories c where c.id in (select public.get_categories_id(_user_id, _groyps_id)));
		end
$function$
;


-- пирнимает id транзакций и удаляет их
CREATE OR REPLACE FUNCTION public.delete_transaction(_transactions_id bigint[])
 RETURNS text
 LANGUAGE plpgsql
AS $function$
begin 
	delete from cash_flow where id = ANY(_transactions_id);
return 'OK';
		end
$function$
;

-- принимает id пользователя и возвращает все операции за сегодня
CREATE OR REPLACE FUNCTION public.get_daily_transactions(_user_id bigint)
RETURNS TABLE(transact text)
LANGUAGE sql
AS $function$
SELECT CONCAT_WS(' ',
    c."name",
    COALESCE(c2."name", '-'),
    CASE 
        WHEN cf.value::text LIKE '%.%' THEN 
            RTRIM(TRIM(TRAILING '0' FROM cf.value::text), '.')
        ELSE 
            cf.value::text
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

-- принимает id пользователя и id пруппы, возвращает сумму всех категорий группы
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

-- принимает id пользователя и имя категории, возвращает остаток по этой категории
CREATE OR REPLACE FUNCTION public.get_remains(_user_id bigint, _category CHARACTER)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
begin
return (select COALESCE (get_category_balance(_user_id,(select c.id from categories c join categories_category_groups ccg on c.id = ccg.categories_id
                    where ccg.category_groyps_id = 14 and ccg.users_id = _user_id
                    and c.name=_category)), 0))
			  ;
		end
$function$
;   

-- принимает id пользователя и id группы, возвращает сумму всех категорий группы
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

-- запускает функции месячного распределения
CREATE OR REPLACE FUNCTION public.monthly()
 RETURNS TABLE (get_remains jsonb)
 LANGUAGE plpgsql
AS $function$
BEGIN
return  query
		(SELECT monthly_distribute(943915310, 37)
		 UNION ALL
		 SELECT monthly_distribute(249716305, 16)) ;
end
$function$
;  

-- записывает расход одной валюты и доход в той же категории другой валюты
CREATE OR REPLACE FUNCTION public.exchange(_users_id bigint, _category_id int, _value_out numeric, _currency_out character VARYING, _value_in numeric, _currency_in character varying)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
begin 
	insert into cash_flow(users_id, category_id_from, value, currency, description)
		   values(_users_id, _category_id, _value_out, _currency_out, concat('exchange to ', _value_in, ' ',  _currency_in));
	insert into cash_flow(users_id, category_id_to, value, currency, description)
		   values(_users_id, _category_id, _value_in, _currency_in, concat('exchange from ', _value_out, ' ',  _currency_out));
return 'OK';
		end
$function$
;

--получение id категории из имени
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

-- получить баланс категории с разбивкой по валютам
CREATE OR REPLACE FUNCTION public.get_category_balance_with_currency(_user_id bigint, _category_id integer)
 RETURNS TABLE (value numeric, currensy varchar)
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
		AND users_id IN (
		SELECT
			get_users_id(_user_id))
UNION ALL
	SELECT
		-cash_flow.value,
		currency
	FROM
		cash_flow
	WHERE
		category_id_from = _category_id
		AND users_id IN (
		SELECT
			get_users_id(_user_id))
			  ) cf
GROUP BY currency);
END
$function$
;

-- возвращает все валюты которые есть в базе
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

-- записывает расход при и меняет валюту если это не основная валюта

CREATE OR REPLACE FUNCTION public.insert_spend_with_exchange(_users_id bigint, _category_name_from character varying, _value numeric, _currency character varying, _description text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    _value_RUB NUMERIC(10,2);
    _reserv_id int;
    _category_id_from int;
BEGIN 
    _value_RUB := (SELECT _value / (er.rate / er2.rate) as value
FROM (SELECT
		datetime,
		currency,
		rate,
		ROW_NUMBER() OVER (PARTITION BY currency ORDER BY datetime DESC) AS rown
	  FROM
		exchange_rates) er
JOIN exchange_rates er2 ON er.datetime = er2.datetime 
WHERE er.currency = _currency 
AND er2.currency = 'RUB'  
AND rown = 1);
    _reserv_id := (SELECT get_categories_id(_users_id, 9));
    _category_id_from := (SELECT c.id
			                from categories c
			                join categories_category_groups ccg on c.id = ccg.categories_id
			                where ccg.category_groyps_id = 14 and ccg.users_id = _users_id
			                and c."name"=_category_name_from);

    INSERT INTO cash_flow (users_id, category_id_from, category_id_to, value, currency, description)
    VALUES
        (_users_id, _reserv_id, _category_id_from, _value, _currency, concat('auto exchange ', _value_RUB, ' RUB to ', _value, ' ', _currency, ' ', _description)),
        (_users_id, _category_id_from, _reserv_id, _value_RUB, 'RUB', concat('auto exchange ', _value, ' ', _currency, ' to ', _value_RUB, ' RUB', ' ', _description)),
        (_users_id, _category_id_from, NULL, _value, _currency, _description);

    RETURN 'OK';
END
$function$
;
