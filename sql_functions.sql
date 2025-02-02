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
create or replace function distribute_to_group(_user_id bigint, _group_id int, _income_category_id int, _income_value numeric)
 returns numeric
 language plpgsql
 as $function$
 declare 
		 _reminder numeric;
begin
_reminder := (select  _income_value * (1 - sum("percent"))
	from categories c join categories_category_groups ccg on c.id = ccg.categories_id 
	where ccg.category_groyps_id = _group_id and users_id = _user_id);
set search_path to 'public';	
	insert into cash_flow (users_id, category_id_from, category_id_to, value, currency)
	select ccg.users_id, _income_category_id, c.id, _income_value * "percent", 'RUB'
	from categories c join categories_category_groups ccg on c.id = ccg.categories_id 
	where ccg.category_groyps_id = _group_id and users_id = _user_id and _income_value > 0;
return _reminder;
end
$function$;



-- принимает id пользователя и id категории прихода и распределяет по всем категориям								
CREATE OR REPLACE FUNCTION public.monthly_distribute(_user_id bigint, _income_category integer)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
 declare _sum_value numeric(10,2);
		 _sum_velue_second numeric(10,2);
		_value_for_second_member numeric;
		 _free_money numeric(10,2);
		 _reminder_first numeric;
		_second_member_id bigint;
		_second_member_free_money numeric;
		_sum_last numeric;
		_general_categories numeric;
		_general_categories_second numeric;
		_sum_earnings NUMERIC;
		_sum_spend NUMERIC;
begin
perform transact_from_group_to_category(_user_id, 11, (select get_categories_id(_user_id, 13))); -- переводим месячные доходы в одну категорию	
perform transact_from_group_to_category(_user_id, 12, (select get_categories_id(_user_id, 7))); -- переводим другие доходы в категорию продарки себе
_sum_last := (select get_category_balance(_user_id, (select get_categories_id(_user_id, 6)))); -- получение остатка свободных денег	
perform distribute_to_group(_user_id, 7, (select c.id from categories c 
join categories_category_groups ccg on c.id = ccg.categories_id
where ccg.category_groyps_id = 6 and ccg.users_id = _user_id), _sum_last); -- перевод части остатка на подарки себе
insert into cash_flow (users_id, category_id_from, category_id_to, value, currency)
select _user_id, id, (select c.id from categories c join categories_category_groups ccg on c.id = ccg.categories_id
where ccg.category_groyps_id = 9 and ccg.users_id = _user_id) , abs("sum") * 0.01, 'RUB' from (select c.id, get_category_balance(_user_id, c.id) as "sum" from categories c join
categories_category_groups ccg on c.id = ccg.categories_id 
where ccg.users_id = _user_id and ccg.category_groyps_id = 8) where "sum" < 0;-- если есть долги то увеличивает резерв на один процент за счет счета должника
_sum_value := (select get_category_balance(_user_id, _income_category)); -- сумма дохода за месяц
_value_for_second_member := (select _sum_value * ("percent") from categories where id = 15); -- сумма семейного взноса
_sum_velue_second := (select distribute_to_group(_user_id, 1, _income_category, _sum_value)); -- перевод денег на нз и остаток после
_free_money := (select distribute_to_group(_user_id, 2, _income_category, (_sum_value - _value_for_second_member ))); --распределение по категориям
_second_member_id := (select * from get_users_id(_user_id) where get_users_id != _user_id); -- id второго пользователя
_second_member_free_money := (select distribute_to_group(_second_member_id, 3, 15, _value_for_second_member)); -- распределение денег второго пользователя
perform distribute_to_group(_user_id, 6, _income_category,  _free_money - _sum_value * 0.1); --внесение свободных денег
perform distribute_to_group(_second_member_id, 6, 15, _second_member_free_money); -- внесение свободных денег второго пользователя
_general_categories := (select sum((_sum_value - _value_for_second_member) * c."percent") from categories c join categories_category_groups ccg  on 	c.id = ccg.categories_id and ccg.category_groyps_id = 4 );
_general_categories_second := (select sum(_value_for_second_member * c."percent") from categories c join categories_category_groups ccg  on c.id = ccg.categories_id and ccg.category_groyps_id = 4 );	
_sum_earnings := (SELECT COALESCE(sum(value), 0) FROM cash_flow cf WHERE users_id = _user_id AND category_id_from IS NULL AND date_trunc('month', datetime) = date_trunc('month', now()) - INTERVAL '1' MONTH);
_sum_spend := (SELECT COALESCE(sum(value), 0) FROM cash_flow cf WHERE users_id = _user_id AND category_id_to IS NULL AND date_trunc('month', datetime) = date_trunc('month', now()) - INTERVAL '1' MONTH);
return 
		jsonb_build_object('user_id', _user_id,
						   'общие_категории', _general_categories,
						   'second_user_id', _second_member_id,
						   'семейный_взнос', _value_for_second_member,
						   'second_user_pay',  _general_categories_second,
						   'investition',  _sum_value * 0.1,
						   'investition_second',  _value_for_second_member * 0.1,
						   'month_earnings', _sum_earnings,
						   'month_spend', _sum_spend
						   )  ;
end
$function$

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
create or replace function get_last_transaction(_user_id bigint, _num int)
 returns table (id bigint, datetime timestamp, "from" varchar(100), "to" varchar(100), value numeric, currency varchar(3), description text)
 language plpgsql
as $function$
begin
return query (select cf.id, cf.datetime, c."name" as "from", c2."name" as "to", cf.value, cf.currency, cf.description  from 
	(select cf_sub.id, cf_sub.datetime, cf_sub.category_id_from, cf_sub.category_id_to, cf_sub.value, cf_sub.currency, cf_sub.description, dense_rank() over(order by cf_sub.datetime desc)
	as "rank"
	from cash_flow cf_sub where users_id = _user_id) cf 
	left join categories c on cf.category_id_from = c.id 
	left join categories c2 on  cf.category_id_to = c2.id 
	where "rank" = _num);
		end
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
 LANGUAGE plpgsql
AS $function$
begin
return query (
select concat(c."name", ' ', COALESCE(c2."name", '-'), ' ', cf.value, ' ', cf.currency) AS transact
	from cash_flow cf
	left join categories c on cf.category_id_from = c.id 
	left join categories c2 on  cf.category_id_to = c2.id 
	where date_trunc('day', cf.datetime) = date_trunc('day', now()) AND users_id = _user_id);
	end
$function$
;	

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
