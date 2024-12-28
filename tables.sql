create table if not exists users (
id bigint primary key,
nickname varchar(10)
);

create table if not exists users_groups(
id serial primary key,
users_id bigint references users(id),
users_groups int);



create table if not exists categories (
id serial primary key,
"name" varchar(100),
"percent" numeric(3,2)
);

create table if not exists category_groups (
id serial primary key,
"name" varchar(100),
description text);

create table if not exists categories_category_groups (
id serial primary key,
categories_id int references categories(id),
category_groyps_id int references category_groups(id),
users_id int references users(id));



create table if not exists cash_flow (
id bigserial primary key,
users_id bigint references users(id),
"datetime" timestamp not null default now(),
category_id_from int references categories(id),
category_id_to int references categories(id),
value numeric(20,10),
currency varchar(3),
description text);


create table if not exists public.exchange_rates (
"datetime" timestamp,
currency varchar(3),
rate numeric(20,10));


CREATE INDEX idx_exchange_rates_currency_datetime ON exchange_rates (currency, datetime DESC);
CREATE INDEX idx_cash_flow_category_users ON cash_flow (users_id, category_id_to, category_id_from, currency, value);
