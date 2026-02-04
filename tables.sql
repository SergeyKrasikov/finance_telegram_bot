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
currency varchar(16),
description text);


create table if not exists public.exchange_rates (
"datetime" timestamp,
currency varchar(16),
rate numeric(20,10));

-- Compatibility upgrade for existing databases with varchar(3) currency columns
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'cash_flow'
          AND column_name = 'currency'
          AND character_maximum_length = 3
    ) THEN
        ALTER TABLE public.cash_flow
            ALTER COLUMN currency TYPE varchar(16);
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'exchange_rates'
          AND column_name = 'currency'
          AND character_maximum_length = 3
    ) THEN
        ALTER TABLE public.exchange_rates
            ALTER COLUMN currency TYPE varchar(16);
    END IF;
END $$;

-- Base USD rate (anchor) for a fresh database
INSERT INTO exchange_rates ("datetime", currency, rate)
SELECT now(), 'USD', 1
WHERE NOT EXISTS (
    SELECT 1 FROM exchange_rates WHERE currency = 'USD'
);


CREATE INDEX IF NOT EXISTS idx_exchange_rates_currency_datetime ON exchange_rates (currency, datetime DESC);
CREATE INDEX IF NOT EXISTS idx_cash_flow_user_categories ON cash_flow (users_id, category_id_from, category_id_to);
CREATE INDEX IF NOT EXISTS idx_cash_flow_user_datetime ON cash_flow (users_id, datetime DESC);
CREATE INDEX IF NOT EXISTS idx_categories_category_groups_users_group ON categories_category_groups (users_id, category_groyps_id);
CREATE INDEX IF NOT EXISTS idx_categories_name ON categories (name);
CREATE INDEX IF NOT EXISTS idx_users_groups_users_id ON users_groups (users_id);

-- Additional indexes for frequent queries
CREATE INDEX IF NOT EXISTS idx_users_groups_group_user ON users_groups (users_groups, users_id);
CREATE INDEX IF NOT EXISTS idx_cash_flow_user_category_to ON cash_flow (users_id, category_id_to);
CREATE INDEX IF NOT EXISTS idx_cash_flow_user_category_from ON cash_flow (users_id, category_id_from);
CREATE INDEX IF NOT EXISTS idx_cash_flow_income_monthly ON cash_flow (users_id, datetime) WHERE category_id_from IS NULL;
CREATE INDEX IF NOT EXISTS idx_cash_flow_spend_monthly ON cash_flow (users_id, datetime) WHERE category_id_to IS NULL;
CREATE INDEX IF NOT EXISTS idx_ccg_user_category ON categories_category_groups (users_id, categories_id);
