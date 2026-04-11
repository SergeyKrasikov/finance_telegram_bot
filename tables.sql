create table if not exists users (
id bigint primary key,
nickname varchar(10)
);

alter table public.users
    add column if not exists created_at timestamptz not null default now();

alter table public.users
    add column if not exists active boolean not null default true;

create table if not exists users_groups(
id serial primary key,
users_id bigint references users(id),
users_groups int);

create table if not exists public.user_groups (
id bigserial primary key,
slug varchar(100) unique,
"name" varchar(100) not null,
description text,
created_at timestamptz not null default now(),
active boolean not null default true
);

create table if not exists public.user_group_memberships (
id bigserial primary key,
user_id bigint not null references users(id),
user_group_id bigint not null references public.user_groups(id),
joined_at timestamptz not null default now(),
active boolean not null default true,
unique(user_id, user_group_id)
);



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

create table if not exists public.allocation_nodes (
id bigserial primary key,
user_id bigint references users(id),
user_group_id bigint references public.user_groups(id),
slug varchar(100) not null,
"name" varchar(100) not null,
description text,
node_kind varchar(16) not null,
-- Compatibility bridge while cash_flow still references legacy categories.
legacy_category_id int references categories(id),
visible boolean not null default true,
include_in_report boolean not null default false,
metadata jsonb not null default '{}'::jsonb,
active boolean not null default true,
constraint allocation_nodes_owner_check
    check ((user_id is not null) <> (user_group_id is not null)),
constraint allocation_nodes_node_kind_check
    check (node_kind in ('technical', 'income', 'expense', 'both', 'neutral', 'storage'))
);

alter table public.allocation_nodes
    add column if not exists metadata jsonb not null default '{}'::jsonb;

alter table public.allocation_nodes
    alter column metadata set default '{}'::jsonb;

update public.allocation_nodes
set metadata = '{}'::jsonb
where metadata is null;

alter table public.allocation_nodes
    alter column metadata set not null;

create table if not exists public.allocation_routes (
id bigserial primary key,
source_node_id bigint not null references public.allocation_nodes(id) on delete cascade,
target_node_id bigint not null references public.allocation_nodes(id) on delete cascade,
percent numeric(10,6) not null,
description text,
metadata jsonb not null default '{}'::jsonb,
active boolean not null default true,
constraint allocation_routes_percent_check
    check (percent > 0 and percent <= 1)
);

alter table public.allocation_routes
    add column if not exists metadata jsonb not null default '{}'::jsonb;

create table if not exists public.allocation_node_groups (
id bigserial primary key,
node_id bigint not null references public.allocation_nodes(id) on delete cascade,
legacy_group_id int not null references category_groups(id),
active boolean not null default true,
unique(node_id, legacy_group_id)
);

create table if not exists public.allocation_postings (
id bigserial primary key,
"datetime" timestamp not null default now(),
user_id bigint not null references users(id),
from_node_id bigint references public.allocation_nodes(id),
to_node_id bigint references public.allocation_nodes(id),
value numeric(20,10) not null,
currency varchar(16) not null,
description text,
metadata jsonb not null default '{}'::jsonb,
constraint allocation_postings_value_check
    check (value > 0),
constraint allocation_postings_direction_check
    check (from_node_id is not null or to_node_id is not null)
);

create table if not exists public.allocation_scenarios (
id bigserial primary key,
owner_user_id bigint references users(id),
owner_user_group_id bigint references public.user_groups(id),
scenario_kind varchar(32) not null,
schedule_cron varchar(100),
slug varchar(100) not null,
"name" varchar(100) not null,
description text,
active boolean not null default true,
metadata jsonb not null default '{}'::jsonb,
created_at timestamptz not null default now(),
constraint allocation_scenarios_owner_check
    check ((owner_user_id is not null) <> (owner_user_group_id is not null)),
constraint allocation_scenarios_kind_check
    check (length(trim(scenario_kind)) > 0),
constraint allocation_scenarios_schedule_cron_check
    check (schedule_cron is null or length(trim(schedule_cron)) > 0)
);

create table if not exists public.allocation_scenario_node_bindings (
id bigserial primary key,
scenario_id bigint not null references public.allocation_scenarios(id) on delete cascade,
root_node_id bigint not null references public.allocation_nodes(id) on delete cascade,
binding_kind varchar(32) not null,
bound_node_id bigint not null references public.allocation_nodes(id) on delete cascade,
priority integer not null default 100,
active boolean not null default true,
metadata jsonb not null default '{}'::jsonb,
constraint allocation_scenario_node_bindings_kind_check
    check (length(trim(binding_kind)) > 0)
);

create table if not exists public.allocation_scenario_root_params (
id bigserial primary key,
scenario_id bigint not null references public.allocation_scenarios(id) on delete cascade,
root_node_id bigint not null references public.allocation_nodes(id) on delete cascade,
param_key varchar(64) not null,
param_value text not null,
active boolean not null default true,
metadata jsonb not null default '{}'::jsonb,
constraint allocation_scenario_root_params_key_check
    check (length(trim(param_key)) > 0),
constraint allocation_scenario_root_params_value_check
    check (length(trim(param_value)) > 0)
);

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
CREATE INDEX IF NOT EXISTS idx_user_group_memberships_user ON public.user_group_memberships (user_id);
CREATE INDEX IF NOT EXISTS idx_user_group_memberships_group ON public.user_group_memberships (user_group_id);
CREATE INDEX IF NOT EXISTS idx_allocation_nodes_user ON public.allocation_nodes (user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_allocation_nodes_group ON public.allocation_nodes (user_group_id) WHERE user_group_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_allocation_nodes_legacy_category ON public.allocation_nodes (legacy_category_id) WHERE legacy_category_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_allocation_nodes_user_slug ON public.allocation_nodes (user_id, slug) WHERE user_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_allocation_nodes_group_slug ON public.allocation_nodes (user_group_id, slug) WHERE user_group_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_allocation_routes_source ON public.allocation_routes (source_node_id);
CREATE INDEX IF NOT EXISTS idx_allocation_routes_target ON public.allocation_routes (target_node_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_allocation_routes_source_target ON public.allocation_routes (source_node_id, target_node_id);
CREATE INDEX IF NOT EXISTS idx_allocation_node_groups_group ON public.allocation_node_groups (legacy_group_id);
CREATE INDEX IF NOT EXISTS idx_allocation_node_groups_node ON public.allocation_node_groups (node_id);
CREATE INDEX IF NOT EXISTS idx_allocation_postings_user_datetime ON public.allocation_postings (user_id, datetime DESC);
CREATE INDEX IF NOT EXISTS idx_allocation_postings_from_node ON public.allocation_postings (from_node_id, datetime DESC) WHERE from_node_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_allocation_postings_to_node ON public.allocation_postings (to_node_id, datetime DESC) WHERE to_node_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_allocation_postings_legacy_cash_flow_id
    ON public.allocation_postings ((metadata->>'legacy_cash_flow_id'))
    WHERE metadata ? 'legacy_cash_flow_id';
CREATE INDEX IF NOT EXISTS idx_allocation_scenarios_owner_user
    ON public.allocation_scenarios (owner_user_id, scenario_kind, active)
    WHERE owner_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_allocation_scenarios_owner_group
    ON public.allocation_scenarios (owner_user_group_id, scenario_kind, active)
    WHERE owner_user_group_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_allocation_scenarios_schedule
    ON public.allocation_scenarios (active, schedule_cron)
    WHERE schedule_cron IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_allocation_scenarios_user_kind_slug
    ON public.allocation_scenarios (owner_user_id, scenario_kind, slug)
    WHERE owner_user_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_allocation_scenarios_group_kind_slug
    ON public.allocation_scenarios (owner_user_group_id, scenario_kind, slug)
    WHERE owner_user_group_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_allocation_scenario_bindings_lookup
    ON public.allocation_scenario_node_bindings (scenario_id, root_node_id, binding_kind, priority DESC, id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_allocation_scenario_bindings
    ON public.allocation_scenario_node_bindings (scenario_id, root_node_id, binding_kind, bound_node_id);
CREATE INDEX IF NOT EXISTS idx_allocation_scenario_root_params_lookup
    ON public.allocation_scenario_root_params (scenario_id, root_node_id, param_key, id)
    WHERE active;
CREATE UNIQUE INDEX IF NOT EXISTS uq_allocation_scenario_root_params
    ON public.allocation_scenario_root_params (scenario_id, root_node_id, param_key);
