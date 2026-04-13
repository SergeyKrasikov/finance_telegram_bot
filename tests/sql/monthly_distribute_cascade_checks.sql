-- monthly_distribute_cascade end-to-end checks
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/monthly_distribute_cascade_checks.sql

BEGIN;

DELETE FROM allocation_postings WHERE user_id IN (906021, 906022);
DELETE FROM cash_flow WHERE users_id IN (906021, 906022);

DELETE FROM public.allocation_scenarios
WHERE owner_user_id IN (906021, 906022)
   OR slug = 'test_monthly_cascade';

DELETE FROM allocation_routes
WHERE source_node_id IN (
    SELECT id
    FROM allocation_nodes
    WHERE user_id IN (906021, 906022)
       OR user_group_id IN (
            SELECT id
            FROM user_groups
            WHERE slug = 'test_monthly_cascade_group'
       )
)
   OR target_node_id IN (
    SELECT id
    FROM allocation_nodes
    WHERE user_id IN (906021, 906022)
       OR user_group_id IN (
            SELECT id
            FROM user_groups
            WHERE slug = 'test_monthly_cascade_group'
       )
);

DELETE FROM allocation_nodes
WHERE user_id IN (906021, 906022)
   OR user_group_id IN (
        SELECT id
        FROM user_groups
        WHERE slug = 'test_monthly_cascade_group'
   );

DELETE FROM user_group_memberships WHERE user_id IN (906021, 906022);
DELETE FROM user_groups WHERE slug = 'test_monthly_cascade_group';
DELETE FROM categories
WHERE id IN (
    906201,
    906202,
    906203,
    906204,
    906206,
    906207,
    906209,
    906213,
    906214,
    906215,
    906216,
    906217,
    906218,
    906221
);
DELETE FROM category_groups WHERE id IN (906808, 906811, 906812, 906815);
DELETE FROM users WHERE id IN (906021, 906022);

INSERT INTO users(id, nickname) VALUES
    (906021, 'mc1'),
    (906022, 'mc2');

INSERT INTO category_groups(id, "name", description) VALUES
    (906808, 'test spend', ''),
    (906811, 'test monthly income source', ''),
    (906812, 'test extra income source', ''),
    (906815, 'test personal', '');

INSERT INTO categories(id, "name", "percent") VALUES
    (906201, 'test invest self leaf', 0.00),
    (906202, 'test self personal leaf', 0.00),
    (906203, 'test partner personal leaf', 0.00),
    (906204, 'test common leaf', 0.00),
    (906206, 'test free leaf', 0.00),
    (906207, 'test gifts leaf', 0.00),
    (906209, 'test reserve leaf', 0.00),
    (906213, 'test salary bucket', 0.00),
    (906214, 'test monthly income source leaf', 0.00),
    (906215, 'test extra income source leaf', 0.00),
    (906216, 'test negative personal spend leaf', 0.00),
    (906217, 'test history bucket', 0.00),
    (906218, 'test partner family source leaf', 0.00),
    (906221, 'test invest partner leaf', 0.00);

INSERT INTO exchange_rates("datetime", currency, rate)
VALUES (now(), 'RUB', 1);

INSERT INTO user_groups(slug, "name", description)
VALUES ('test_monthly_cascade_group', 'test monthly cascade group', 'fixture');

INSERT INTO user_group_memberships(user_id, user_group_id)
SELECT 906021, id
FROM user_groups
WHERE slug = 'test_monthly_cascade_group';

INSERT INTO user_group_memberships(user_id, user_group_id)
SELECT 906022, id
FROM user_groups
WHERE slug = 'test_monthly_cascade_group';

INSERT INTO allocation_nodes(
    user_id,
    user_group_id,
    slug,
    "name",
    description,
    node_kind,
    legacy_category_id,
    visible,
    include_in_report,
    active
)
VALUES
    (906021, NULL, 'monthly_income_sources', 'monthly income sources', 'prep root', 'technical', NULL, false, false, true),
    (906021, NULL, 'extra_income_sources', 'extra income sources', 'prep root', 'technical', NULL, false, false, true),
    (906021, NULL, 'free_to_gifts', 'free to gifts', 'prep root', 'technical', NULL, false, false, true),
    (906021, NULL, 'debt_reserve', 'debt reserve', 'prep root', 'technical', NULL, false, false, true),
    (906021, NULL, 'salary_primary', 'salary primary', 'main root', 'technical', NULL, false, false, true),
    (906021, NULL, 'invest_self_report', 'invest self report', 'report node', 'technical', NULL, false, true, true),
    (906021, NULL, 'family_contribution_out', 'family contribution out', 'report bridge', 'technical', NULL, false, true, true),
    (906021, NULL, 'self_distribution', 'self distribution', 'self branch', 'technical', NULL, false, false, true),
    (906021, NULL, 'cat_906201', 'self invest leaf', 'leaf', 'expense', 906201, true, false, true),
    (906021, NULL, 'cat_906202', 'self personal leaf', 'leaf', 'expense', 906202, true, true, true),
    (906021, NULL, 'cat_906206', 'free leaf', 'leaf', 'expense', 906206, true, false, true),
    (906021, NULL, 'cat_906207', 'gifts leaf', 'leaf', 'expense', 906207, true, false, true),
    (906021, NULL, 'cat_906209', 'reserve leaf', 'leaf', 'expense', 906209, true, false, true),
    (906021, NULL, 'cat_906213', 'salary bucket leaf', 'leaf', 'income', 906213, true, false, true),
    (906021, NULL, 'cat_906214', 'monthly income source leaf', 'leaf', 'income', 906214, true, false, true),
    (906021, NULL, 'cat_906215', 'extra income source leaf', 'leaf', 'income', 906215, true, false, true),
    (906021, NULL, 'cat_906216', 'negative personal spend leaf', 'leaf', 'expense', 906216, true, false, true),
    (906021, NULL, 'cat_906217', 'history bucket leaf', 'leaf', 'both', 906217, true, false, true),
    (906022, NULL, 'family_contribution_in', 'family contribution in', 'bridge target', 'technical', NULL, false, false, true),
    (906022, NULL, 'partner_contribution_split', 'partner contribution split', 'partner split', 'technical', NULL, false, false, true),
    (906022, NULL, 'invest_partner_report', 'invest partner report', 'report node', 'technical', NULL, false, true, true),
    (906022, NULL, 'partner_distribution', 'partner distribution', 'partner branch', 'technical', NULL, false, false, true),
    (906022, NULL, 'cat_906203', 'partner personal leaf', 'leaf', 'expense', 906203, true, true, true),
    (906022, NULL, 'cat_906218', 'partner family source leaf', 'leaf', 'expense', 906218, true, false, true),
    (906022, NULL, 'cat_906221', 'partner invest leaf', 'leaf', 'expense', 906221, true, false, true);

INSERT INTO allocation_nodes(
    user_id,
    user_group_id,
    slug,
    "name",
    description,
    node_kind,
    legacy_category_id,
    visible,
    include_in_report,
    active
)
SELECT
    NULL,
    id,
    'cat_906204',
    'common leaf',
    'shared common leaf',
    'expense',
    906204,
    true,
    true,
    true
FROM user_groups
WHERE slug = 'test_monthly_cascade_group';

INSERT INTO allocation_node_groups(node_id, legacy_group_id)
SELECT id, 906811
FROM allocation_nodes
WHERE user_id = 906021
  AND slug = 'cat_906214';

INSERT INTO allocation_node_groups(node_id, legacy_group_id)
SELECT id, 906812
FROM allocation_nodes
WHERE user_id = 906021
  AND slug = 'cat_906215';

INSERT INTO allocation_node_groups(node_id, legacy_group_id)
SELECT id, 906808
FROM allocation_nodes
WHERE user_id = 906021
  AND slug = 'cat_906216';

INSERT INTO allocation_node_groups(node_id, legacy_group_id)
SELECT id, 906815
FROM allocation_nodes
WHERE user_id = 906021
  AND slug = 'cat_906216';

INSERT INTO public.allocation_scenarios(
    owner_user_id,
    scenario_kind,
    schedule_cron,
    slug,
    "name",
    description,
    active
)
VALUES (
    906021,
    'monthly',
    NULL,
    'test_monthly_cascade',
    'test monthly cascade',
    'fixture scenario for monthly_distribute_cascade checks',
    true
);

INSERT INTO public.allocation_scenario_node_bindings(
    scenario_id,
    root_node_id,
    binding_kind,
    bound_node_id,
    priority,
    active,
    metadata
)
SELECT
    scenario.id,
    root.id,
    'branch_source',
    bound.id,
    100,
    true,
    jsonb_build_object('origin', 'monthly_distribute_cascade_checks')
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = 906021
 AND root.slug = 'salary_primary'
JOIN public.allocation_nodes bound
  ON bound.user_id = 906021
 AND bound.slug = 'cat_906213'
WHERE scenario.owner_user_id = 906021
  AND scenario.scenario_kind = 'monthly'
  AND scenario.slug = 'test_monthly_cascade';

INSERT INTO public.allocation_scenario_node_bindings(
    scenario_id,
    root_node_id,
    binding_kind,
    bound_node_id,
    priority,
    active,
    metadata
)
SELECT
    scenario.id,
    root.id,
    'bridge_source',
    bound.id,
    100,
    true,
    jsonb_build_object('origin', 'monthly_distribute_cascade_checks')
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = 906021
 AND root.slug = 'family_contribution_out'
JOIN public.allocation_nodes bound
  ON bound.user_id = 906022
 AND bound.slug = 'cat_906218'
WHERE scenario.owner_user_id = 906021
  AND scenario.scenario_kind = 'monthly'
  AND scenario.slug = 'test_monthly_cascade';

INSERT INTO public.allocation_scenario_root_params(
    scenario_id,
    root_node_id,
    param_key,
    param_value,
    active,
    metadata
)
SELECT
    scenario.id,
    root.id,
    'source_legacy_group_id',
    '906811',
    true,
    jsonb_build_object('origin', 'monthly_distribute_cascade_checks')
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = 906021
 AND root.slug = 'monthly_income_sources'
WHERE scenario.owner_user_id = 906021
  AND scenario.scenario_kind = 'monthly'
  AND scenario.slug = 'test_monthly_cascade';

INSERT INTO public.allocation_scenario_root_params(
    scenario_id,
    root_node_id,
    param_key,
    param_value,
    active,
    metadata
)
SELECT
    scenario.id,
    root.id,
    'source_legacy_group_id',
    '906812',
    true,
    jsonb_build_object('origin', 'monthly_distribute_cascade_checks')
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = 906021
 AND root.slug = 'extra_income_sources'
WHERE scenario.owner_user_id = 906021
  AND scenario.scenario_kind = 'monthly'
  AND scenario.slug = 'test_monthly_cascade';

INSERT INTO public.allocation_scenario_root_params(
    scenario_id,
    root_node_id,
    param_key,
    param_value,
    active,
    metadata
)
SELECT scenario.id, root.id, param.param_key, param.param_value, true, jsonb_build_object('origin', 'monthly_distribute_cascade_checks')
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = 906021
 AND root.slug = 'debt_reserve'
JOIN (
    VALUES
        ('spend_legacy_group_id', '906808'),
        ('personal_legacy_group_id', '906815')
) AS param(param_key, param_value)
  ON true
WHERE scenario.owner_user_id = 906021
  AND scenario.scenario_kind = 'monthly'
  AND scenario.slug = 'test_monthly_cascade';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'monthly income prep'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906021
 AND dst.slug = 'cat_906213'
WHERE src.user_id = 906021
  AND src.slug = 'monthly_income_sources';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'extra income prep'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906021
 AND dst.slug = 'cat_906207'
WHERE src.user_id = 906021
  AND src.slug = 'extra_income_sources';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 0.5, 'free to gifts share'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906021
 AND dst.slug = 'cat_906207'
WHERE src.user_id = 906021
  AND src.slug = 'free_to_gifts';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'reserve move'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906021
 AND dst.slug = 'cat_906209'
WHERE src.user_id = 906021
  AND src.slug = 'debt_reserve';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 0.10, 'salary to invest self'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906021
 AND dst.slug = 'invest_self_report'
WHERE src.user_id = 906021
  AND src.slug = 'salary_primary';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 0.40, 'salary to family contribution'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906021
 AND dst.slug = 'family_contribution_out'
WHERE src.user_id = 906021
  AND src.slug = 'salary_primary';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'salary remainder to self distribution'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906021
 AND dst.slug = 'self_distribution'
WHERE src.user_id = 906021
  AND src.slug = 'salary_primary';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'invest self to leaf'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906021
 AND dst.slug = 'cat_906201'
WHERE src.user_id = 906021
  AND src.slug = 'invest_self_report';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'family bridge'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906022
 AND dst.slug = 'family_contribution_in'
WHERE src.user_id = 906021
  AND src.slug = 'family_contribution_out';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'family in to split'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906022
 AND dst.slug = 'partner_contribution_split'
WHERE src.user_id = 906022
  AND src.slug = 'family_contribution_in';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 0.10, 'partner split to invest'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906022
 AND dst.slug = 'invest_partner_report'
WHERE src.user_id = 906022
  AND src.slug = 'partner_contribution_split';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'partner split remainder'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906022
 AND dst.slug = 'partner_distribution'
WHERE src.user_id = 906022
  AND src.slug = 'partner_contribution_split';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'invest partner to leaf'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906022
 AND dst.slug = 'cat_906221'
WHERE src.user_id = 906022
  AND src.slug = 'invest_partner_report';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 0.25, 'self to personal'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906021
 AND dst.slug = 'cat_906202'
WHERE src.user_id = 906021
  AND src.slug = 'self_distribution';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 0.25, 'self to common'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.slug = 'cat_906204'
WHERE src.user_id = 906021
  AND src.slug = 'self_distribution';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'self remainder to free'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906021
 AND dst.slug = 'cat_906206'
WHERE src.user_id = 906021
  AND src.slug = 'self_distribution';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 0.50, 'partner to common'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.slug = 'cat_906204'
WHERE src.user_id = 906022
  AND src.slug = 'partner_distribution';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'partner remainder to personal'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906022
 AND dst.slug = 'cat_906203'
WHERE src.user_id = 906022
  AND src.slug = 'partner_distribution';

INSERT INTO allocation_postings(user_id, to_node_id, value, currency, description, metadata)
SELECT
    906021,
    id,
    50,
    'RUB',
    'fixture monthly income source',
    jsonb_build_object('kind', 'fixture')
FROM allocation_nodes
WHERE user_id = 906021
  AND slug = 'cat_906214';

INSERT INTO allocation_postings(user_id, to_node_id, value, currency, description, metadata)
SELECT
    906021,
    id,
    20,
    'RUB',
    'fixture extra income source',
    jsonb_build_object('kind', 'fixture')
FROM allocation_nodes
WHERE user_id = 906021
  AND slug = 'cat_906215';

INSERT INTO allocation_postings(user_id, to_node_id, value, currency, description, metadata)
SELECT
    906021,
    id,
    100,
    'RUB',
    'fixture salary source',
    jsonb_build_object('kind', 'fixture')
FROM allocation_nodes
WHERE user_id = 906021
  AND slug = 'cat_906213';

INSERT INTO allocation_postings(user_id, to_node_id, value, currency, description, metadata)
SELECT
    906021,
    id,
    10,
    'RUB',
    'fixture free balance',
    jsonb_build_object('kind', 'fixture')
FROM allocation_nodes
WHERE user_id = 906021
  AND slug = 'cat_906206';

INSERT INTO allocation_postings(user_id, from_node_id, value, currency, description, metadata)
SELECT
    906021,
    id,
    100,
    'RUB',
    'fixture negative personal spend',
    jsonb_build_object('kind', 'fixture')
FROM allocation_nodes
WHERE user_id = 906021
  AND slug = 'cat_906216';

INSERT INTO allocation_postings(user_id, "datetime", to_node_id, value, currency, description, metadata)
SELECT
    906021,
    date_trunc('month', now()) - INTERVAL '1 month' + INTERVAL '1 day',
    id,
    70,
    'RUB',
    'fixture previous earning',
    jsonb_build_object('kind', 'fixture')
FROM allocation_nodes
WHERE user_id = 906021
  AND slug = 'cat_906217';

INSERT INTO allocation_postings(user_id, "datetime", from_node_id, value, currency, description, metadata)
SELECT
    906021,
    date_trunc('month', now()) - INTERVAL '1 month' + INTERVAL '2 days',
    id,
    30,
    'RUB',
    'fixture previous spend',
    jsonb_build_object('kind', 'fixture')
FROM allocation_nodes
WHERE user_id = 906021
  AND slug = 'cat_906217';

DO $$
DECLARE
    out jsonb;
    gifts_amount numeric;
    reserve_amount numeric;
    self_invest_amount numeric;
    self_personal_amount numeric;
    self_common_amount numeric;
    self_free_amount numeric;
    partner_invest_amount numeric;
    partner_personal_amount numeric;
    partner_common_amount numeric;
    linked_legacy_rows integer;
    monthly_cash_flow_rows integer;
BEGIN
    out := public.monthly_distribute_cascade(906021);

    IF (out ->> 'user_id')::bigint <> 906021 THEN
        RAISE EXCEPTION 'Expected user_id 906021, got %', out ->> 'user_id';
    END IF;

    IF (out ->> 'second_user_id')::bigint <> 906022 THEN
        RAISE EXCEPTION 'Expected second_user_id 906022, got %', out ->> 'second_user_id';
    END IF;

    IF abs((out ->> 'семейный_взнос')::numeric - 60) > 1e-9 THEN
        RAISE EXCEPTION 'Expected семейный_взнос 60, got %', out ->> 'семейный_взнос';
    END IF;

    IF abs((out ->> 'общие_категории')::numeric - 18.75) > 1e-9 THEN
        RAISE EXCEPTION 'Expected общие_категории 18.75, got %', out ->> 'общие_категории';
    END IF;

    IF abs((out ->> 'second_user_pay')::numeric - 27) > 1e-9 THEN
        RAISE EXCEPTION 'Expected second_user_pay 27, got %', out ->> 'second_user_pay';
    END IF;

    IF abs((out ->> 'investition')::numeric - 15) > 1e-9 THEN
        RAISE EXCEPTION 'Expected investition 15, got %', out ->> 'investition';
    END IF;

    IF abs((out ->> 'investition_second')::numeric - 6) > 1e-9 THEN
        RAISE EXCEPTION 'Expected investition_second 6, got %', out ->> 'investition_second';
    END IF;

    IF abs((out ->> 'month_earnings')::numeric - 70) > 1e-9 THEN
        RAISE EXCEPTION 'Expected month_earnings 70, got %', out ->> 'month_earnings';
    END IF;

    IF abs((out ->> 'month_spend')::numeric - 30) > 1e-9 THEN
        RAISE EXCEPTION 'Expected month_spend 30, got %', out ->> 'month_spend';
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO gifts_amount
    FROM allocation_postings
    WHERE user_id = 906021
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE user_id = 906021
            AND slug = 'cat_906207'
      )
      AND description = 'monthly distribute';

    IF abs(gifts_amount - 25) > 1e-9 THEN
        RAISE EXCEPTION 'Expected gifts prep amount 25, got %', gifts_amount;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO reserve_amount
    FROM allocation_postings
    WHERE user_id = 906021
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE user_id = 906021
            AND slug = 'cat_906209'
      )
      AND description = 'monthly distribute';

    IF abs(reserve_amount - 1) > 1e-9 THEN
        RAISE EXCEPTION 'Expected reserve prep amount 1, got %', reserve_amount;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO self_invest_amount
    FROM allocation_postings
    WHERE user_id = 906021
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE user_id = 906021
            AND slug = 'cat_906201'
      )
      AND description = 'monthly distribute';

    IF abs(self_invest_amount - 15) > 1e-9 THEN
        RAISE EXCEPTION 'Expected self invest amount 15, got %', self_invest_amount;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO self_personal_amount
    FROM allocation_postings
    WHERE user_id = 906021
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE user_id = 906021
            AND slug = 'cat_906202'
      )
      AND description = 'monthly distribute';

    IF abs(self_personal_amount - 18.75) > 1e-9 THEN
        RAISE EXCEPTION 'Expected self personal amount 18.75, got %', self_personal_amount;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO self_common_amount
    FROM allocation_postings
    WHERE user_id = 906021
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE slug = 'cat_906204'
      )
      AND description = 'monthly distribute';

    IF abs(self_common_amount - 18.75) > 1e-9 THEN
        RAISE EXCEPTION 'Expected primary common amount 18.75, got %', self_common_amount;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO self_free_amount
    FROM allocation_postings
    WHERE user_id = 906021
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE user_id = 906021
            AND slug = 'cat_906206'
      )
      AND description = 'monthly distribute';

    IF abs(self_free_amount - 37.5) > 1e-9 THEN
        RAISE EXCEPTION 'Expected self free amount 37.5, got %', self_free_amount;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO partner_invest_amount
    FROM allocation_postings
    WHERE user_id = 906022
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE user_id = 906022
            AND slug = 'cat_906221'
      )
      AND description = 'monthly distribute';

    IF abs(partner_invest_amount - 6) > 1e-9 THEN
        RAISE EXCEPTION 'Expected partner invest amount 6, got %', partner_invest_amount;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO partner_personal_amount
    FROM allocation_postings
    WHERE user_id = 906022
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE user_id = 906022
            AND slug = 'cat_906203'
      )
      AND description = 'monthly distribute';

    IF abs(partner_personal_amount - 27) > 1e-9 THEN
        RAISE EXCEPTION 'Expected partner personal amount 27, got %', partner_personal_amount;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO partner_common_amount
    FROM allocation_postings
    WHERE user_id = 906022
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE slug = 'cat_906204'
      )
      AND description = 'monthly distribute';

    IF abs(partner_common_amount - 27) > 1e-9 THEN
        RAISE EXCEPTION 'Expected partner common amount 27, got %', partner_common_amount;
    END IF;

    SELECT COUNT(*)
    INTO linked_legacy_rows
    FROM allocation_postings
    WHERE user_id IN (906021, 906022)
      AND description = 'monthly distribute'
      AND metadata ? 'legacy_cash_flow_id';

    IF linked_legacy_rows <> 0 THEN
        RAISE EXCEPTION 'Expected no legacy_cash_flow_id on ledger-only monthly rows, got %', linked_legacy_rows;
    END IF;

    SELECT COUNT(*)
    INTO monthly_cash_flow_rows
    FROM cash_flow
    WHERE users_id IN (906021, 906022)
      AND description = 'monthly distribute';

    IF monthly_cash_flow_rows <> 0 THEN
        RAISE EXCEPTION 'Expected no cash_flow monthly distribute rows, got %', monthly_cash_flow_rows;
    END IF;

    BEGIN
        DELETE FROM public.allocation_scenario_node_bindings
        WHERE scenario_id = (
            SELECT id
            FROM public.allocation_scenarios
            WHERE owner_user_id = 906021
              AND scenario_kind = 'monthly'
              AND slug = 'test_monthly_cascade'
        )
          AND root_node_id = (
              SELECT id
              FROM public.allocation_nodes
              WHERE user_id = 906021
                AND slug = 'salary_primary'
          )
          AND binding_kind = 'branch_source';

        PERFORM public.monthly_distribute_cascade(906021);
        RAISE EXCEPTION 'Expected missing branch_source binding to fail';
    EXCEPTION WHEN OTHERS THEN
        IF POSITION('branch_source binding is required' IN SQLERRM) = 0 THEN
            RAISE;
        END IF;
    END;

    BEGIN
        DELETE FROM public.allocation_scenario_node_bindings
        WHERE scenario_id = (
            SELECT id
            FROM public.allocation_scenarios
            WHERE owner_user_id = 906021
              AND scenario_kind = 'monthly'
              AND slug = 'test_monthly_cascade'
        )
          AND root_node_id = (
              SELECT id
              FROM public.allocation_nodes
              WHERE user_id = 906021
                AND slug = 'family_contribution_out'
          )
          AND binding_kind = 'bridge_source';

        PERFORM public.monthly_distribute_cascade(906021);
        RAISE EXCEPTION 'Expected missing bridge_source binding to fail';
    EXCEPTION WHEN OTHERS THEN
        IF POSITION('must define bridge_source binding' IN SQLERRM) = 0 THEN
            RAISE;
        END IF;
    END;

    BEGIN
        DELETE FROM public.allocation_nodes
        WHERE user_id = 906021
          AND slug = 'free_to_gifts';

        PERFORM public.monthly_distribute_cascade(906021);
        RAISE EXCEPTION 'Expected missing free_to_gifts root to fail';
    EXCEPTION WHEN OTHERS THEN
        IF POSITION('free_to_gifts allocation root is required' IN SQLERRM) = 0 THEN
            RAISE;
        END IF;
    END;
END $$;

ROLLBACK;
