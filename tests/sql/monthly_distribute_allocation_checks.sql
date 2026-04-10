-- monthly_distribute_allocation checks
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/monthly_distribute_allocation_checks.sql

BEGIN;

DELETE FROM allocation_postings WHERE user_id IN (906011, 906012);
DELETE FROM cash_flow WHERE users_id IN (906011, 906012);

DELETE FROM allocation_routes
WHERE source_node_id IN (
    SELECT id FROM allocation_nodes WHERE slug LIKE 'test_monthly_%'
)
   OR target_node_id IN (
    SELECT id FROM allocation_nodes WHERE slug LIKE 'test_monthly_%'
);

DELETE FROM allocation_nodes
WHERE slug LIKE 'test_monthly_%';

DELETE FROM user_group_memberships WHERE user_id IN (906011, 906012);
DELETE FROM user_groups WHERE slug = 'test_monthly_group';
DELETE FROM categories WHERE id IN (906291, 906292, 906293, 906294, 906299);
DELETE FROM users WHERE id IN (906011, 906012);

INSERT INTO users(id, nickname) VALUES
    (906011, 'm1'),
    (906012, 'm2');

INSERT INTO categories(id, "name", "percent") VALUES
    (906291, 'monthly personal leaf', 0.00),
    (906292, 'monthly shared leaf', 0.00),
    (906293, 'monthly partner leaf', 0.00),
    (906294, 'monthly history bucket', 0.00),
    (906299, 'monthly source category', 0.00);

INSERT INTO exchange_rates("datetime", currency, rate)
VALUES (now(), 'RUB', 1);

INSERT INTO user_groups(slug, "name", description)
VALUES ('test_monthly_group', 'test monthly group', 'fixture');

INSERT INTO user_group_memberships(user_id, user_group_id)
SELECT 906011, id FROM user_groups WHERE slug = 'test_monthly_group';

INSERT INTO user_group_memberships(user_id, user_group_id)
SELECT 906012, id FROM user_groups WHERE slug = 'test_monthly_group';

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
    (906011, NULL, 'test_monthly_root', 'test monthly root', 'root node', 'technical', NULL, false, false, true),
    (906011, NULL, 'test_monthly_stage', 'test monthly stage', 'stage node', 'technical', NULL, false, true, true),
    (906011, NULL, 'test_monthly_source', 'test monthly source', 'source balance node', 'income', 906299, true, false, true),
    (906011, NULL, 'test_monthly_history', 'test monthly history', 'history bucket node', 'both', 906294, true, false, true),
    (906011, NULL, 'test_monthly_personal', 'test monthly personal', 'personal leaf', 'expense', 906291, true, true, true),
    (906012, NULL, 'test_monthly_partner', 'test monthly partner', 'partner leaf', 'expense', 906293, true, true, true);

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
    'test_monthly_common',
    'test monthly common',
    'shared leaf',
    'expense',
    906292,
    true,
    true,
    true
FROM user_groups
WHERE slug = 'test_monthly_group';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 0.40, 'root to stage'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906011
 AND dst.slug = 'test_monthly_stage'
WHERE src.user_id = 906011
  AND src.slug = 'test_monthly_root';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'root remainder to common'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.slug = 'test_monthly_common'
WHERE src.user_id = 906011
  AND src.slug = 'test_monthly_root';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 0.25, 'stage to personal'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906011
 AND dst.slug = 'test_monthly_personal'
WHERE src.user_id = 906011
  AND src.slug = 'test_monthly_stage';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 0.25, 'stage to partner'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906012
 AND dst.slug = 'test_monthly_partner'
WHERE src.user_id = 906011
  AND src.slug = 'test_monthly_stage';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'stage remainder to common'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.slug = 'test_monthly_common'
WHERE src.user_id = 906011
  AND src.slug = 'test_monthly_stage';

INSERT INTO allocation_postings(user_id, to_node_id, value, currency, description, metadata)
SELECT
    906011,
    id,
    100,
    'RUB',
    'fixture source balance',
    jsonb_build_object('kind', 'fixture')
FROM allocation_nodes
WHERE user_id = 906011
  AND slug = 'test_monthly_source';

INSERT INTO allocation_postings(user_id, datetime, to_node_id, value, currency, description, metadata)
SELECT
    906011,
    date_trunc('month', now()) - INTERVAL '15 days',
    id,
    70,
    'RUB',
    'fixture previous earning',
    jsonb_build_object('kind', 'fixture')
FROM allocation_nodes
WHERE user_id = 906011
  AND slug = 'test_monthly_history';

INSERT INTO allocation_postings(user_id, datetime, from_node_id, value, currency, description, metadata)
SELECT
    906011,
    date_trunc('month', now()) - INTERVAL '10 days',
    id,
    30,
    'RUB',
    'fixture previous spend',
    jsonb_build_object('kind', 'fixture')
FROM allocation_nodes
WHERE user_id = 906011
  AND slug = 'test_monthly_history';

DO $$
DECLARE
    root_id bigint;
    source_node_id bigint;
    out jsonb;
    stage_amount numeric;
    personal_amount numeric;
    partner_amount numeric;
    common_amount numeric;
    posted_personal numeric;
    posted_partner numeric;
    posted_common numeric;
    source_balance numeric;
    common_owner_user_id bigint;
    posted_rows integer;
    linked_legacy_rows integer;
BEGIN
    SELECT id
    INTO root_id
    FROM allocation_nodes
    WHERE user_id = 906011
      AND slug = 'test_monthly_root';

    SELECT id
    INTO source_node_id
    FROM allocation_nodes
    WHERE user_id = 906011
      AND slug = 'test_monthly_source';

    out := public.monthly_distribute_allocation(
        906011,
        root_id,
        NULL::integer,
        'RUB',
        'monthly distribute allocation test',
        source_node_id
    );

    IF (out ->> 'source_category_node_id')::bigint <> source_node_id THEN
        RAISE EXCEPTION 'Expected source_category_node_id %, got %', source_node_id, out ->> 'source_category_node_id';
    END IF;

    IF (out ->> 'source_category_id')::integer <> 906299 THEN
        RAISE EXCEPTION 'Expected source_category_id 906299, got %', out ->> 'source_category_id';
    END IF;

    IF abs((out ->> 'source_amount')::numeric - 100) > 1e-9 THEN
        RAISE EXCEPTION 'Expected source_amount 100, got %', out ->> 'source_amount';
    END IF;

    IF abs((out ->> 'month_earnings')::numeric - 70) > 1e-9 THEN
        RAISE EXCEPTION 'Expected month_earnings 70, got %', out ->> 'month_earnings';
    END IF;

    IF abs((out ->> 'month_spend')::numeric - 30) > 1e-9 THEN
        RAISE EXCEPTION 'Expected month_spend 30, got %', out ->> 'month_spend';
    END IF;

    IF jsonb_array_length(out -> 'report') <> 4 THEN
        RAISE EXCEPTION 'Expected 4 report rows, got %', jsonb_array_length(out -> 'report');
    END IF;

    SELECT
        MAX(CASE WHEN report_row.value ->> 'slug' = 'test_monthly_stage' THEN (report_row.value ->> 'amount')::numeric END),
        MAX(CASE WHEN report_row.value ->> 'slug' = 'test_monthly_personal' THEN (report_row.value ->> 'amount')::numeric END),
        MAX(CASE WHEN report_row.value ->> 'slug' = 'test_monthly_partner' THEN (report_row.value ->> 'amount')::numeric END),
        MAX(CASE WHEN report_row.value ->> 'slug' = 'test_monthly_common' THEN (report_row.value ->> 'amount')::numeric END),
        MAX(CASE WHEN report_row.value ->> 'slug' = 'test_monthly_common' THEN (report_row.value ->> 'owner_user_id')::bigint END)
    INTO
        stage_amount,
        personal_amount,
        partner_amount,
        common_amount,
        common_owner_user_id
    FROM jsonb_array_elements(out -> 'report') AS report_row(value);

    IF abs(stage_amount - 40) > 1e-9 THEN
        RAISE EXCEPTION 'Expected stage report amount 40, got %', stage_amount;
    END IF;

    IF abs(personal_amount - 10) > 1e-9 THEN
        RAISE EXCEPTION 'Expected personal report amount 10, got %', personal_amount;
    END IF;

    IF abs(partner_amount - 10) > 1e-9 THEN
        RAISE EXCEPTION 'Expected partner report amount 10, got %', partner_amount;
    END IF;

    IF abs(common_amount - 80) > 1e-9 THEN
        RAISE EXCEPTION 'Expected common report amount 80, got %', common_amount;
    END IF;

    IF common_owner_user_id <> 906011 THEN
        RAISE EXCEPTION 'Expected shared common report row to carry current branch owner_user_id 906011, got %', common_owner_user_id;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO posted_personal
    FROM allocation_postings
    WHERE user_id = 906011
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE slug = 'test_monthly_personal'
      )
      AND description = 'monthly distribute allocation test';

    IF abs(posted_personal - 10) > 1e-9 THEN
        RAISE EXCEPTION 'Expected posted personal amount 10, got %', posted_personal;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO posted_partner
    FROM allocation_postings
    WHERE user_id = 906012
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE slug = 'test_monthly_partner'
      )
      AND description = 'monthly distribute allocation test';

    IF abs(posted_partner - 10) > 1e-9 THEN
        RAISE EXCEPTION 'Expected posted partner amount 10, got %', posted_partner;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO posted_common
    FROM allocation_postings
    WHERE user_id = 906011
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE slug = 'test_monthly_common'
      )
      AND description = 'monthly distribute allocation test';

    IF abs(posted_common - 80) > 1e-9 THEN
        RAISE EXCEPTION 'Expected posted common amount 80, got %', posted_common;
    END IF;

    SELECT public.get_category_balance_v2(906011, 906299, 'RUB')
    INTO source_balance;

    IF abs(source_balance) > 1e-9 THEN
        RAISE EXCEPTION 'Expected source category balance 0 after monthly allocation debit, got %', source_balance;
    END IF;

    SELECT COUNT(*)
    INTO posted_rows
    FROM allocation_postings
    WHERE user_id IN (906011, 906012)
      AND description = 'monthly distribute allocation test'
      AND from_node_id = source_node_id;

    IF posted_rows <> 4 THEN
        RAISE EXCEPTION 'Expected 4 monthly allocation rows debited from explicit source node, got %', posted_rows;
    END IF;

    SELECT COUNT(*)
    INTO posted_rows
    FROM allocation_postings
    WHERE user_id = 906011
      AND description = 'monthly distribute allocation test';

    IF posted_rows <> 3 THEN
        RAISE EXCEPTION 'Expected 3 inserted ledger rows for executor user, got %', posted_rows;
    END IF;

    SELECT COUNT(*)
    INTO posted_rows
    FROM allocation_postings
    WHERE user_id = 906012
      AND description = 'monthly distribute allocation test';

    IF posted_rows <> 1 THEN
        RAISE EXCEPTION 'Expected 1 inserted ledger row for partner user, got %', posted_rows;
    END IF;

    SELECT COUNT(*)
    INTO linked_legacy_rows
    FROM allocation_postings
    WHERE user_id IN (906011, 906012)
      AND description = 'monthly distribute allocation test'
      AND metadata ? 'legacy_cash_flow_id';

    IF linked_legacy_rows <> 0 THEN
        RAISE EXCEPTION 'Expected no legacy_cash_flow_id for ledger-only monthly allocation rows, got %', linked_legacy_rows;
    END IF;

    SELECT COUNT(*)
    INTO posted_rows
    FROM cash_flow
    WHERE users_id IN (906011, 906012)
      AND description = 'monthly distribute allocation test';

    IF posted_rows <> 0 THEN
        RAISE EXCEPTION 'Expected no cash_flow rows for ledger-only monthly allocation, got %', posted_rows;
    END IF;
END $$;

ROLLBACK;
