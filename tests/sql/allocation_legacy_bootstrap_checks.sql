-- checks for bootstrap_allocation_ledger_from_legacy()
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/allocation_legacy_bootstrap_checks.sql

BEGIN;

DELETE FROM allocation_postings WHERE user_id = 908001;
DELETE FROM allocation_node_groups
WHERE node_id IN (
    SELECT id
    FROM allocation_nodes
    WHERE user_id = 908001
       OR legacy_category_id IN (908011, 908012)
);
DELETE FROM allocation_nodes WHERE user_id = 908001 OR legacy_category_id IN (908011, 908012);
DELETE FROM cash_flow WHERE users_id = 908001;
DELETE FROM categories_category_groups WHERE users_id = 908001;
DELETE FROM categories WHERE id IN (908011, 908012);
DELETE FROM users WHERE id = 908001;

INSERT INTO users(id, nickname) VALUES (908001, 'bootu');
INSERT INTO categories(id, "name", "percent") VALUES
    (908011, 'Boot Income', 0.00),
    (908012, 'Boot Spend', 0.00);
INSERT INTO category_groups(id, "name", description) VALUES
    (13, 'income_group', 'bootstrap income fixture'),
    (6, 'free_group', 'bootstrap spend fixture')
ON CONFLICT (id) DO NOTHING;
SELECT setval(
    pg_get_serial_sequence('public.categories_category_groups', 'id'),
    COALESCE((SELECT max(id) FROM public.categories_category_groups), 1),
    true
);
INSERT INTO categories_category_groups(categories_id, category_groyps_id, users_id) VALUES
    (908011, 13, 908001),
    (908012, 6, 908001);

INSERT INTO cash_flow(users_id, "datetime", category_id_to, value, currency, description) VALUES
    (908001, now(), 908011, 100::numeric, 'RUB', 'bootstrap revenue');
INSERT INTO cash_flow(users_id, "datetime", category_id_from, value, currency, description) VALUES
    (908001, now() + interval '1 second', 908012, 40::numeric, 'RUB', 'bootstrap spend');
INSERT INTO cash_flow(users_id, "datetime", category_id_from, value, currency, description) VALUES
    (908001, now() + interval '2 seconds', 908011, 30::numeric, 'USD', 'exchange to USD');
INSERT INTO cash_flow(users_id, "datetime", category_id_from, value, currency, description) VALUES
    (908001, now() + interval '3 seconds', 908012, 0::numeric, 'RUB', 'zero row');

SELECT public.ensure_monthly_allocation_nodes_from_legacy(ARRAY[908001]);
SELECT public.ensure_monthly_allocation_nodes_from_legacy(ARRAY[908001]);
SELECT public.bootstrap_allocation_ledger_from_legacy();
SELECT public.bootstrap_allocation_ledger_from_legacy();

DO $$
DECLARE
    monthly_leaf_count int;
    monthly_leaf_group_count int;
    compat_node_count int;
    node_group_count int;
    posting_count int;
    linked_ids int;
    exchange_count int;
    zero_row_count int;
BEGIN
    SELECT count(*)
    INTO monthly_leaf_count
    FROM allocation_nodes
    WHERE user_id = 908001
      AND slug IN ('cat_908011', 'cat_908012')
      AND active;

    IF monthly_leaf_count <> 2 THEN
        RAISE EXCEPTION 'Expected 2 active monthly leaf nodes without duplicates, got %', monthly_leaf_count;
    END IF;

    SELECT count(*)
    INTO monthly_leaf_group_count
    FROM allocation_node_groups ang
    JOIN allocation_nodes an
      ON an.id = ang.node_id
    WHERE an.user_id = 908001
      AND an.slug IN ('cat_908011', 'cat_908012')
      AND ang.legacy_group_id IN (13, 6)
      AND ang.active;

    IF monthly_leaf_group_count <> 2 THEN
        RAISE EXCEPTION 'Expected 2 active monthly leaf allocation_node_groups rows, got %', monthly_leaf_group_count;
    END IF;

    SELECT count(*)
    INTO compat_node_count
    FROM allocation_nodes
    WHERE user_id = 908001
      AND slug IN ('legacy_bridge_cat_908011', 'legacy_bridge_cat_908012')
      AND active;

    IF compat_node_count <> 0 THEN
        RAISE EXCEPTION 'Expected 0 compatibility nodes for canonical monthly categories, got %', compat_node_count;
    END IF;

    SELECT count(*)
    INTO node_group_count
    FROM allocation_node_groups ang
    JOIN allocation_nodes an
      ON an.id = ang.node_id
    WHERE an.user_id = 908001
      AND ang.legacy_group_id IN (13, 6)
      AND ang.active;

    IF node_group_count <> 2 THEN
        RAISE EXCEPTION 'Expected 2 allocation_node_groups rows, got %', node_group_count;
    END IF;

    SELECT count(*)
    INTO posting_count
    FROM allocation_postings
    WHERE user_id = 908001
      AND metadata->>'origin' = 'migration'
      AND metadata->>'backfill_kind' = 'cash_flow';

    IF posting_count <> 3 THEN
        RAISE EXCEPTION 'Expected 3 backfilled postings without duplicates, got %', posting_count;
    END IF;

    SELECT count(DISTINCT metadata->>'legacy_cash_flow_id')
    INTO linked_ids
    FROM allocation_postings
    WHERE user_id = 908001
      AND metadata ? 'legacy_cash_flow_id';

    IF linked_ids <> 3 THEN
        RAISE EXCEPTION 'Expected 3 distinct legacy_cash_flow_id links, got %', linked_ids;
    END IF;

    SELECT count(*)
    INTO exchange_count
    FROM allocation_postings
    WHERE user_id = 908001
      AND metadata->>'kind' = 'exchange'
      AND metadata->>'subkind' = 'manual'
      AND metadata->>'direction' = 'out';

    IF exchange_count <> 1 THEN
        RAISE EXCEPTION 'Expected 1 reclassified exchange posting, got %', exchange_count;
    END IF;

    SELECT count(*)
    INTO zero_row_count
    FROM allocation_postings
    WHERE user_id = 908001
      AND description = 'zero row';

    IF zero_row_count <> 0 THEN
        RAISE EXCEPTION 'Expected zero-value cash_flow row to be skipped, got %', zero_row_count;
    END IF;
END $$;

ROLLBACK;
