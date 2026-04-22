-- checks that delete_transaction() no longer mutates cash_flow in runtime
-- and uses tombstones to block backfill resurrection
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/delete_transaction_runtime_checks.sql

BEGIN;

DELETE FROM public.allocation_postings WHERE user_id = 909001;
DELETE FROM public.allocation_backfill_tombstones WHERE legacy_cash_flow_id IN (
    SELECT id FROM public.cash_flow WHERE users_id = 909001
);
DELETE FROM public.allocation_node_groups
WHERE node_id IN (
    SELECT id
    FROM public.allocation_nodes
    WHERE user_id = 909001
       OR legacy_category_id IN (909011, 909012)
);
DELETE FROM public.allocation_nodes WHERE user_id = 909001 OR legacy_category_id IN (909011, 909012);
DELETE FROM public.cash_flow WHERE users_id = 909001;
DELETE FROM public.categories_category_groups WHERE users_id = 909001;
DELETE FROM public.categories WHERE id IN (909011, 909012);
DELETE FROM public.users WHERE id = 909001;

INSERT INTO public.users(id, nickname) VALUES (909001, 'delu');
INSERT INTO public.categories(id, "name", "percent") VALUES
    (909011, 'Delete Income', 0.00),
    (909012, 'Delete Spend', 0.00);
SELECT setval(
    pg_get_serial_sequence('public.categories_category_groups', 'id'),
    COALESCE((SELECT max(id) FROM public.categories_category_groups), 1),
    true
);
INSERT INTO public.categories_category_groups(categories_id, category_groyps_id, users_id) VALUES
    (909011, 13, 909001),
    (909012, 6, 909001);

INSERT INTO public.cash_flow(users_id, "datetime", category_id_to, value, currency, description) VALUES
    (909001, now(), 909011, 100::numeric, 'RUB', 'delete bootstrap revenue');

SELECT public.bootstrap_allocation_ledger_from_legacy();

DO $$
DECLARE
    posting_id bigint;
    linked_legacy_cash_flow_id bigint;
    cash_flow_rows_before int;
    cash_flow_rows_after_delete int;
    posting_rows_after_delete int;
    tombstone_rows int;
    posting_rows_after_rebootstrap int;
BEGIN
    SELECT id
    INTO linked_legacy_cash_flow_id
    FROM public.cash_flow
    WHERE users_id = 909001
      AND description = 'delete bootstrap revenue'
    ORDER BY id DESC
    LIMIT 1;

    SELECT ap.id
    INTO posting_id
    FROM public.allocation_postings ap
    WHERE ap.user_id = 909001
      AND ap.metadata->>'legacy_cash_flow_id' = linked_legacy_cash_flow_id::text
    ORDER BY ap.id DESC
    LIMIT 1;

    IF posting_id IS NULL THEN
        RAISE EXCEPTION 'Expected bootstrap posting linked to legacy cash_flow row';
    END IF;

    SELECT count(*)
    INTO cash_flow_rows_before
    FROM public.cash_flow
    WHERE users_id = 909001;

    PERFORM public.delete_transaction(ARRAY[posting_id]);

    SELECT count(*)
    INTO cash_flow_rows_after_delete
    FROM public.cash_flow
    WHERE users_id = 909001;

    IF cash_flow_rows_after_delete <> cash_flow_rows_before THEN
        RAISE EXCEPTION 'delete_transaction() should not delete cash_flow rows, before=% after=%',
            cash_flow_rows_before,
            cash_flow_rows_after_delete;
    END IF;

    SELECT count(*)
    INTO posting_rows_after_delete
    FROM public.allocation_postings
    WHERE id = posting_id;

    IF posting_rows_after_delete <> 0 THEN
        RAISE EXCEPTION 'Expected allocation_posting % to be deleted', posting_id;
    END IF;

    SELECT count(*)
    INTO tombstone_rows
    FROM public.allocation_backfill_tombstones
    WHERE legacy_cash_flow_id = linked_legacy_cash_flow_id;

    IF tombstone_rows <> 1 THEN
        RAISE EXCEPTION 'Expected 1 tombstone row for legacy cash_flow %, got %',
            linked_legacy_cash_flow_id,
            tombstone_rows;
    END IF;

    PERFORM public.bootstrap_allocation_ledger_from_legacy();

    SELECT count(*)
    INTO posting_rows_after_rebootstrap
    FROM public.allocation_postings
    WHERE user_id = 909001
      AND metadata->>'legacy_cash_flow_id' = linked_legacy_cash_flow_id::text;

    IF posting_rows_after_rebootstrap <> 0 THEN
        RAISE EXCEPTION 'Tombstoned legacy cash_flow row % was resurrected by bootstrap', linked_legacy_cash_flow_id;
    END IF;
END $$;

ROLLBACK;
