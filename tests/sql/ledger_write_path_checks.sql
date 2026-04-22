-- checks that simple manual v2 writes are ledger-only
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/ledger_write_path_checks.sql

BEGIN;

DELETE FROM allocation_postings WHERE user_id = 907001;
DELETE FROM allocation_nodes WHERE user_id = 907001 OR legacy_category_id IN (907011, 907012);
DELETE FROM cash_flow WHERE users_id = 907001;
DELETE FROM categories WHERE id IN (907011, 907012);
DELETE FROM users WHERE id = 907001;

INSERT INTO users(id, nickname) VALUES (907001, 'ledger_u');
INSERT INTO categories(id, "name", "percent") VALUES
    (907011, 'Ledger Spend', 0.00),
    (907012, 'Ledger Revenue', 0.00);

INSERT INTO allocation_nodes(
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
VALUES
    (907001, 'ledger_spend', 'Ledger Spend', 'test spend node', 'expense', 907011, true, true, true),
    (907001, 'ledger_revenue', 'Ledger Revenue', 'test revenue node', 'income', 907012, true, true, true);

SELECT public.insert_spend_v2(907001, 'Ledger Spend', 10::numeric, 'RUB', 'ledger spend test');
SELECT public.insert_revenue_v2(907001, 'Ledger Revenue', 20::numeric, 'RUB', 'ledger revenue test');

DO $$
DECLARE
    ledger_rows int;
    linked_legacy_rows int;
    cash_flow_rows int;
BEGIN
    SELECT count(*)
    INTO ledger_rows
    FROM allocation_postings
    WHERE user_id = 907001
      AND metadata->>'kind' = 'transaction'
      AND metadata->>'subkind' IN ('spend', 'revenue');

    IF ledger_rows <> 2 THEN
        RAISE EXCEPTION 'Expected 2 ledger transaction rows, got %', ledger_rows;
    END IF;

    SELECT count(*)
    INTO linked_legacy_rows
    FROM allocation_postings
    WHERE user_id = 907001
      AND metadata ? 'legacy_cash_flow_id';

    IF linked_legacy_rows <> 0 THEN
        RAISE EXCEPTION 'Expected no legacy_cash_flow_id for ledger-only writes, got %', linked_legacy_rows;
    END IF;

    SELECT count(*)
    INTO cash_flow_rows
    FROM cash_flow
    WHERE users_id = 907001;

    IF cash_flow_rows <> 0 THEN
        RAISE EXCEPTION 'Expected no cash_flow rows for ledger-only writes, got %', cash_flow_rows;
    END IF;
END $$;

ROLLBACK;
