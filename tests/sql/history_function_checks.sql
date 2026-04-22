-- History function checks (format + return type) against ledger-backed helper.
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/history_function_checks.sql

BEGIN;

DELETE FROM allocation_postings WHERE user_id = 910001;
DELETE FROM allocation_nodes WHERE user_id = 910001;
DELETE FROM users WHERE id = 910001;

INSERT INTO users(id, nickname) VALUES (910001, 'histuser');

INSERT INTO allocation_nodes(
    id,
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
    (9100011, 910001, 'hist_from', 'CatFrom', 'history source node', 'expense', NULL, true, true, true),
    (9100012, 910001, 'hist_to', 'CatTo', 'history target node', 'income', NULL, true, true, true);

INSERT INTO allocation_postings(
    user_id,
    "datetime",
    from_node_id,
    to_node_id,
    value,
    currency,
    description,
    metadata
)
VALUES
    (
        910001,
        now(),
        9100011,
        9100012,
        123.456::numeric,
        'USD',
        'history test large',
        jsonb_build_object('kind', 'fixture', 'origin', 'history_test')
    ),
    (
        910001,
        now() + interval '1 second',
        9100011,
        9100012,
        0.0001234500::numeric,
        'USD',
        'history test small',
        jsonb_build_object('kind', 'fixture', 'origin', 'history_test')
    );

DO $$
DECLARE
    v text;
    t text;
BEGIN
    SELECT value, pg_typeof(value)::text
    INTO v, t
    FROM get_last_transaction_v2(910001, 1)
    LIMIT 1;

    IF t <> 'character varying' THEN
        RAISE EXCEPTION 'Expected value type character varying, got %', t;
    END IF;

    IF v <> '0.00012345' THEN
        RAISE EXCEPTION 'Expected formatted value 0.00012345, got %', v;
    END IF;

    SELECT value, pg_typeof(value)::text
    INTO v, t
    FROM get_last_transaction_v2(910001, 2)
    LIMIT 1;

    IF t <> 'character varying' THEN
        RAISE EXCEPTION 'Expected value type character varying, got %', t;
    END IF;

    IF v <> '123.46' THEN
        RAISE EXCEPTION 'Expected formatted value 123.46, got %', v;
    END IF;
END $$;

ROLLBACK;
