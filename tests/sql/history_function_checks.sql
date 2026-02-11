-- History function checks (format + return type)
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/history_function_checks.sql

BEGIN;

DELETE FROM cash_flow WHERE users_id = 910001;
DELETE FROM categories WHERE id IN (910001, 910002);
DELETE FROM users WHERE id = 910001;

INSERT INTO users(id, nickname) VALUES (910001, 'histuser');
INSERT INTO categories(id, "name", "percent") VALUES
    (910001, 'CatFrom', 0.00),
    (910002, 'CatTo', 0.00);

INSERT INTO cash_flow(users_id, "datetime", category_id_from, category_id_to, value, currency, description)
VALUES
    (910001, now(), 910001, 910002, 123.456::numeric, 'USD', 'history test large'),
    (910001, now() + interval '1 second', 910001, 910002, 0.0001234500::numeric, 'USD', 'history test small');

DO $$
DECLARE v text;
DECLARE t text;
BEGIN
    SELECT value, pg_typeof(value)::text
    INTO v, t
    FROM get_last_transaction(910001, 1)
    LIMIT 1;

    IF t <> 'character varying' THEN
        RAISE EXCEPTION 'Expected value type character varying, got %', t;
    END IF;

    IF v <> '0.00012345' THEN
        RAISE EXCEPTION 'Expected formatted value 0.00012345, got %', v;
    END IF;

    SELECT value, pg_typeof(value)::text
    INTO v, t
    FROM get_last_transaction(910001, 2)
    LIMIT 1;

    IF t <> 'character varying' THEN
        RAISE EXCEPTION 'Expected value type character varying, got %', t;
    END IF;

    IF v <> '123.46' THEN
        RAISE EXCEPTION 'Expected formatted value 123.46, got %', v;
    END IF;
END $$;

ROLLBACK;
