-- negative checks for insert_spend_with_exchange_v2 preconditions
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/spend_with_exchange_negative_checks.sql

BEGIN;

DELETE FROM allocation_postings WHERE user_id = 903001;
DELETE FROM allocation_node_groups
WHERE node_id IN (
    SELECT id
    FROM allocation_nodes
    WHERE user_id = 903001 OR legacy_category_id IN (903011, 903012)
);
DELETE FROM allocation_nodes WHERE user_id = 903001 OR legacy_category_id IN (903011, 903012);
DELETE FROM cash_flow WHERE users_id = 903001;
DELETE FROM users WHERE id = 903001;
DELETE FROM categories WHERE id IN (903011, 903012);
DELETE FROM category_groups WHERE id IN (9, 14);
DELETE FROM exchange_rates WHERE currency IN ('USD', 'RUB', 'USDT');

INSERT INTO users(id, nickname) VALUES (903001, 'neg_fx');

INSERT INTO category_groups(id, "name", description) VALUES
  (9, 'reserve_group', ''),
  (14, 'all_group', '');

INSERT INTO categories(id, "name", "percent") VALUES
  (903011, 'ReserveNeg', 0.00),
  (903012, 'SpendNeg', 0.00);

DO $$
BEGIN
    -- 1) Missing rates should fail
    BEGIN
        PERFORM public.insert_spend_with_exchange_v2(903001, 'SpendNeg', 10::numeric, 'USDT', 'neg1');
        RAISE EXCEPTION 'Expected failure for missing rates';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('Exchange rates for' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;

    -- Prepare rates for next checks
    WITH ts AS (SELECT now() AS t)
    INSERT INTO exchange_rates("datetime", currency, rate)
    SELECT t, 'USD', 1 FROM ts
    UNION ALL
    SELECT t, 'RUB', 80 FROM ts
    UNION ALL
    SELECT t, 'USDT', 1 FROM ts;

    -- 2) Missing reserve category (group 9) should fail
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
    VALUES (
        903022,
        903001,
        'spend_neg',
        'SpendNeg',
        'test spend node',
        'expense',
        903012,
        true,
        true,
        true
    );

    INSERT INTO allocation_node_groups(node_id, legacy_group_id, active)
    VALUES (903022, 14, true);

    BEGIN
        PERFORM public.insert_spend_with_exchange_v2(903001, 'SpendNeg', 10::numeric, 'USDT', 'neg2');
        RAISE EXCEPTION 'Expected failure for missing reserve category';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('Reserve allocation category node' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;

    -- 3) Missing spend category in group 14 should fail
    INSERT INTO allocation_nodes(id, user_id, slug, "name", description, node_kind, legacy_category_id, visible, include_in_report, active)
    VALUES (903021, 903001, 'reserve_neg', 'ReserveNeg', 'test reserve node', 'expense', 903011, true, true, true);
    INSERT INTO allocation_node_groups(node_id, legacy_group_id, active)
    VALUES (903021, 9, true);

    DELETE FROM allocation_node_groups
    WHERE node_id = 903022
      AND legacy_group_id = 14;

    BEGIN
        PERFORM public.insert_spend_with_exchange_v2(903001, 'UnknownCategory', 10::numeric, 'USDT', 'neg3');
        RAISE EXCEPTION 'Expected failure for missing category in group 14';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('in group 14 not found' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;

    -- 4) Non-positive value should fail
    BEGIN
        PERFORM public.insert_spend_with_exchange_v2(903001, 'SpendNeg', 0::numeric, 'USDT', 'neg4');
        RAISE EXCEPTION 'Expected failure for non-positive spend value';
    EXCEPTION
        WHEN OTHERS THEN
            IF POSITION('must be greater than zero' IN SQLERRM) = 0 THEN
                RAISE;
            END IF;
    END;

    IF (SELECT count(*) FROM allocation_postings WHERE user_id = 903001) <> 0 THEN
        RAISE EXCEPTION 'Failed auto exchange spend should not create ledger rows';
    END IF;

    IF (SELECT count(*) FROM cash_flow WHERE users_id = 903001) <> 0 THEN
        RAISE EXCEPTION 'Failed auto exchange spend should not create cash_flow rows';
    END IF;
END $$;

ROLLBACK;
