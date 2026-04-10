-- allocation cascade distribution checks
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/allocation_cascade_checks.sql

BEGIN;

DELETE FROM allocation_postings WHERE user_id IN (906001, 906002);
DELETE FROM cash_flow WHERE users_id IN (906001, 906002);

DELETE FROM allocation_routes
WHERE source_node_id IN (
    SELECT id FROM allocation_nodes WHERE user_id IN (906001, 906002) OR slug LIKE 'test_cascade_%'
)
   OR target_node_id IN (
    SELECT id FROM allocation_nodes WHERE user_id IN (906001, 906002) OR slug LIKE 'test_cascade_%'
);

DELETE FROM allocation_nodes
WHERE user_id IN (906001, 906002)
   OR slug LIKE 'test_cascade_%';

DELETE FROM user_group_memberships WHERE user_id IN (906001, 906002);
DELETE FROM user_groups WHERE slug = 'test_cascade_group';
DELETE FROM categories WHERE id IN (906101, 906102, 906103, 906104, 906199);
DELETE FROM users WHERE id IN (906001, 906002);

INSERT INTO users(id, nickname) VALUES
    (906001, 'u1'),
    (906002, 'u2');

INSERT INTO categories(id, "name", "percent") VALUES
    (906101, 'test personal leaf', 0.00),
    (906102, 'test shared leaf', 0.00),
    (906103, 'test partner leaf', 0.00),
    (906104, 'test family source leaf', 0.00),
    (906199, 'test source category', 0.00);

INSERT INTO user_groups(slug, "name", description)
VALUES ('test_cascade_group', 'test cascade group', 'fixture');

INSERT INTO user_group_memberships(user_id, user_group_id)
SELECT 906001, id FROM user_groups WHERE slug = 'test_cascade_group';

INSERT INTO user_group_memberships(user_id, user_group_id)
SELECT 906002, id FROM user_groups WHERE slug = 'test_cascade_group';

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
    (906001, NULL, 'test_cascade_root', 'test root', 'root node', 'technical', NULL, false, false, true),
    (906001, NULL, 'test_cascade_stage', 'test stage', 'stage node', 'technical', NULL, false, true, true),
    (906001, NULL, 'test_cascade_empty_root', 'test empty root', 'route-less technical node', 'technical', NULL, false, false, true),
    (906001, NULL, 'test_cascade_family_root', 'test family root', 'family bridge root', 'technical', NULL, false, false, true),
    (906001, NULL, 'test_cascade_native_root', 'test native root', 'native leaf root', 'technical', NULL, false, false, true),
    (906001, NULL, 'test_cascade_source', 'test source', 'source balance node', 'income', 906199, true, false, true),
    (906001, NULL, 'test_cascade_native_leaf', 'test native leaf', 'graph-native leaf without legacy category', 'expense', NULL, true, true, true),
    (906001, NULL, 'test_cascade_personal', 'test personal', 'personal leaf', 'expense', 906101, true, true, true),
    (906002, NULL, 'family_contribution_in', 'test family in', 'family bridge target', 'technical', NULL, false, false, true),
    (906002, NULL, 'test_cascade_family_source', 'test family source', 'family source leaf', 'expense', 906104, true, false, true),
    (906002, NULL, 'test_cascade_partner', 'test partner', 'partner leaf', 'expense', 906103, true, true, true);

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
    'test_cascade_common',
    'test common',
    'shared leaf',
    'expense',
    906102,
    true,
    true,
    true
FROM user_groups
WHERE slug = 'test_cascade_group';

UPDATE allocation_nodes
SET metadata = jsonb_build_object('partner_source_category_slug', 'test_cascade_family_source')
WHERE user_id = 906001
  AND slug = 'test_cascade_family_root';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 0.40, 'root to stage'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906001
 AND dst.slug = 'test_cascade_stage'
WHERE src.user_id = 906001
  AND src.slug = 'test_cascade_root';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'root remainder to common'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.slug = 'test_cascade_common'
WHERE src.user_id = 906001
  AND src.slug = 'test_cascade_root';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 0.25, 'stage to personal'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906001
 AND dst.slug = 'test_cascade_personal'
WHERE src.user_id = 906001
  AND src.slug = 'test_cascade_stage';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 0.25, 'stage to partner'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906002
 AND dst.slug = 'test_cascade_partner'
WHERE src.user_id = 906001
  AND src.slug = 'test_cascade_stage';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'stage remainder to common'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.slug = 'test_cascade_common'
WHERE src.user_id = 906001
  AND src.slug = 'test_cascade_stage';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT
    src.id,
    dst.id,
    1,
    'family bridge route'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906002
 AND dst.slug = 'family_contribution_in'
WHERE src.user_id = 906001
  AND src.slug = 'test_cascade_family_root';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'family bridge to partner leaf'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906002
 AND dst.slug = 'test_cascade_partner'
WHERE src.user_id = 906002
  AND src.slug = 'family_contribution_in';

INSERT INTO allocation_routes(source_node_id, target_node_id, percent, description)
SELECT src.id, dst.id, 1, 'native root to native leaf'
FROM allocation_nodes src
JOIN allocation_nodes dst
  ON dst.user_id = 906001
 AND dst.slug = 'test_cascade_native_leaf'
WHERE src.user_id = 906001
  AND src.slug = 'test_cascade_native_root';

INSERT INTO allocation_postings(user_id, to_node_id, value, currency, description, metadata)
SELECT
    906001,
    id,
    100,
    'RUB',
    'fixture source balance',
    jsonb_build_object('kind', 'fixture')
FROM allocation_nodes
WHERE user_id = 906001
  AND slug = 'test_cascade_source';

DO $$
DECLARE
    root_id bigint;
    source_node_id bigint;
    stage_amount numeric;
    personal_amount numeric;
    partner_amount numeric;
    common_amount numeric;
    posted_personal numeric;
    posted_partner numeric;
    posted_common numeric;
    source_balance numeric;
    posted_rows integer;
    linked_legacy_rows integer;
    family_root_id bigint;
    family_source_node_id bigint;
    family_source_rows integer;
    native_root_id bigint;
    native_leaf_id bigint;
    native_rows integer;
    native_legacy_metadata_rows integer;
BEGIN
    SELECT id
    INTO root_id
    FROM allocation_nodes
    WHERE user_id = 906001
      AND slug = 'test_cascade_root';

    SELECT id
    INTO source_node_id
    FROM allocation_nodes
    WHERE user_id = 906001
      AND slug = 'test_cascade_source';

    WITH result AS (
        SELECT *
        FROM public.allocation_distribute(
            906001,
            root_id,
            100::numeric,
            'RUB',
            906199,
            'test cascade',
            source_node_id
        )
    )
    SELECT
        MAX(CASE WHEN report_node_slug = 'test_cascade_stage' THEN report_amount END),
        MAX(CASE WHEN report_node_slug = 'test_cascade_personal' THEN report_amount END),
        MAX(CASE WHEN report_node_slug = 'test_cascade_partner' THEN report_amount END),
        MAX(CASE WHEN report_node_slug = 'test_cascade_common' THEN report_amount END)
    INTO
        stage_amount,
        personal_amount,
        partner_amount,
        common_amount
    FROM result;

    IF abs(stage_amount - 40) > 1e-9 THEN
        RAISE EXCEPTION 'Expected stage report amount 40, got %', stage_amount;
    END IF;

    IF abs(personal_amount - 10) > 1e-9 THEN
        RAISE EXCEPTION 'Expected personal leaf report amount 10, got %', personal_amount;
    END IF;

    IF abs(partner_amount - 10) > 1e-9 THEN
        RAISE EXCEPTION 'Expected partner leaf report amount 10, got %', partner_amount;
    END IF;

    IF abs(common_amount - 80) > 1e-9 THEN
        RAISE EXCEPTION 'Expected common leaf report amount 80, got %', common_amount;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO posted_personal
    FROM allocation_postings
    WHERE user_id = 906001
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE slug = 'test_cascade_personal'
      )
      AND description = 'test cascade';

    IF abs(posted_personal - 10) > 1e-9 THEN
        RAISE EXCEPTION 'Expected posted personal amount 10, got %', posted_personal;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO posted_partner
    FROM allocation_postings
    WHERE user_id = 906002
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE slug = 'test_cascade_partner'
      )
      AND description = 'test cascade';

    IF abs(posted_partner - 10) > 1e-9 THEN
        RAISE EXCEPTION 'Expected posted partner amount 10, got %', posted_partner;
    END IF;

    SELECT COALESCE(SUM(value), 0)
    INTO posted_common
    FROM allocation_postings
    WHERE user_id = 906001
      AND to_node_id = (
          SELECT id
          FROM allocation_nodes
          WHERE slug = 'test_cascade_common'
      )
      AND description = 'test cascade';

    IF abs(posted_common - 80) > 1e-9 THEN
        RAISE EXCEPTION 'Expected posted common amount 80, got %', posted_common;
    END IF;

    SELECT public.get_category_balance_v2(906001, 906199, 'RUB')
    INTO source_balance;

    IF abs(source_balance) > 1e-9 THEN
        RAISE EXCEPTION 'Expected source category balance 0 after allocation debit, got %', source_balance;
    END IF;

    SELECT id
    INTO family_root_id
    FROM allocation_nodes
    WHERE user_id = 906001
      AND slug = 'test_cascade_family_root';

    SELECT id
    INTO family_source_node_id
    FROM allocation_nodes
    WHERE user_id = 906002
      AND slug = 'test_cascade_family_source';

    PERFORM *
    FROM public.allocation_distribute(
        906001,
        family_root_id,
        25::numeric,
        'RUB',
        906199,
        'test family bridge',
        source_node_id
    );

    SELECT COUNT(*)
    INTO family_source_rows
    FROM allocation_postings
    WHERE user_id = 906002
      AND description = 'test family bridge'
      AND from_node_id = family_source_node_id;

    IF family_source_rows <> 1 THEN
        RAISE EXCEPTION 'Expected family bridge row debited from source node metadata config, got %', family_source_rows;
    END IF;

    SELECT id
    INTO native_root_id
    FROM allocation_nodes
    WHERE user_id = 906001
      AND slug = 'test_cascade_native_root';

    SELECT id
    INTO native_leaf_id
    FROM allocation_nodes
    WHERE user_id = 906001
      AND slug = 'test_cascade_native_leaf';

    PERFORM *
    FROM public.allocation_distribute(
        906001,
        native_root_id,
        7::numeric,
        'RUB',
        NULL::integer,
        'test native leaf',
        source_node_id
    );

    SELECT COUNT(*)
    INTO native_rows
    FROM allocation_postings
    WHERE user_id = 906001
      AND description = 'test native leaf'
      AND from_node_id = source_node_id
      AND to_node_id = native_leaf_id;

    IF native_rows <> 1 THEN
        RAISE EXCEPTION 'Expected graph-native leaf without legacy_category_id to create one posting, got %', native_rows;
    END IF;

    SELECT COUNT(*)
    INTO native_legacy_metadata_rows
    FROM allocation_postings
    WHERE user_id = 906001
      AND description = 'test native leaf'
      AND metadata ? 'legacy_category_id_to';

    IF native_legacy_metadata_rows <> 0 THEN
        RAISE EXCEPTION 'Expected graph-native leaf posting without legacy_category_id_to metadata, got %', native_legacy_metadata_rows;
    END IF;

    SELECT COUNT(*)
    INTO posted_rows
    FROM allocation_postings
    WHERE user_id IN (906001, 906002)
      AND description = 'test cascade'
      AND from_node_id = source_node_id;

    IF posted_rows <> 4 THEN
        RAISE EXCEPTION 'Expected 4 allocation rows debited from explicit source node, got %', posted_rows;
    END IF;

    BEGIN
        PERFORM *
        FROM public.allocation_distribute(
            906001,
            (
                SELECT id
                FROM allocation_nodes
                WHERE user_id = 906001
                  AND slug = 'test_cascade_empty_root'
            ),
            1::numeric,
            'RUB',
            906199,
            'test technical leaf guard'
        );
        RAISE EXCEPTION 'Expected route-less technical node to fail';
    EXCEPTION WHEN OTHERS THEN
        IF POSITION('technical leaf node' IN SQLERRM) = 0 THEN
            RAISE;
        END IF;
    END;

    SELECT COUNT(*)
    INTO posted_rows
    FROM allocation_postings
    WHERE user_id = 906001
      AND description = 'test cascade';

    IF posted_rows <> 3 THEN
        RAISE EXCEPTION 'Expected 3 inserted ledger rows for executor user, got %', posted_rows;
    END IF;

    SELECT COUNT(*)
    INTO posted_rows
    FROM allocation_postings
    WHERE user_id = 906002
      AND description = 'test cascade';

    IF posted_rows <> 1 THEN
        RAISE EXCEPTION 'Expected 1 inserted ledger row for partner user, got %', posted_rows;
    END IF;

    SELECT COUNT(*)
    INTO linked_legacy_rows
    FROM allocation_postings
    WHERE user_id IN (906001, 906002)
      AND description = 'test cascade'
      AND metadata ? 'legacy_cash_flow_id';

    IF linked_legacy_rows <> 0 THEN
        RAISE EXCEPTION 'Expected no legacy_cash_flow_id for ledger-only allocation rows, got %', linked_legacy_rows;
    END IF;

    SELECT COUNT(*)
    INTO posted_rows
    FROM cash_flow
    WHERE users_id IN (906001, 906002)
      AND description = 'test cascade';

    IF posted_rows <> 0 THEN
        RAISE EXCEPTION 'Expected no cash_flow rows for ledger-only allocation, got %', posted_rows;
    END IF;
END $$;

ROLLBACK;
