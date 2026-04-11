-- allocation scenarios schema checks
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/allocation_scenarios_schema_checks.sql

BEGIN;

DELETE FROM public.allocation_scenario_root_params
WHERE scenario_id IN (
    SELECT id
    FROM public.allocation_scenarios
    WHERE slug LIKE 'test_scenario_%'
);

DELETE FROM public.allocation_scenario_node_bindings
WHERE scenario_id IN (
    SELECT id
    FROM public.allocation_scenarios
    WHERE slug LIKE 'test_scenario_%'
);

DELETE FROM public.allocation_scenarios
WHERE slug LIKE 'test_scenario_%';

DELETE FROM public.allocation_nodes
WHERE user_id IN (908001, 908002)
   OR slug LIKE 'test_scenario_%';

DELETE FROM public.user_group_memberships
WHERE user_id IN (908001, 908002);

DELETE FROM public.user_groups
WHERE slug = 'test_scenario_group';

DELETE FROM public.users
WHERE id IN (908001, 908002);

INSERT INTO public.users(id, nickname) VALUES
    (908001, 'scn1'),
    (908002, 'scn2');

INSERT INTO public.user_groups(slug, "name", description)
VALUES ('test_scenario_group', 'test scenario group', 'fixture');

INSERT INTO public.user_group_memberships(user_id, user_group_id)
SELECT 908001, id FROM public.user_groups WHERE slug = 'test_scenario_group';

INSERT INTO public.user_group_memberships(user_id, user_group_id)
SELECT 908002, id FROM public.user_groups WHERE slug = 'test_scenario_group';

INSERT INTO public.allocation_nodes(
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
    (908001, 'test_scenario_root', 'test scenario root', 'fixture root', 'technical', NULL, false, false, true),
    (908001, 'test_scenario_leaf', 'test scenario leaf', 'fixture leaf', 'expense', NULL, true, false, true);

DO $$
DECLARE
    user_scenario_id bigint;
    group_scenario_id bigint;
    root_node_id bigint;
    leaf_node_id bigint;
    binding_rows integer;
    param_rows integer;
BEGIN
    INSERT INTO public.allocation_scenarios(
        owner_user_id,
        scenario_kind,
        schedule_cron,
        slug,
        "name",
        description,
        metadata
    )
    VALUES (
        908001,
        'monthly',
        '0 9 1 */2 *',
        'test_scenario_user',
        'test user scenario',
        'fixture',
        jsonb_build_object('scope', 'user')
    )
    RETURNING id INTO user_scenario_id;

    INSERT INTO public.allocation_scenarios(
        owner_user_group_id,
        scenario_kind,
        slug,
        "name",
        description,
        metadata
    )
    SELECT
        id,
        'monthly',
        'test_scenario_group',
        'test group scenario',
        'fixture',
        jsonb_build_object('scope', 'group')
    FROM public.user_groups
    WHERE slug = 'test_scenario_group'
    RETURNING id INTO group_scenario_id;

    BEGIN
        INSERT INTO public.allocation_scenarios(
            owner_user_id,
            owner_user_group_id,
            scenario_kind,
            slug,
            "name"
        )
        SELECT
            908001,
            id,
            'monthly',
            'test_scenario_invalid_owner',
            'invalid owner'
        FROM public.user_groups
        WHERE slug = 'test_scenario_group';

        RAISE EXCEPTION 'Expected allocation_scenarios owner XOR check to reject mixed owner row';
    EXCEPTION
        WHEN check_violation THEN
            NULL;
    END;

    BEGIN
        INSERT INTO public.allocation_scenarios(
            owner_user_id,
            scenario_kind,
            slug,
            "name"
        )
        VALUES (
            908001,
            'monthly',
            'test_scenario_user',
            'duplicate user scenario'
        );

        RAISE EXCEPTION 'Expected duplicate user-owned scenario slug to fail';
    EXCEPTION
        WHEN unique_violation THEN
            NULL;
    END;

    BEGIN
        INSERT INTO public.allocation_scenarios(
            owner_user_id,
            scenario_kind,
            schedule_cron,
            slug,
            "name"
        )
        VALUES (
            908001,
            'monthly',
            '',
            'test_scenario_empty_schedule',
            'empty schedule'
        );

        RAISE EXCEPTION 'Expected empty schedule_cron to fail';
    EXCEPTION
        WHEN check_violation THEN
            NULL;
    END;

    SELECT id
    INTO root_node_id
    FROM public.allocation_nodes
    WHERE user_id = 908001
      AND slug = 'test_scenario_root';

    SELECT id
    INTO leaf_node_id
    FROM public.allocation_nodes
    WHERE user_id = 908001
      AND slug = 'test_scenario_leaf';

    INSERT INTO public.allocation_scenario_node_bindings(
        scenario_id,
        root_node_id,
        binding_kind,
        bound_node_id,
        priority,
        metadata
    )
    VALUES
        (user_scenario_id, root_node_id, 'branch_source', leaf_node_id, 100, jsonb_build_object('question', 'where_to_start')),
        (user_scenario_id, root_node_id, 'root_target', leaf_node_id, 90, jsonb_build_object('question', 'single_target_root')),
        (group_scenario_id, root_node_id, 'bridge_source', leaf_node_id, 80, jsonb_build_object('question', 'partner_bridge_source'));

    SELECT COUNT(*)
    INTO binding_rows
    FROM public.allocation_scenario_node_bindings
    WHERE scenario_id IN (user_scenario_id, group_scenario_id);

    IF binding_rows <> 3 THEN
        RAISE EXCEPTION 'Expected 3 scenario bindings, got %', binding_rows;
    END IF;

    INSERT INTO public.allocation_scenario_root_params(
        scenario_id,
        root_node_id,
        param_key,
        param_value,
        metadata
    )
    VALUES
        (user_scenario_id, root_node_id, 'source_legacy_group_id', '11', jsonb_build_object('question', 'prep_source_group')),
        (group_scenario_id, root_node_id, 'spend_legacy_group_id', '8', jsonb_build_object('question', 'reserve_spend_group'));

    SELECT COUNT(*)
    INTO param_rows
    FROM public.allocation_scenario_root_params
    WHERE scenario_id IN (user_scenario_id, group_scenario_id);

    IF param_rows <> 2 THEN
        RAISE EXCEPTION 'Expected 2 scenario root params, got %', param_rows;
    END IF;

    BEGIN
        INSERT INTO public.allocation_scenario_node_bindings(
            scenario_id,
            root_node_id,
            binding_kind,
            bound_node_id
        )
        VALUES (
            user_scenario_id,
            root_node_id,
            'branch_source',
            leaf_node_id
        );

        RAISE EXCEPTION 'Expected duplicate scenario binding to fail';
    EXCEPTION
        WHEN unique_violation THEN
            NULL;
    END;

    BEGIN
        INSERT INTO public.allocation_scenario_root_params(
            scenario_id,
            root_node_id,
            param_key,
            param_value
        )
        VALUES (
            user_scenario_id,
            root_node_id,
            'source_legacy_group_id',
            '12'
        );

        RAISE EXCEPTION 'Expected duplicate scenario root param to fail';
    EXCEPTION
        WHEN unique_violation THEN
            NULL;
    END;
END $$;

ROLLBACK;
