-- allocation seed profiles schema checks
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/allocation_seed_profiles_schema_checks.sql

BEGIN;

DELETE FROM public.allocation_seed_profile_root_params
WHERE profile_id IN (
    SELECT id
    FROM public.allocation_seed_profiles
    WHERE slug LIKE 'test_seed_profile_%'
);

DELETE FROM public.allocation_seed_profile_bindings
WHERE profile_id IN (
    SELECT id
    FROM public.allocation_seed_profiles
    WHERE slug LIKE 'test_seed_profile_%'
);

DELETE FROM public.allocation_seed_profile_users
WHERE profile_id IN (
    SELECT id
    FROM public.allocation_seed_profiles
    WHERE slug LIKE 'test_seed_profile_%'
);

DELETE FROM public.allocation_seed_profiles
WHERE slug LIKE 'test_seed_profile_%';

DELETE FROM public.users
WHERE id IN (908101, 908102);

INSERT INTO public.users(id, nickname) VALUES
    (908101, 'seed1'),
    (908102, 'seed2');

DO $$
DECLARE
    _profile_id bigint;
    binding_rows integer;
    param_rows integer;
BEGIN
    INSERT INTO public.allocation_seed_profiles(
        profile_kind,
        slug,
        "name",
        description,
        shared_group_slug,
        shared_group_name,
        shared_group_description,
        metadata
    )
    VALUES (
        'monthly',
        'test_seed_profile_default',
        'test seed profile',
        'fixture',
        'test_seed_profile_group',
        'test seed group',
        'fixture',
        jsonb_build_object('scope', 'fixture')
    )
    RETURNING id INTO _profile_id;

    INSERT INTO public.allocation_seed_profile_users(
        profile_id,
        user_id,
        scenario_slug,
        scenario_name,
        scenario_description
    )
    VALUES
        (_profile_id, 908101, 'monthly_default', 'Monthly default', 'fixture'),
        (_profile_id, 908102, 'monthly_default', 'Monthly default', 'fixture');

    INSERT INTO public.allocation_seed_profile_bindings(
        profile_id,
        user_id,
        root_slug,
        binding_kind,
        bound_slug
    )
    VALUES
        (_profile_id, 908101, 'salary_primary', 'branch_source', 'cat_37'),
        (_profile_id, 908101, 'debt_reserve', 'root_target', 'cat_27');

    INSERT INTO public.allocation_seed_profile_root_params(
        profile_id,
        user_id,
        root_slug,
        param_key,
        param_value
    )
    VALUES
        (_profile_id, 908101, 'monthly_income_sources', 'source_legacy_group_id', '11'),
        (_profile_id, 908101, 'debt_reserve', 'spend_legacy_group_id', '8');

    SELECT COUNT(*)
    INTO binding_rows
    FROM public.allocation_seed_profile_bindings
    WHERE profile_id = _profile_id;

    IF binding_rows <> 2 THEN
        RAISE EXCEPTION 'Expected 2 seed profile bindings, got %', binding_rows;
    END IF;

    SELECT COUNT(*)
    INTO param_rows
    FROM public.allocation_seed_profile_root_params
    WHERE profile_id = _profile_id;

    IF param_rows <> 2 THEN
        RAISE EXCEPTION 'Expected 2 seed profile root params, got %', param_rows;
    END IF;

    BEGIN
        INSERT INTO public.allocation_seed_profiles(
            profile_kind,
            slug,
            "name",
            shared_group_slug,
            shared_group_name
        )
        VALUES (
            'monthly',
            'test_seed_profile_default',
            'duplicate',
            'dup_group',
            'dup group'
        );

        RAISE EXCEPTION 'Expected duplicate seed profile slug to fail';
    EXCEPTION
        WHEN unique_violation THEN
            NULL;
    END;

    BEGIN
        INSERT INTO public.allocation_seed_profile_users(
            profile_id,
            user_id,
            scenario_slug,
            scenario_name
        )
        VALUES (
            _profile_id,
            908101,
            'monthly_alt',
            'duplicate user'
        );

        RAISE EXCEPTION 'Expected duplicate seed profile user to fail';
    EXCEPTION
        WHEN unique_violation THEN
            NULL;
    END;

    BEGIN
        INSERT INTO public.allocation_seed_profile_root_params(
            profile_id,
            user_id,
            root_slug,
            param_key,
            param_value
        )
        VALUES (
            _profile_id,
            908101,
            'monthly_income_sources',
            'source_legacy_group_id',
            '12'
        );

        RAISE EXCEPTION 'Expected duplicate seed profile root param to fail';
    EXCEPTION
        WHEN unique_violation THEN
            NULL;
    END;
END $$;

ROLLBACK;
