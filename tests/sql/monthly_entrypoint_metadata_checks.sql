-- monthly entrypoint metadata checks
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/monthly_entrypoint_metadata_checks.sql

BEGIN;

DO $$
DECLARE
    has_node_metadata boolean;
    has_node_legacy_category boolean;
    cascade_callable_count integer;
    monthly_def text;
    cascade_def text;
    recursive_def text;
    distribute_def text;
    exchange_def text;
    monthly_allocation_helper_def text;
    binding_helper_def text;
    root_param_helper_def text;
    salary_source_helper_def text;
    reserve_helper_def text;
    report_json_helper_def text;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'allocation_nodes'
          AND column_name = 'metadata'
          AND data_type = 'jsonb'
    )
    INTO has_node_metadata;

    IF NOT has_node_metadata THEN
        RAISE EXCEPTION 'Expected allocation_nodes.metadata jsonb column';
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'allocation_nodes'
          AND column_name = 'legacy_category_id'
    )
    INTO has_node_legacy_category;

    IF NOT has_node_legacy_category THEN
        RAISE EXCEPTION 'Expected allocation_nodes.legacy_category_id column';
    END IF;

    SELECT count(*)
    INTO cascade_callable_count
    FROM pg_proc proc
    JOIN pg_namespace ns ON ns.oid = proc.pronamespace
    WHERE ns.nspname = 'public'
      AND proc.proname = 'monthly_distribute_cascade'
      AND pg_get_function_arguments(proc.oid) LIKE '_user_id bigint%';

    IF cascade_callable_count != 1 THEN
        RAISE EXCEPTION 'Expected exactly one callable monthly_distribute_cascade(_user_id bigint...) function, got %',
            cascade_callable_count;
    END IF;

    SELECT pg_get_functiondef('public.monthly()'::regprocedure)
    INTO monthly_def;

    IF POSITION('monthly_distribute_cascade(salary_root.user_id)' IN monthly_def) = 0
       OR POSITION('salary_root.slug = ''salary_primary''' IN monthly_def) = 0 THEN
        RAISE EXCEPTION 'Expected monthly() to discover monthly users from active salary_primary roots';
    END IF;

    IF POSITION('943915310' IN monthly_def) > 0
       OR POSITION('249716305' IN monthly_def) > 0
       OR POSITION(', 37' IN monthly_def) > 0
       OR POSITION(', 16' IN monthly_def) > 0 THEN
        RAISE EXCEPTION 'monthly() still hard-codes monthly users or legacy income category ids';
    END IF;

    SELECT pg_get_functiondef('public.monthly_distribute_cascade(bigint)'::regprocedure)
    INTO cascade_def;

    SELECT pg_get_functiondef('public.find_allocation_scenario_binding_node_id(bigint,text,bigint,text)'::regprocedure)
    INTO binding_helper_def;

    SELECT pg_get_functiondef('public.allocation_distribute(bigint,bigint,numeric,varchar,integer,text,bigint)'::regprocedure)
    INTO distribute_def;

    SELECT pg_get_functiondef('public.exchange(bigint,integer,numeric,varchar,numeric,varchar)'::regprocedure)
    INTO exchange_def;

    SELECT pg_get_functiondef('public.monthly_distribute_allocation(bigint,bigint,integer,varchar,text,bigint)'::regprocedure)
    INTO monthly_allocation_helper_def;

    SELECT pg_get_functiondef('public.find_allocation_scenario_root_param_value(bigint,text,bigint,text)'::regprocedure)
    INTO root_param_helper_def;

    SELECT pg_get_functiondef('public.resolve_monthly_salary_source(bigint,bigint)'::regprocedure)
    INTO salary_source_helper_def;

    SELECT pg_get_functiondef('public.run_monthly_debt_reserve(bigint,varchar,text)'::regprocedure)
    INTO reserve_helper_def;

    SELECT pg_get_functiondef('public.build_allocation_report_json(bigint,bigint,numeric,varchar,integer,text,bigint)'::regprocedure)
    INTO report_json_helper_def;

    IF POSITION('allocation_scenarios' IN binding_helper_def) = 0
       OR POSITION('allocation_scenario_node_bindings' IN binding_helper_def) = 0 THEN
        RAISE EXCEPTION 'Expected scenario binding helper to read allocation_scenarios and allocation_scenario_node_bindings';
    END IF;

    IF POSITION('allocation_scenarios' IN root_param_helper_def) = 0
       OR POSITION('allocation_scenario_root_params' IN root_param_helper_def) = 0 THEN
        RAISE EXCEPTION 'Expected scenario root param helper to read allocation_scenarios and allocation_scenario_root_params';
    END IF;

    IF POSITION('resolve_monthly_salary_source' IN cascade_def) = 0 THEN
        RAISE EXCEPTION 'Expected monthly_distribute_cascade() to delegate salary source resolution to helper';
    END IF;

    IF POSITION('find_allocation_scenario_binding_node_id' IN salary_source_helper_def) = 0 THEN
        RAISE EXCEPTION 'Expected resolve_monthly_salary_source() to resolve salary source via scenario bindings';
    END IF;

    IF POSITION('branch_source' IN salary_source_helper_def) = 0 THEN
        RAISE EXCEPTION 'Expected resolve_monthly_salary_source() to require branch_source binding';
    END IF;

    IF POSITION('metadata->>''source_category_node_id''' IN salary_source_helper_def) > 0 THEN
        RAISE EXCEPTION 'resolve_monthly_salary_source() still reads salary source from metadata fallback';
    END IF;

    IF POSITION('run_monthly_group_source_root' IN cascade_def) = 0
       OR POSITION('run_monthly_debt_reserve' IN cascade_def) = 0 THEN
        RAISE EXCEPTION 'Expected monthly_distribute_cascade() to delegate prep/reserve steps to helpers';
    END IF;

    IF POSITION('build_allocation_report_json' IN cascade_def) = 0 THEN
        RAISE EXCEPTION 'Expected monthly_distribute_cascade() to delegate report JSON building to helper';
    END IF;

    IF POSITION('allocation_distribute' IN report_json_helper_def) = 0
       OR POSITION('jsonb_agg' IN report_json_helper_def) = 0 THEN
        RAISE EXCEPTION 'Expected build_allocation_report_json() to wrap allocation_distribute() and ordered jsonb_agg';
    END IF;

    IF POSITION('find_allocation_scenario_root_param_value' IN cascade_def) = 0
       AND POSITION('find_allocation_scenario_root_param_value' IN reserve_helper_def) = 0 THEN
        RAISE EXCEPTION 'Expected monthly monthly helpers to resolve prep/reserve config via scenario root params';
    END IF;

    IF POSITION('source_legacy_group_id' IN cascade_def) = 0
       OR POSITION('spend_legacy_group_id' IN reserve_helper_def) = 0
       OR POSITION('personal_legacy_group_id' IN reserve_helper_def) = 0 THEN
        RAISE EXCEPTION 'Expected monthly prep/reserve helpers to keep required scenario root param keys';
    END IF;

    IF POSITION('metadata->>''source_legacy_group_id''' IN cascade_def) > 0
       OR POSITION('metadata->>''spend_legacy_group_id''' IN reserve_helper_def) > 0
       OR POSITION('metadata->>''personal_legacy_group_id''' IN reserve_helper_def) > 0 THEN
        RAISE EXCEPTION 'monthly prep/reserve helpers still read config from root metadata';
    END IF;

    IF POSITION('legacy_group_id = 11' IN cascade_def) > 0
       OR POSITION('legacy_group_id = 12' IN cascade_def) > 0
       OR POSITION('legacy_group_id = 8' IN cascade_def) > 0
       OR POSITION('legacy_group_id = 15' IN cascade_def) > 0 THEN
        RAISE EXCEPTION 'monthly_distribute_cascade() still hard-codes monthly legacy group ids';
    END IF;

    IF POSITION('find_allocation_category_node_id_by_legacy' IN salary_source_helper_def) > 0 THEN
        RAISE EXCEPTION 'resolve_monthly_salary_source() still contains legacy income category fallback';
    END IF;

    IF POSITION('_income_category' IN salary_source_helper_def) > 0 THEN
        RAISE EXCEPTION 'resolve_monthly_salary_source() still depends on explicit income category argument';
    END IF;

    IF POSITION('_income_category' IN cascade_def) > 0 THEN
        RAISE EXCEPTION 'monthly_distribute_cascade() still depends on legacy income category argument';
    END IF;

    IF POSITION('ensure_allocation_compatibility_node' IN distribute_def) > 0 THEN
        RAISE EXCEPTION 'allocation_distribute() still auto-creates compatibility nodes at runtime';
    END IF;

    IF POSITION('ensure_allocation_compatibility_node' IN exchange_def) > 0 THEN
        RAISE EXCEPTION 'exchange() still auto-creates compatibility nodes at runtime';
    END IF;

    IF POSITION('find_allocation_category_node_id_by_legacy' IN monthly_allocation_helper_def) > 0 THEN
        RAISE EXCEPTION 'monthly_distribute_allocation() still falls back to legacy category lookup';
    END IF;

    SELECT pg_get_functiondef(
        'public.allocation_distribute_recursive(bigint,bigint,numeric,varchar,integer,text,bigint[],bigint)'::regprocedure
    )
    INTO recursive_def;

    IF POSITION('find_allocation_scenario_binding_node_id' IN recursive_def) = 0 THEN
        RAISE EXCEPTION 'Expected allocation_distribute_recursive() to resolve bridge source via scenario bindings';
    END IF;

    IF POSITION('bridge_source' IN recursive_def) = 0 THEN
        RAISE EXCEPTION 'Expected allocation_distribute_recursive() to require bridge_source binding';
    END IF;

    IF POSITION('partner_source_category_slug' IN recursive_def) > 0 THEN
        RAISE EXCEPTION 'allocation_distribute_recursive() still reads partner bridge source from metadata fallback';
    END IF;

    IF POSITION('owner_user_id := COALESCE(_node.user_id, _executor_user_id)' IN recursive_def) = 0 THEN
        RAISE EXCEPTION 'Expected group-owned report rows to carry branch owner_user_id via executor fallback';
    END IF;
END $$;

ROLLBACK;
