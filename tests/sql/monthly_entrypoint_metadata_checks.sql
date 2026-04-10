-- monthly entrypoint metadata checks
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/monthly_entrypoint_metadata_checks.sql

BEGIN;

DO $$
DECLARE
    has_node_metadata boolean;
    monthly_def text;
    cascade_def text;
    recursive_def text;
    binding_helper_def text;
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

    SELECT pg_get_functiondef('public.monthly_distribute_cascade(bigint,integer)'::regprocedure)
    INTO cascade_def;

    SELECT pg_get_functiondef('public.find_allocation_scenario_binding_node_id(bigint,text,bigint,text)'::regprocedure)
    INTO binding_helper_def;

    IF POSITION('allocation_scenarios' IN binding_helper_def) = 0
       OR POSITION('allocation_scenario_node_bindings' IN binding_helper_def) = 0 THEN
        RAISE EXCEPTION 'Expected scenario binding helper to read allocation_scenarios and allocation_scenario_node_bindings';
    END IF;

    IF POSITION('find_allocation_scenario_binding_node_id' IN cascade_def) = 0 THEN
        RAISE EXCEPTION 'Expected monthly_distribute_cascade() to resolve salary source via scenario bindings';
    END IF;

    IF POSITION('source_category_node_id' IN cascade_def) = 0 THEN
        RAISE EXCEPTION 'Expected monthly_distribute_cascade() to keep metadata salary source fallback during migration';
    END IF;

    IF POSITION('source_legacy_group_id' IN cascade_def) = 0
       OR POSITION('spend_legacy_group_id' IN cascade_def) = 0
       OR POSITION('personal_legacy_group_id' IN cascade_def) = 0 THEN
        RAISE EXCEPTION 'Expected monthly_distribute_cascade() to read monthly source group config from root metadata';
    END IF;

    IF POSITION('legacy_group_id = 11' IN cascade_def) > 0
       OR POSITION('legacy_group_id = 12' IN cascade_def) > 0
       OR POSITION('legacy_group_id = 8' IN cascade_def) > 0
       OR POSITION('legacy_group_id = 15' IN cascade_def) > 0 THEN
        RAISE EXCEPTION 'monthly_distribute_cascade() still hard-codes monthly legacy group ids';
    END IF;

    IF POSITION('find_allocation_category_node_id_by_legacy' IN cascade_def) = 0 THEN
        RAISE EXCEPTION 'Expected monthly_distribute_cascade() to keep legacy income category fallback during migration';
    END IF;

    SELECT pg_get_functiondef(
        'public.allocation_distribute_recursive(bigint,bigint,numeric,varchar,integer,text,bigint[])'::regprocedure
    )
    INTO recursive_def;

    IF POSITION('find_allocation_scenario_binding_node_id' IN recursive_def) = 0 THEN
        RAISE EXCEPTION 'Expected allocation_distribute_recursive() to resolve bridge source via scenario bindings';
    END IF;

    IF POSITION('metadata.partner_source_category_slug' IN recursive_def) = 0
       AND POSITION('partner_source_category_slug' IN recursive_def) = 0 THEN
        RAISE EXCEPTION 'Expected allocation_distribute_recursive() to keep metadata bridge fallback during migration';
    END IF;

    IF POSITION('owner_user_id := COALESCE(_node.user_id, _executor_user_id)' IN recursive_def) = 0 THEN
        RAISE EXCEPTION 'Expected group-owned report rows to carry branch owner_user_id via executor fallback';
    END IF;
END $$;

ROLLBACK;
