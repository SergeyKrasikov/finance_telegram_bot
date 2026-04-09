-- monthly entrypoint metadata checks
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/monthly_entrypoint_metadata_checks.sql

BEGIN;

DO $$
DECLARE
    has_node_metadata boolean;
    monthly_def text;
    cascade_def text;
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

    IF POSITION('metadata' IN cascade_def) = 0
       OR POSITION('source_category_node_id' IN cascade_def) = 0 THEN
        RAISE EXCEPTION 'Expected monthly_distribute_cascade() to read salary source from salary_primary metadata';
    END IF;

    IF POSITION('find_allocation_category_node_id_by_legacy' IN cascade_def) = 0 THEN
        RAISE EXCEPTION 'Expected monthly_distribute_cascade() to keep legacy income category fallback during migration';
    END IF;
END $$;

ROLLBACK;
