-- Canonical wrapper for the SQL bootstrap entrypoint kept on the prod branch.
-- Assumes tables.sql, sql_functions.sql, and monthly seed are already applied.
SELECT public.bootstrap_allocation_ledger_from_legacy();
