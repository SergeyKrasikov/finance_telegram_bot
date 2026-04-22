#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PSQL_BIN="${PSQL:-psql}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-finance_test}"

RUN_MONTHLY_SMOKE="${RUN_MONTHLY_SMOKE:-0}"

run_file() {
  local file="$1"
  echo
  echo "==> Running ${file}"
  "${PSQL_BIN}" \
    -h "${PGHOST}" \
    -p "${PGPORT}" \
    -U "${PGUSER}" \
    -d "${PGDATABASE}" \
    -v ON_ERROR_STOP=1 \
    -f "${ROOT_DIR}/${file}"
}

run_sql() {
  local sql="$1"
  echo
  echo "==> Running SQL: ${sql}"
  "${PSQL_BIN}" \
    -h "${PGHOST}" \
    -p "${PGPORT}" \
    -U "${PGUSER}" \
    -d "${PGDATABASE}" \
    -v ON_ERROR_STOP=1 \
    -c "${sql}"
}

echo "==> Test server validation started"
echo "PGHOST=${PGHOST}"
echo "PGPORT=${PGPORT}"
echo "PGUSER=${PGUSER}"
echo "PGDATABASE=${PGDATABASE}"
echo "RUN_MONTHLY_SMOKE=${RUN_MONTHLY_SMOKE}"

run_file "tables.sql"
run_file "sql_functions.sql"
run_file "scripts/seed_monthly_allocation_graph.sql"
run_sql "SELECT public.bootstrap_allocation_ledger_from_legacy();"

run_sql "SELECT count(*) AS active_monthly_scenarios FROM public.allocation_scenarios WHERE active AND scenario_kind = 'monthly';"
run_sql "SELECT count(*) AS active_salary_roots FROM public.allocation_nodes WHERE active AND slug = 'salary_primary';"
run_sql "SELECT count(*) AS backfilled_rows FROM public.allocation_postings WHERE metadata->>'origin' = 'migration' AND metadata->>'backfill_kind' = 'cash_flow';"

run_file "tests/sql/predeploy_business_checks.sql"
run_file "tests/sql/currency_code_length_checks.sql"
run_file "tests/sql/technical_cashflow_description_checks.sql"
run_file "tests/sql/user_membership_helper_checks.sql"
run_file "tests/sql/legacy_sql_app_api_cleanup_checks.sql"
run_file "tests/sql/exchange_negative_checks.sql"
run_file "tests/sql/exchange_edge_case_checks.sql"
run_file "tests/sql/spend_with_exchange_checks.sql"
run_file "tests/sql/spend_with_exchange_negative_checks.sql"
run_file "tests/sql/allocation_legacy_bootstrap_checks.sql"
run_file "tests/sql/delete_transaction_runtime_checks.sql"
run_file "tests/sql/ledger_write_path_checks.sql"
run_file "tests/sql/balance_functions_checks.sql"
run_file "tests/sql/allocation_cascade_checks.sql"
run_file "tests/sql/allocation_scenarios_schema_checks.sql"
run_file "tests/sql/allocation_seed_profiles_schema_checks.sql"
run_file "tests/sql/monthly_distribute_allocation_checks.sql"
run_file "tests/sql/monthly_distribute_cascade_checks.sql"
run_file "tests/sql/monthly_entrypoint_metadata_checks.sql"
run_file "tests/sql/monthly_business_checks.sql"

if [[ "${RUN_MONTHLY_SMOKE}" == "1" ]]; then
  run_sql "SELECT public.monthly();"
fi

echo
echo "==> Test server validation completed successfully"
