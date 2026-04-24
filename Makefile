PYTHON ?= python3.13
VENV_DIR ?= .venv
VENV_PYTHON := $(VENV_DIR)/bin/python
PSQL ?= psql
PGHOST ?= localhost
PGPORT ?= 5432
PGUSER ?= postgres
PGDATABASE ?= finance_test

.PHONY: venv test-deps lint-deps test test-sql test-server-validate test-all lint fmt

venv:
	@test -x "$(VENV_PYTHON)" || $(PYTHON) -m venv $(VENV_DIR)

test-deps: venv
	@$(VENV_PYTHON) -m pip install -q pytest

lint-deps: venv
	@$(VENV_PYTHON) -m pip install -q ruff black

test: test-deps
	@$(VENV_PYTHON) -m pytest -q

lint: lint-deps
	@$(VENV_PYTHON) -m ruff check app tests *.py
	@$(VENV_PYTHON) -m black --check app tests *.py

fmt: lint-deps
	@$(VENV_PYTHON) -m ruff check --fix app tests *.py
	@$(VENV_PYTHON) -m black app tests *.py

test-sql:
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tables.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f sql_functions.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/predeploy_business_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/currency_code_length_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/technical_cashflow_description_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/user_membership_helper_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/legacy_sql_app_api_cleanup_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/runtime_legacy_boundary_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/exchange_negative_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/exchange_edge_case_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/spend_with_exchange_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/spend_with_exchange_negative_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/allocation_legacy_bootstrap_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/delete_transaction_runtime_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/ledger_write_path_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/balance_functions_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/allocation_cascade_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/allocation_scenarios_schema_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/allocation_seed_profiles_schema_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/monthly_distribute_allocation_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/monthly_distribute_cascade_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/monthly_entrypoint_metadata_checks.sql
	@$(PSQL) -h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" -v ON_ERROR_STOP=1 -f tests/sql/monthly_business_checks.sql

test-server-validate:
	@bash scripts/run_test_server_validation.sh

test-all: test test-sql
