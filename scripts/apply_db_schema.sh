#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <postgres_container> <db_user> <db_name> <project_dir>"
  exit 1
fi

CONTAINER="$1"
DB_USER="$2"
DB_NAME="$3"
PROJECT_DIR="$4"

TABLES_FILE="$PROJECT_DIR/tables.sql"
FUNCTIONS_FILE="$PROJECT_DIR/sql_functions.sql"

if [ ! -f "$TABLES_FILE" ]; then
  echo "tables.sql file not found at $TABLES_FILE"
  exit 1
fi

if [ ! -f "$FUNCTIONS_FILE" ]; then
  echo "sql_functions.sql file not found at $FUNCTIONS_FILE"
  exit 1
fi

echo "Applying tables.sql..."
docker exec -i "$CONTAINER" psql -v ON_ERROR_STOP=1 -p "5432" -U "$DB_USER" -d "$DB_NAME" < "$TABLES_FILE"
echo "tables.sql applied successfully."

echo "Applying sql_functions.sql..."
docker exec -i "$CONTAINER" psql -v ON_ERROR_STOP=1 -p "5432" -U "$DB_USER" -d "$DB_NAME" < "$FUNCTIONS_FILE"
echo "sql_functions.sql applied successfully."

echo "Database schema and functions applied successfully."
