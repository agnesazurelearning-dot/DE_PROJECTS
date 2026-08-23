#!/usr/bin/env bash
# Runs once, automatically, the first time the postgres container's data
# volume is initialized (standard postgres docker-entrypoint-initdb.d hook).
# Creates the separate analytics database + role that bronze/silver/gold
# live in, kept apart from Airflow's own metadata database.
set -euo pipefail

: "${ANALYTICS_DB_USER:=analytics}"
: "${ANALYTICS_DB_PASSWORD:=analytics}"
: "${ANALYTICS_DB_NAME:=earthquake_analytics}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER ${ANALYTICS_DB_USER} WITH PASSWORD '${ANALYTICS_DB_PASSWORD}';
    CREATE DATABASE ${ANALYTICS_DB_NAME} OWNER ${ANALYTICS_DB_USER};
    GRANT ALL PRIVILEGES ON DATABASE ${ANALYTICS_DB_NAME} TO ${ANALYTICS_DB_USER};
EOSQL
