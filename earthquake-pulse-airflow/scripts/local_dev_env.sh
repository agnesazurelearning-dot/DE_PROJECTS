#!/usr/bin/env bash
# Source this file (do NOT execute it) in every terminal before running any
# `airflow ...` command for this project's non-Docker local setup:
#
#   source scripts/local_dev_env.sh
#
# This only sets what genuinely CANNOT be configured through the Airflow
# UI -- because the UI itself doesn't exist until Airflow has already
# started up pointed at a metadata database. That's a hard chicken-and-egg
# constraint of Airflow, not a design choice.
#
# Everything that CAN be done in the browser -- the analytics_db
# connection, the usgs_min_magnitude / swarm_spike_ratio tuning knobs --
# is deliberately left out of this file. Set those via Admin > Connections
# and Admin > Variables after `airflow standalone` is running (see
# README.md's "Fully UI-driven setup" section). That's the whole point of
# routing them through Airflow's Connection/Variable system instead of
# environment variables.

# --- Airflow home + DAG discovery -------------------------------------
export AIRFLOW_HOME="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="false"
export AIRFLOW__CORE__DEFAULT_TIMEZONE="utc"
export AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT="false"

# --- Executor + metadata DB (LocalExecutor needs Postgres, not SQLite) --
# Adjust user/password here if you created the `airflow` Postgres role
# with different credentials than the setup guide used.
export AIRFLOW__CORE__EXECUTOR="LocalExecutor"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@localhost:5432/airflow_meta"

# --- Make `include` importable everywhere (belt-and-suspenders; the DAG --
# file also does this itself at import time) -----------------------------
export PYTHONPATH="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd):${PYTHONPATH}"

echo "Local dev environment set. AIRFLOW_HOME=$AIRFLOW_HOME"
echo "Next: airflow db init  (first time only)"
echo "Then: airflow standalone"
echo "Then in the browser: Admin > Connections > add 'analytics_db', Admin > Variables > add usgs_min_magnitude / swarm_spike_ratio"
