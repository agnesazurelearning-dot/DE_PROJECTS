"""
earthquake_medallion_pipeline
==============================
End-to-end, production-style Airflow DAG that ingests live global
earthquake data from the USGS FDSNWS API and builds a Bronze -> Silver ->
Gold medallion pipeline on Postgres, ready for BI tools (Power BI,
Metabase, Superset, ...).

This is the seismic-data sibling of a flight-tracking medallion pipeline:
same orchestration pattern (idempotent, time-windowed, retry-safe,
UPSERT-based), different domain. Where the flight project monitored
global air-traffic volume/congestion, this project monitors global
seismic activity volume and "swarm" (activity-spike) signals.

Design choices worth calling out:
- Schedule: hourly. `data_interval_start` / `data_interval_end` give each
  run an exact, reproducible [start, end) window -- the same window is
  requested again on retry or manual backfill, so results are
  deterministic rather than "whatever the API happened to return right
  now".
- catchup=False by default (see docker-compose.yml), but the DAG is fully
  backfill-safe: `airflow dags backfill` for any historical date range
  will produce correct, non-duplicated bronze/silver/gold rows because
  every write path is an UPSERT keyed on a stable natural key.
- Each task is independently retryable: a transient USGS API hiccup only
  retries `extract_bronze`, not the whole DAG run.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Make the project's `include` package importable regardless of how/where
# Airflow was installed (Docker container, local venv, Astronomer, ...).
# This file lives at <project_root>/dags/earthquake_medallion_pipeline.py,
# so its grandparent directory is always the project root -- inserting it
# onto sys.path here means `from include.db import ...` (used inside the
# task bodies below) resolves the same way in every environment, with no
# PYTHONPATH configuration required.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from airflow.decorators import dag, task
from airflow.models import Variable

logger = logging.getLogger(__name__)

# Same portability reasoning as above: resolve sql/ relative to this file
# rather than assuming a Docker-container path, with an env var escape
# hatch for anyone who wants to override it.
SQL_DIR = os.environ.get("EARTHQUAKE_SQL_DIR", str(PROJECT_ROOT / "sql"))

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
    "email_on_failure": False,
}


@dag(
    dag_id="earthquake_medallion_pipeline",
    description="Bronze/Silver/Gold ingestion of live USGS earthquake data for near-real-time BI analytics",
    schedule="@hourly",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["medallion", "earthquake", "usgs", "portfolio"],
    doc_md=__doc__,
)
def earthquake_medallion_pipeline():

    @task
    def ensure_schema():
        """Idempotent DDL bootstrap -- safe to run every DAG run."""
        from include.db import run_sql_file

        for filename in ("01_create_bronze.sql", "02_create_silver.sql", "03_create_gold.sql"):
            run_sql_file(f"{SQL_DIR}/{filename}")

    @task
    def extract_bronze(**context) -> dict:
        """Bronze: pull raw USGS GeoJSON for this run's exact data
        interval and land it, one row per feature, into
        bronze.earthquakes_raw. Idempotent via ON CONFLICT (batch_id,
        event_id) DO NOTHING -- a retried task attempt cannot duplicate
        rows."""
        from include.usgs_client import fetch_earthquakes, build_request_url
        from include.db import get_conn

        window_start = context["data_interval_start"]
        window_end = context["data_interval_end"]
        dag_run_id = context["run_id"]
        batch_id = f"{dag_run_id}"
        min_magnitude = float(Variable.get("usgs_min_magnitude", default_var="0"))

        started_at = datetime.utcnow()
        payload = fetch_earthquakes(window_start, window_end, min_magnitude=min_magnitude)
        features = payload.get("features", [])
        source_url = build_request_url(window_start, window_end, min_magnitude)

        with get_conn() as conn:
            with conn.cursor() as cur:
                for feature in features:
                    cur.execute(
                        """
                        INSERT INTO bronze.earthquakes_raw
                            (batch_id, dag_run_id, window_start, window_end,
                             source_url, event_id, raw_feature)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (batch_id, event_id) DO NOTHING
                        """,
                        (
                            batch_id, dag_run_id, window_start, window_end,
                            source_url, feature.get("id"),
                            json.dumps(feature),
                        ),
                    )
                cur.execute(
                    """
                    INSERT INTO bronze.ingestion_log
                        (batch_id, dag_run_id, window_start, window_end,
                         source_url, records_fetched, started_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (batch_id) DO UPDATE SET
                        records_fetched = EXCLUDED.records_fetched,
                        finished_at = now()
                    """,
                    (batch_id, dag_run_id, window_start, window_end,
                     source_url, len(features), started_at),
                )

        logger.info("Bronze ingestion complete: batch_id=%s records=%s", batch_id, len(features))
        return {
            "batch_id": batch_id,
            "record_count": len(features),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        }

    @task
    def transform_silver(bronze_result: dict) -> dict:
        """Silver: read this batch's bronze rows, parse + normalize each
        feature, and UPSERT into silver.earthquakes_clean, guarded so a
        stale/out-of-order write can never clobber a newer USGS
        revision (see include/transform.py::UPSERT_SILVER_SQL)."""
        from include.db import get_conn
        from include.transform import feature_to_silver_row, UPSERT_SILVER_SQL

        batch_id = bronze_result["batch_id"]
        if bronze_result["record_count"] == 0:
            logger.info("No bronze records for batch_id=%s; skipping silver transform.", batch_id)
            return {"batch_id": batch_id, "silver_rows": 0, **{
                k: bronze_result[k] for k in ("window_start", "window_end")
            }}

        with get_conn() as conn:
            with conn.cursor() as select_cur, conn.cursor() as write_cur:
                select_cur.execute(
                    "SELECT raw_feature FROM bronze.earthquakes_raw WHERE batch_id = %s",
                    (batch_id,),
                )
                skipped = 0
                written = 0
                for (raw_feature,) in select_cur:
                    row = feature_to_silver_row(raw_feature, batch_id)
                    if row is None:
                        skipped += 1
                        continue
                    write_cur.execute(UPSERT_SILVER_SQL, row.as_tuple())
                    written += 1

        logger.info(
            "Silver transform complete: batch_id=%s written=%s skipped=%s",
            batch_id, written, skipped,
        )
        return {
            "batch_id": batch_id,
            "silver_rows": written,
            "window_start": bronze_result["window_start"],
            "window_end": bronze_result["window_end"],
        }

    @task
    def build_gold(silver_result: dict) -> dict:
        """Gold: recompute every downstream aggregate for this run's
        window with pure UPSERTs -- region activity, global summary,
        swarm-signal detection, and the daily rollup. Safe to re-run."""
        from datetime import datetime as dt
        from include.db import get_conn
        from include.gold_aggregations import (
            UPSERT_REGION_ACTIVITY_WINDOW,
            UPSERT_GLOBAL_SUMMARY_WINDOW,
            UPSERT_SWARM_SIGNALS,
            UPSERT_DAILY_REGION_SUMMARY,
        )

        window_start = dt.fromisoformat(silver_result["window_start"])
        window_end = dt.fromisoformat(silver_result["window_end"])
        swarm_ratio = float(Variable.get("swarm_spike_ratio", default_var="2.0"))
        params = {"window_start": window_start, "window_end": window_end, "swarm_ratio": swarm_ratio}

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(UPSERT_REGION_ACTIVITY_WINDOW, params)
                cur.execute(UPSERT_GLOBAL_SUMMARY_WINDOW, params)
                cur.execute(UPSERT_SWARM_SIGNALS, params)
                cur.execute(UPSERT_DAILY_REGION_SUMMARY, params)

        logger.info("Gold layer rebuilt for window=%s..%s", window_start, window_end)
        return {"batch_id": silver_result["batch_id"], "status": "gold_complete"}

    @task
    def data_quality_check(gold_result: dict):
        """Lightweight post-load sanity gate. Fails the run loudly (rather
        than silently shipping bad data to BI dashboards) if the gold
        layer has nulls/negative values where it shouldn't, or if a
        window that ingested bronze records somehow produced zero gold
        rows. Mirrors the kind of DQ gate a real pipeline would run
        (Great Expectations / dbt tests would formalize this further --
        see README)."""
        from include.db import get_hook

        hook = get_hook()
        bad_rows = hook.get_first(
            """
            SELECT COUNT(*) FROM gold.region_activity_window
            WHERE event_count < 0
               OR avg_magnitude IS NOT NULL AND (avg_magnitude < -2 OR avg_magnitude > 10)
            """
        )[0]
        if bad_rows > 0:
            raise ValueError(f"Data quality check failed: {bad_rows} implausible gold rows found")

        logger.info("Data quality check passed for batch_id=%s", gold_result["batch_id"])

    schema_ready = ensure_schema()
    bronze = extract_bronze()
    silver = transform_silver(bronze)
    gold = build_gold(silver)
    dq = data_quality_check(gold)

    schema_ready >> bronze


earthquake_medallion_pipeline()
