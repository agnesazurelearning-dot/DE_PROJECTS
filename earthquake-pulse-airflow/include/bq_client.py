"""
BigQuery access helpers shared by every task in the BigQuery DAG variant.

Everything account-specific -- which GCP project, which credentials --
lives in the Airflow Connection named by BIGQUERY_CONN_ID, configured
entirely through Admin > Connections in the Airflow UI. Nothing here
hardcodes a project ID or a credentials path; that's the whole point of
routing it through Airflow's Connection system instead of environment
variables.
"""
from __future__ import annotations

import logging
from typing import Any, Iterable

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

BIGQUERY_CONN_ID = "bigquery_default"
logger = logging.getLogger(__name__)


def get_hook() -> BigQueryHook:
    return BigQueryHook(gcp_conn_id=BIGQUERY_CONN_ID, use_legacy_sql=False)


def get_client() -> bigquery.Client:
    hook = get_hook()
    return hook.get_client(project_id=hook.project_id)


def get_project_id() -> str:
    """The GCP project BigQuery objects live in -- read from the
    `gcp_project_id` Airflow Variable (Admin > Variables in the UI), not
    from the Connection, so DAG code and ad-hoc queries agree on one
    source of truth."""
    from airflow.models import Variable

    return Variable.get("gcp_project_id")


def run_script(sql: str) -> None:
    """Executes a multi-statement DDL script (several CREATE SCHEMA /
    CREATE TABLE statements separated by `;`) as a single BigQuery job.
    Safe to run every DAG run: every statement uses IF NOT EXISTS."""
    client = get_client()
    logger.info("Running BigQuery DDL script (%d chars)", len(sql))
    client.query(sql).result()


def run_query(sql: str, params: list | None = None) -> bigquery.table.RowIterator:
    """Runs a single parameterized query/MERGE/DML statement and returns
    the row iterator (empty for DDL/DML with no result rows)."""
    client = get_client()
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    return client.query(sql, job_config=job_config).result()


def merge_rows(
    sql_template: str,
    project: str,
    rows: list[dict[str, Any]],
    struct_fields: list[tuple[str, str]],
    chunk_size: int = 500,
) -> int:
    """
    Runs a `MERGE ... USING (SELECT * FROM UNNEST(@rows)) ...` statement,
    passing `rows` as a single ARRAY<STRUCT<...>> query parameter -- the
    standard BigQuery pattern for bulk upsert without a separate staging
    table. Chunked at `chunk_size` rows per query so a large backfill
    can't build one oversized request; each chunk is its own MERGE job,
    so a failure partway through a backfill only needs to re-run the
    remaining chunks (each MERGE is independently idempotent).

    `struct_fields` is an ordered list of (field_name, bigquery_type)
    pairs describing every key `rows` dicts must contain.
    """
    if not rows:
        return 0

    sql = sql_template.format(project=project)
    total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        struct_params = [
            bigquery.StructQueryParameter(
                None,
                *[
                    bigquery.ScalarQueryParameter(name, bq_type, row[name])
                    for name, bq_type in struct_fields
                ],
            )
            for row in chunk
        ]
        rows_param = bigquery.ArrayQueryParameter("rows", "STRUCT", struct_params)
        run_query(sql, params=[rows_param])
        total += len(chunk)
        logger.info("Merged rows %d..%d of %d", i, i + len(chunk), len(rows))
    return total
