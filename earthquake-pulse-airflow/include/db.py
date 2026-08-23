"""
Postgres access helpers shared by every DAG task. Uses Airflow's
PostgresHook (conn_id="analytics_db", set up in docker-compose.yml /
airflow-init) so connection credentials never live in DAG code.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

from airflow.providers.postgres.hooks.postgres import PostgresHook

ANALYTICS_CONN_ID = "analytics_db"
logger = logging.getLogger(__name__)


def get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=ANALYTICS_CONN_ID)


@contextmanager
def get_conn() -> Iterator:
    hook = get_hook()
    conn = hook.get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_sql_file(path: str) -> None:
    """Executes a .sql file (schema DDL) against the analytics DB. Safe to
    call every DAG run since all DDL uses CREATE ... IF NOT EXISTS."""
    with open(path, "r", encoding="utf-8") as fh:
        sql = fh.read()
    hook = get_hook()
    logger.info("Running SQL file: %s", path)
    hook.run(sql)


def executemany(sql: str, rows: list[tuple]) -> int:
    """Bulk executes an INSERT/UPSERT statement for many rows in one
    transaction. Returns the number of rows submitted (not necessarily the
    number of rows changed, since ON CONFLICT clauses may no-op)."""
    if not rows:
        return 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
    return len(rows)
