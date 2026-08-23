"""
Ad-hoc integration smoke test -- NOT part of the pytest suite (imports
psycopg2 directly, not via Airflow's PostgresHook, since it's meant to run
standalone against a real Postgres instance without Airflow installed).

Simulates one full DAG run (extract_bronze -> transform_silver ->
build_gold) against a real Postgres database using realistic synthetic
USGS features (place strings taken from a live USGS pull performed
earlier), then re-runs it to prove idempotency, and finally simulates a
USGS revision (magnitude upgrade) to prove the updated_time guard works.

Usage:
    PGHOST=localhost PGUSER=analytics PGPASSWORD=analytics PGDATABASE=earthquake_analytics \
        python3 tests/_integration_smoke_test.py
"""
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2  # noqa: E402

from include.transform import feature_to_silver_row, UPSERT_SILVER_SQL  # noqa: E402
from include.gold_aggregations import (  # noqa: E402
    UPSERT_REGION_ACTIVITY_WINDOW,
    UPSERT_GLOBAL_SUMMARY_WINDOW,
    UPSERT_SWARM_SIGNALS,
    UPSERT_DAILY_REGION_SUMMARY,
)

CONN_KWARGS = dict(
    host=os.environ.get("PGHOST", "localhost"),
    user=os.environ.get("PGUSER", "analytics"),
    password=os.environ.get("PGPASSWORD", "analytics"),
    dbname=os.environ.get("PGDATABASE", "earthquake_analytics"),
)


def make_feature(event_id, mag, place, minutes_ago, sig=None, tsunami=0, updated_offset_sec=600):
    event_time = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    updated_time = event_time + timedelta(seconds=updated_offset_sec)
    coords = {
        "Ridgecrest, CA": [-117.67, 35.65, 8.2],
        "Cobb, CA": [-122.75, 38.83, 2.4],
        "Kimbe, Papua New Guinea": [150.14, -5.55, 62.0],
        "Fiji": [178.0, -17.0, 550.0],
        "Anza, CA": [-116.68, 33.55, 12.1],
    }.get(place, [0.0, 0.0, 10.0])
    return {
        "type": "Feature",
        "id": event_id,
        "properties": {
            "mag": mag,
            "place": f"5 km N of {place}" if "," in place or place == "Fiji" else place,
            "time": int(event_time.timestamp() * 1000),
            "updated": int(updated_time.timestamp() * 1000),
            "type": "earthquake",
            "alert": None,
            "tsunami": tsunami,
            "sig": sig if sig is not None else int(mag * 60),
            "status": "automatic",
            "felt": None,
            "net": "us",
            "magType": "ml",
        },
        "geometry": {"type": "Point", "coordinates": coords},
    }


def run_extract_bronze(conn, batch_id, dag_run_id, window_start, window_end, features):
    with conn.cursor() as cur:
        for feature in features:
            cur.execute(
                """
                INSERT INTO bronze.earthquakes_raw
                    (batch_id, dag_run_id, window_start, window_end, source_url, event_id, raw_feature)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id, event_id) DO NOTHING
                """,
                (batch_id, dag_run_id, window_start, window_end, "https://test", feature["id"], json.dumps(feature)),
            )
    conn.commit()


def run_transform_silver(conn, batch_id):
    written, skipped = 0, 0
    with conn.cursor() as select_cur, conn.cursor() as write_cur:
        select_cur.execute("SELECT raw_feature FROM bronze.earthquakes_raw WHERE batch_id = %s", (batch_id,))
        for (raw_feature,) in select_cur:
            row = feature_to_silver_row(raw_feature, batch_id)
            if row is None:
                skipped += 1
                continue
            write_cur.execute(UPSERT_SILVER_SQL, row.as_tuple())
            written += 1
    conn.commit()
    return written, skipped


def run_build_gold(conn, window_start, window_end, swarm_ratio=2.0):
    params = {"window_start": window_start, "window_end": window_end, "swarm_ratio": swarm_ratio}
    with conn.cursor() as cur:
        cur.execute(UPSERT_REGION_ACTIVITY_WINDOW, params)
        cur.execute(UPSERT_GLOBAL_SUMMARY_WINDOW, params)
        cur.execute(UPSERT_SWARM_SIGNALS, params)
        cur.execute(UPSERT_DAILY_REGION_SUMMARY, params)
    conn.commit()


def fetch_all(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def main():
    conn = psycopg2.connect(**CONN_KWARGS)

    # Aligned to "now" (not the truncated calendar hour) so that every
    # synthetic event's `minutes_ago` (5..55) is guaranteed to fall inside
    # [window_start, window_end) regardless of what minute this runs at.
    window_end = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=1)
    window_start = window_end - timedelta(hours=1)
    dag_run_id = f"manual__{uuid.uuid4()}"
    batch_id = dag_run_id

    features = [
        make_feature("test-us1", 3.2, "Ridgecrest, CA", minutes_ago=50),
        make_feature("test-us2", 2.1, "Ridgecrest, CA", minutes_ago=45),
        make_feature("test-us3", 4.8, "Ridgecrest, CA", minutes_ago=40),  # significant
        make_feature("test-us4", 1.5, "Cobb, CA", minutes_ago=30),
        make_feature("test-intl1", 5.1, "Kimbe, Papua New Guinea", minutes_ago=20, tsunami=1),  # significant + tsunami
        make_feature("test-intl2", 3.9, "Fiji", minutes_ago=10),
        make_feature("test-bad-coords", 2.0, "Anza, CA", minutes_ago=5),
    ]
    # Corrupt one feature's coordinates to prove the "skip, don't crash" path
    features[-1]["geometry"]["coordinates"] = [None, None, None]

    print(f"== Run 1: batch_id={batch_id} window={window_start}..{window_end} ==")
    run_extract_bronze(conn, batch_id, dag_run_id, window_start, window_end, features)
    written, skipped = run_transform_silver(conn, batch_id)
    print(f"silver: written={written} skipped={skipped} (expect written=6 skipped=1)")
    assert written == 6 and skipped == 1

    run_build_gold(conn, window_start, window_end)

    region_rows = fetch_all(conn, "SELECT region, country, event_count, avg_magnitude, significant_event_count, tsunami_alert_count FROM gold.region_activity_window WHERE window_start = %(ws)s ORDER BY region", {"ws": window_start})
    print("gold.region_activity_window:")
    for r in region_rows:
        print(" ", r)

    global_row = fetch_all(conn, "SELECT * FROM gold.global_summary_window WHERE window_start = %(ws)s", {"ws": window_start})
    print("gold.global_summary_window:", global_row)
    assert global_row[0]["total_events"] == 6
    assert global_row[0]["significant_event_count"] == 2
    assert global_row[0]["tsunami_alert_count"] == 1

    ridgecrest = next(r for r in region_rows if "Ridgecrest" in r["region"])
    assert ridgecrest["event_count"] == 3
    assert ridgecrest["significant_event_count"] == 1

    daily_rows = fetch_all(conn, "SELECT region, country, event_count FROM gold.daily_region_summary WHERE activity_date = %(d)s ORDER BY region", {"d": window_start.date()})
    print("gold.daily_region_summary:", daily_rows)

    # --- Idempotency check: re-run the exact same batch end-to-end ---
    print("\n== Re-run 1 (idempotency check) ==")
    run_extract_bronze(conn, batch_id, dag_run_id, window_start, window_end, features)
    written2, skipped2 = run_transform_silver(conn, batch_id)
    run_build_gold(conn, window_start, window_end)
    bronze_count = fetch_all(conn, "SELECT COUNT(*) AS n FROM bronze.earthquakes_raw WHERE batch_id = %(b)s", {"b": batch_id})[0]["n"]
    silver_count = fetch_all(conn, "SELECT COUNT(*) AS n FROM silver.earthquakes_clean WHERE event_id LIKE 'test-%%'", {})[0]["n"]
    global_row2 = fetch_all(conn, "SELECT total_events FROM gold.global_summary_window WHERE window_start = %(ws)s", {"ws": window_start})
    print(f"bronze rows after re-run: {bronze_count} (expect 7, no dupes)")
    print(f"silver rows after re-run: {silver_count} (expect 6, no dupes)")
    print(f"gold total_events after re-run: {global_row2[0]['total_events']} (expect 6, recomputed not doubled)")
    assert bronze_count == 7
    assert silver_count == 6
    assert global_row2[0]["total_events"] == 6

    # --- Revision guard check: USGS "revises" test-us3 mag 4.8 -> 5.5 ---
    print("\n== Simulating a USGS revision (magnitude upgrade) ==")
    revised = make_feature("test-us3", 5.5, "Ridgecrest, CA", minutes_ago=40, updated_offset_sec=7200)
    row = feature_to_silver_row(revised, "revision-batch")
    with conn.cursor() as cur:
        cur.execute(UPSERT_SILVER_SQL, row.as_tuple())
    conn.commit()
    mag_after = fetch_all(conn, "SELECT magnitude FROM silver.earthquakes_clean WHERE event_id = 'test-us3'")[0]["magnitude"]
    print(f"magnitude after revision: {mag_after} (expect 5.50, newer updated_time wins)")
    assert float(mag_after) == 5.5

    # --- Stale write guard: an OLDER updated_time must NOT overwrite ---
    stale = make_feature("test-us3", 1.0, "Ridgecrest, CA", minutes_ago=40, updated_offset_sec=1)
    row = feature_to_silver_row(stale, "stale-batch")
    with conn.cursor() as cur:
        cur.execute(UPSERT_SILVER_SQL, row.as_tuple())
    conn.commit()
    mag_after_stale = fetch_all(conn, "SELECT magnitude FROM silver.earthquakes_clean WHERE event_id = 'test-us3'")[0]["magnitude"]
    print(f"magnitude after stale write attempt: {mag_after_stale} (expect still 5.50, stale write rejected)")
    assert float(mag_after_stale) == 5.5

    print("\nALL INTEGRATION CHECKS PASSED")
    conn.close()


if __name__ == "__main__":
    main()
