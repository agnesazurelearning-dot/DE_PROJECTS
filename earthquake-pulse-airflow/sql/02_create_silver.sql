-- =====================================================================
-- SILVER LAYER
-- Cleaned, normalized, deduplicated earthquake events. One row per
-- unique USGS event_id. USGS revises events after review (magnitude
-- gets upgraded/downgraded, location refined), so this table is kept
-- current via UPSERT keyed on event_id, guarded by `updated_time` so an
-- out-of-order/backfill run can never overwrite a newer revision with
-- stale data (see include/transform.py::upsert_silver).
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.earthquakes_clean (
    event_id            TEXT PRIMARY KEY,
    magnitude           NUMERIC(4,2),
    mag_type            TEXT,
    place_raw           TEXT,
    region              TEXT        NOT NULL,   -- normalized region/place label
    country             TEXT        NOT NULL,   -- best-effort normalized country/jurisdiction
    latitude            NUMERIC(9,6) NOT NULL,
    longitude           NUMERIC(9,6) NOT NULL,
    depth_km            NUMERIC(7,2),
    event_time          TIMESTAMPTZ NOT NULL,   -- when the quake occurred
    updated_time         TIMESTAMPTZ NOT NULL,   -- USGS revision timestamp (idempotency guard)
    event_type          TEXT        NOT NULL DEFAULT 'earthquake',
    alert_level          TEXT,                   -- green/yellow/orange/red or NULL
    tsunami_flag         BOOLEAN     NOT NULL DEFAULT FALSE,
    significance         INTEGER,                -- USGS "sig" score
    review_status         TEXT,                   -- reviewed / automatic / deleted
    felt_reports          INTEGER,
    source_network        TEXT,
    is_significant        BOOLEAN     GENERATED ALWAYS AS (COALESCE(magnitude, 0) >= 4.5) STORED,
    bronze_batch_id        TEXT        NOT NULL,   -- lineage back to the bronze batch that produced this row
    silver_loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_silver_event_time ON silver.earthquakes_clean (event_time);
CREATE INDEX IF NOT EXISTS ix_silver_region ON silver.earthquakes_clean (region);
CREATE INDEX IF NOT EXISTS ix_silver_country ON silver.earthquakes_clean (country);
CREATE INDEX IF NOT EXISTS ix_silver_significant ON silver.earthquakes_clean (is_significant) WHERE is_significant;
