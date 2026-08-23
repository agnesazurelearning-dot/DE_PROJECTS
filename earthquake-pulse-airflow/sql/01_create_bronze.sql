-- =====================================================================
-- BRONZE LAYER
-- Raw, untransformed landing zone for USGS earthquake API responses.
-- One row per GeoJSON "feature" (= one seismic event record as reported
-- by USGS at ingestion time). The full feature object is preserved
-- byte-for-byte in `raw_feature` so nothing is ever lost or reshaped
-- before it lands here -- this is the audit trail / replay source for
-- every downstream layer.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.earthquakes_raw (
    bronze_id       BIGSERIAL PRIMARY KEY,
    batch_id        TEXT        NOT NULL,       -- one batch_id per DAG run (dag_id + data_interval)
    dag_run_id      TEXT        NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,        -- data_interval_start used for the API pull
    window_end      TIMESTAMPTZ NOT NULL,        -- data_interval_end used for the API pull
    source_url      TEXT        NOT NULL,
    event_id        TEXT        NOT NULL,        -- USGS feature.id, denormalized for indexing/debugging
    raw_feature     JSONB       NOT NULL,        -- the untouched GeoJSON Feature object
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Idempotency guard: re-running (or retrying) the same DAG run for the
    -- same window must not duplicate rows. A retried task simply upserts
    -- the same (batch_id, event_id) pair.
    CONSTRAINT uq_bronze_batch_event UNIQUE (batch_id, event_id)
);

CREATE INDEX IF NOT EXISTS ix_bronze_event_id ON bronze.earthquakes_raw (event_id);
CREATE INDEX IF NOT EXISTS ix_bronze_window ON bronze.earthquakes_raw (window_start, window_end);
CREATE INDEX IF NOT EXISTS ix_bronze_fetched_at ON bronze.earthquakes_raw (fetched_at);

-- Track every ingestion run for observability / lineage (mirrors what you'd
-- otherwise bolt on with OpenLineage or a custom audit table in production).
CREATE TABLE IF NOT EXISTS bronze.ingestion_log (
    batch_id        TEXT PRIMARY KEY,
    dag_run_id      TEXT        NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    source_url      TEXT        NOT NULL,
    records_fetched INTEGER     NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          TEXT        NOT NULL DEFAULT 'success'
);
