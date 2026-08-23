-- =====================================================================
-- GOLD LAYER
-- Time-windowed, BI-ready aggregates. Everything here is rebuilt with
-- UPSERT keyed on its natural grain, so re-running a DAG run (backfill,
-- retry, manual clear) recomputes that slice deterministically instead
-- of double-counting -- the same idempotency property the flight-data
-- project used for gold.flight_kpis.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- 1) Per-region activity for the DAG's own run window (native pipeline grain)
CREATE TABLE IF NOT EXISTS gold.region_activity_window (
    window_start        TIMESTAMPTZ NOT NULL,
    window_end            TIMESTAMPTZ NOT NULL,
    region                TEXT        NOT NULL,
    country                TEXT        NOT NULL,
    event_count            INTEGER     NOT NULL,
    avg_magnitude          NUMERIC(4,2),
    max_magnitude          NUMERIC(4,2),
    min_magnitude          NUMERIC(4,2),
    significant_event_count INTEGER    NOT NULL DEFAULT 0,   -- magnitude >= 4.5
    strongest_event_id      TEXT,
    tsunami_alert_count      INTEGER    NOT NULL DEFAULT 0,
    avg_depth_km            NUMERIC(7,2),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, window_end, region, country)
);

CREATE INDEX IF NOT EXISTS ix_gold_region_window ON gold.region_activity_window (window_start);

-- 2) Global summary per run window -- the single-number KPIs for a BI headline tile
CREATE TABLE IF NOT EXISTS gold.global_summary_window (
    window_start          TIMESTAMPTZ NOT NULL,
    window_end              TIMESTAMPTZ NOT NULL,
    total_events            INTEGER     NOT NULL,
    distinct_regions        INTEGER     NOT NULL,
    avg_magnitude            NUMERIC(4,2),
    max_magnitude            NUMERIC(4,2),
    significant_event_count  INTEGER    NOT NULL DEFAULT 0,
    tsunami_alert_count      INTEGER    NOT NULL DEFAULT 0,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, window_end)
);

-- 3) Seismic "swarm" / congestion-style signal detection: flags a region
-- whose activity in this window is a significant spike above its own
-- trailing baseline -- the seismic analogue of the flight project's
-- "congestion signal" detection.
CREATE TABLE IF NOT EXISTS gold.swarm_signals (
    window_start            TIMESTAMPTZ NOT NULL,
    window_end                TIMESTAMPTZ NOT NULL,
    region                    TEXT        NOT NULL,
    country                    TEXT        NOT NULL,
    event_count                INTEGER     NOT NULL,
    baseline_avg_count          NUMERIC(6,2) NOT NULL,   -- trailing 7-day same-window-length average
    spike_ratio                NUMERIC(6,2),             -- event_count / NULLIF(baseline_avg_count, 0); NULL when there's no baseline yet (new/rare region)
    signal_level                TEXT        NOT NULL,   -- 'normal' | 'elevated' | 'swarm'
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, window_end, region, country)
);

-- 4) Daily region rollup -- the historical, analytics-ready table BI tools
-- (Power BI, Metabase, etc.) point at for trend dashboards across days/weeks.
CREATE TABLE IF NOT EXISTS gold.daily_region_summary (
    activity_date          DATE        NOT NULL,
    region                  TEXT        NOT NULL,
    country                  TEXT        NOT NULL,
    event_count              INTEGER     NOT NULL,
    avg_magnitude            NUMERIC(4,2),
    max_magnitude            NUMERIC(4,2),
    significant_event_count  INTEGER    NOT NULL DEFAULT 0,
    tsunami_alert_count      INTEGER    NOT NULL DEFAULT 0,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (activity_date, region, country)
);

CREATE INDEX IF NOT EXISTS ix_gold_daily_date ON gold.daily_region_summary (activity_date);
