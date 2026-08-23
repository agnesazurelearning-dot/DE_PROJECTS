"""
Silver -> Gold aggregation SQL. Every statement is a pure UPSERT keyed on
its table's natural grain (see sql/03_create_gold.sql), so re-running a
DAG run for the same window is safe and produces the same result --
recompute, not append.

All aggregation is pushed down to Postgres (set-based SQL) rather than
pulled into Python/pandas: it's faster at this volume, and it keeps the
DAG's Python footprint small and easy to reason about, which is closer to
how this would actually be built in production (dbt-on-Postgres would be
the natural next evolution -- see README "Extending this project").
"""
from __future__ import annotations

UPSERT_REGION_ACTIVITY_WINDOW = """
INSERT INTO gold.region_activity_window (
    window_start, window_end, region, country, event_count,
    avg_magnitude, max_magnitude, min_magnitude,
    significant_event_count, strongest_event_id, tsunami_alert_count,
    avg_depth_km
)
SELECT
    %(window_start)s AS window_start,
    %(window_end)s   AS window_end,
    s.region,
    s.country,
    COUNT(*)                                              AS event_count,
    ROUND(AVG(s.magnitude), 2)                            AS avg_magnitude,
    MAX(s.magnitude)                                       AS max_magnitude,
    MIN(s.magnitude)                                       AS min_magnitude,
    COUNT(*) FILTER (WHERE s.is_significant)               AS significant_event_count,
    (ARRAY_AGG(s.event_id ORDER BY s.magnitude DESC NULLS LAST))[1] AS strongest_event_id,
    COUNT(*) FILTER (WHERE s.tsunami_flag)                 AS tsunami_alert_count,
    ROUND(AVG(s.depth_km), 2)                              AS avg_depth_km
FROM silver.earthquakes_clean s
WHERE s.event_time >= %(window_start)s AND s.event_time < %(window_end)s
GROUP BY s.region, s.country
ON CONFLICT (window_start, window_end, region, country) DO UPDATE SET
    event_count              = EXCLUDED.event_count,
    avg_magnitude             = EXCLUDED.avg_magnitude,
    max_magnitude             = EXCLUDED.max_magnitude,
    min_magnitude             = EXCLUDED.min_magnitude,
    significant_event_count   = EXCLUDED.significant_event_count,
    strongest_event_id        = EXCLUDED.strongest_event_id,
    tsunami_alert_count       = EXCLUDED.tsunami_alert_count,
    avg_depth_km              = EXCLUDED.avg_depth_km,
    updated_at                = now();
"""

UPSERT_GLOBAL_SUMMARY_WINDOW = """
INSERT INTO gold.global_summary_window (
    window_start, window_end, total_events, distinct_regions,
    avg_magnitude, max_magnitude, significant_event_count, tsunami_alert_count
)
SELECT
    %(window_start)s AS window_start,
    %(window_end)s   AS window_end,
    COUNT(*)                                  AS total_events,
    COUNT(DISTINCT s.region)                  AS distinct_regions,
    ROUND(AVG(s.magnitude), 2)                AS avg_magnitude,
    MAX(s.magnitude)                          AS max_magnitude,
    COUNT(*) FILTER (WHERE s.is_significant)  AS significant_event_count,
    COUNT(*) FILTER (WHERE s.tsunami_flag)    AS tsunami_alert_count
FROM silver.earthquakes_clean s
WHERE s.event_time >= %(window_start)s AND s.event_time < %(window_end)s
ON CONFLICT (window_start, window_end) DO UPDATE SET
    total_events             = EXCLUDED.total_events,
    distinct_regions         = EXCLUDED.distinct_regions,
    avg_magnitude             = EXCLUDED.avg_magnitude,
    max_magnitude             = EXCLUDED.max_magnitude,
    significant_event_count   = EXCLUDED.significant_event_count,
    tsunami_alert_count       = EXCLUDED.tsunami_alert_count,
    updated_at                = now();
"""

# Compares this run's per-region event_count against each region's trailing
# 7-day average count for windows of the *same length*, so an hourly DAG
# run is always compared against a baseline of other hourly windows (not
# skewed by comparing 1 hour of activity to a 7-day total).
UPSERT_SWARM_SIGNALS = """
WITH current_window AS (
    SELECT region, country, COUNT(*) AS event_count
    FROM silver.earthquakes_clean
    WHERE event_time >= %(window_start)s AND event_time < %(window_end)s
    GROUP BY region, country
),
baseline AS (
    SELECT
        region,
        country,
        COUNT(*) / 7.0 AS baseline_avg_count
    FROM silver.earthquakes_clean
    WHERE event_time >= %(window_start)s - INTERVAL '7 days'
      AND event_time < %(window_start)s
    GROUP BY region, country
)
INSERT INTO gold.swarm_signals (
    window_start, window_end, region, country, event_count,
    baseline_avg_count, spike_ratio, signal_level
)
SELECT
    %(window_start)s,
    %(window_end)s,
    c.region,
    c.country,
    c.event_count,
    COALESCE(b.baseline_avg_count, 0),
    ROUND(c.event_count / NULLIF(COALESCE(b.baseline_avg_count, 0), 0), 2),
    CASE
        WHEN COALESCE(b.baseline_avg_count, 0) = 0 AND c.event_count > 0 THEN 'elevated'
        WHEN c.event_count >= %(swarm_ratio)s * COALESCE(b.baseline_avg_count, 0)
             AND c.event_count >= 3 THEN 'swarm'
        WHEN c.event_count > COALESCE(b.baseline_avg_count, 0) THEN 'elevated'
        ELSE 'normal'
    END
FROM current_window c
LEFT JOIN baseline b USING (region, country)
ON CONFLICT (window_start, window_end, region, country) DO UPDATE SET
    event_count          = EXCLUDED.event_count,
    baseline_avg_count    = EXCLUDED.baseline_avg_count,
    spike_ratio           = EXCLUDED.spike_ratio,
    signal_level           = EXCLUDED.signal_level,
    updated_at             = now();
"""

# Daily rollup is recomputed for every UTC calendar date touched by this
# run's window (usually one date; two if a run straddles midnight UTC).
UPSERT_DAILY_REGION_SUMMARY = """
INSERT INTO gold.daily_region_summary (
    activity_date, region, country, event_count, avg_magnitude,
    max_magnitude, significant_event_count, tsunami_alert_count
)
SELECT
    s.event_time::date AS activity_date,
    s.region,
    s.country,
    COUNT(*)                                 AS event_count,
    ROUND(AVG(s.magnitude), 2)               AS avg_magnitude,
    MAX(s.magnitude)                         AS max_magnitude,
    COUNT(*) FILTER (WHERE s.is_significant) AS significant_event_count,
    COUNT(*) FILTER (WHERE s.tsunami_flag)   AS tsunami_alert_count
FROM silver.earthquakes_clean s
WHERE s.event_time::date >= (%(window_start)s)::date
  AND s.event_time::date <= (%(window_end)s)::date
GROUP BY s.event_time::date, s.region, s.country
ON CONFLICT (activity_date, region, country) DO UPDATE SET
    event_count              = EXCLUDED.event_count,
    avg_magnitude             = EXCLUDED.avg_magnitude,
    max_magnitude             = EXCLUDED.max_magnitude,
    significant_event_count   = EXCLUDED.significant_event_count,
    tsunami_alert_count       = EXCLUDED.tsunami_alert_count,
    updated_at                = now();
"""
