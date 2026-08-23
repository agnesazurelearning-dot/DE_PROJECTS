"""
Thin client around the USGS FDSNWS earthquake "query" endpoint.

Why this endpoint and not the rolling summary feeds
(`.../summary/all_hour.geojson` etc.): the summary feeds only ever show
"now minus N", which makes idempotent, time-windowed backfills impossible
-- you cannot ask them for "everything between 09:00 and 10:00 UTC
yesterday". The FDSNWS query endpoint takes explicit starttime/endtime
parameters, which lets every DAG run request an exact, reproducible slice
of the catalog keyed off Airflow's `data_interval_start` /
`data_interval_end`. Re-running the same data interval always returns the
same rows (modulo USGS revisions, which `updated` timestamps handle --
see include/transform.py) -- exactly the idempotency property a
production pipeline needs.

Docs: https://earthquake.usgs.gov/fdsnws/event/1/
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USGS_QUERY_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

logger = logging.getLogger(__name__)


def _session_with_retries(total: int = 3, backoff_factor: float = 1.5) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=total,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_earthquakes(
    start: datetime,
    end: datetime,
    min_magnitude: float = 0.0,
    timeout: int = 30,
) -> dict[str, Any]:
    """
    Pull every earthquake event USGS has recorded in [start, end) as raw
    GeoJSON. `start`/`end` should be timezone-aware UTC datetimes -- pass
    Airflow's `data_interval_start` / `data_interval_end` directly.

    Raises `requests.HTTPError` on a non-2xx response after retries are
    exhausted, so the calling Airflow task fails cleanly and Airflow's own
    retry/alerting takes over.
    """
    params = {
        "format": "geojson",
        "starttime": start.strftime("%Y-%m-%dT%H:%M:%S"),
        "endtime": end.strftime("%Y-%m-%dT%H:%M:%S"),
        "minmagnitude": min_magnitude,
        "orderby": "time-asc",
    }
    session = _session_with_retries()
    logger.info("Fetching USGS earthquakes window=%s..%s min_mag=%s", start, end, min_magnitude)
    response = session.get(USGS_QUERY_URL, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    logger.info(
        "USGS returned %s features for window=%s..%s",
        payload.get("metadata", {}).get("count", len(payload.get("features", []))),
        start,
        end,
    )
    return payload


def build_request_url(start: datetime, end: datetime, min_magnitude: float = 0.0) -> str:
    """Rebuilds the exact URL fetch_earthquakes() would call, for audit logging."""
    req = requests.Request(
        "GET",
        USGS_QUERY_URL,
        params={
            "format": "geojson",
            "starttime": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "endtime": end.strftime("%Y-%m-%dT%H:%M:%S"),
            "minmagnitude": min_magnitude,
            "orderby": "time-asc",
        },
    )
    return req.prepare().url
