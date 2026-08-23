"""
Bronze -> Silver transform logic: parsing raw USGS GeoJSON features into
clean, typed, normalized rows.

The one genuinely messy part of this dataset -- deliberately left messy
by USGS, the same way flight data leaves you parsing free-text airport /
route strings -- is `properties.place`. It comes in wildly inconsistent
shapes, e.g.:

    "10km SSW of Ridgecrest, CA"          -> US: region="Ridgecrest, CA", country="United States"
    "35 km NNW of Sutton-Alpine, Alaska"  -> US: region="Sutton-Alpine, Alaska", country="United States"
    "South Sandwich Islands region"       -> no comma, no country hint -> region as-is, country="International Waters / Unclassified"
    "88 km SSE of Kimbe, Papua New Guinea"-> region="Kimbe, Papua New Guinea", country="Papua New Guinea"
    "Fiji region"                         -> region="Fiji", country="Fiji"

`parse_place` below applies best-effort heuristics and always returns a
non-null (region, country) pair -- falling back to explicit "Unclassified"
buckets rather than raising, because a malformed `place` string must never
crash the pipeline (production data is never fully clean).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

# US state/territory postal abbreviations AND full names -> every one of
# these after the last comma means "this event is domestic to the United
# States". USGS mixes both forms depending on state (observed empirically
# against live data: e.g. "CA" is always abbreviated, but "Texas",
# "Washington", "Utah", "New Mexico", "Louisiana", "Alaska", "Hawaii",
# "Nevada" all appear as full names) -- both forms must be covered.
US_STATE_TOKENS = {
    # Postal abbreviations
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
    # Full state names
    "ALABAMA", "ALASKA", "ARIZONA", "ARKANSAS", "CALIFORNIA", "COLORADO",
    "CONNECTICUT", "DELAWARE", "FLORIDA", "GEORGIA", "HAWAII", "IDAHO",
    "ILLINOIS", "INDIANA", "IOWA", "KANSAS", "KENTUCKY", "LOUISIANA",
    "MAINE", "MARYLAND", "MASSACHUSETTS", "MICHIGAN", "MINNESOTA",
    "MISSISSIPPI", "MISSOURI", "MONTANA", "NEBRASKA", "NEVADA",
    "NEW HAMPSHIRE", "NEW JERSEY", "NEW MEXICO", "NEW YORK",
    "NORTH CAROLINA", "NORTH DAKOTA", "OHIO", "OKLAHOMA", "OREGON",
    "PENNSYLVANIA", "RHODE ISLAND", "SOUTH CAROLINA", "SOUTH DAKOTA",
    "TENNESSEE", "TEXAS", "UTAH", "VERMONT", "VIRGINIA", "WASHINGTON",
    "WEST VIRGINIA", "WISCONSIN", "WYOMING",
    # Territories USGS reports under the US network
    "PUERTO RICO", "CNMI", "GUAM", "AMERICAN SAMOA",
}

# Oceanic / seafloor features and other non-jurisdictional labels USGS uses
# when an event has no meaningful country attribution (mid-ocean ridges,
# open seas, etc). Matched as a whole-string or trailing-word heuristic so
# these don't get misclassified as their own "country".
_OCEANIC_FEATURE_RE = re.compile(
    r"\b(sea|ocean|ridge|trench|rise|basin|gulf of|strait|passage)\b",
    re.IGNORECASE,
)

_DISTANCE_PREFIX_RE = re.compile(
    r"^\s*\d+\s*km\s+[NSEW]{1,3}\s+of\s+", re.IGNORECASE
)


@dataclass
class ParsedPlace:
    region: str
    country: str


def parse_place(place_raw: Optional[str]) -> ParsedPlace:
    """Best-effort normalization of USGS's free-text `place` field into
    (region, country). Never raises; always returns usable strings."""
    if not place_raw or not place_raw.strip():
        return ParsedPlace(region="Unknown", country="Unclassified")

    place = place_raw.strip()
    # Strip a leading "NNkm DIR of " distance prefix, if present.
    place = _DISTANCE_PREFIX_RE.sub("", place)

    if "," in place:
        region_part, _, trailing = place.rpartition(",")
        trailing_clean = trailing.strip()
        region = f"{region_part.strip()}, {trailing_clean}"
        if trailing_clean.upper() in US_STATE_TOKENS:
            country = "United States"
        else:
            # International "City, Country" shape, e.g. "Kimbe, Papua New Guinea"
            country = trailing_clean
        return ParsedPlace(region=region, country=country)

    # No comma: shapes like "Fiji region", "South Sandwich Islands region",
    # "central Alaska", "offshore Guerrero, Mexico" (already handled above
    # if it had a comma). Strip a trailing "region" suffix and treat the
    # remainder as both region and country when it looks like a single
    # place name; otherwise bucket it as unclassified international waters.
    stripped = re.sub(r"\s+region$", "", place, flags=re.IGNORECASE).strip()
    if stripped.upper() in US_STATE_TOKENS or "alaska" in stripped.lower():
        return ParsedPlace(region=stripped, country="United States")
    if _OCEANIC_FEATURE_RE.search(stripped):
        return ParsedPlace(region=stripped or place, country="International Waters / Unclassified")
    if stripped and stripped[0].isupper():
        return ParsedPlace(region=stripped, country=stripped)
    return ParsedPlace(region=stripped or place, country="International Waters / Unclassified")


def _epoch_ms_to_utc(epoch_ms: Optional[int]) -> Optional[datetime]:
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)


@dataclass
class SilverRow:
    event_id: str
    magnitude: Optional[float]
    mag_type: Optional[str]
    place_raw: Optional[str]
    region: str
    country: str
    latitude: float
    longitude: float
    depth_km: Optional[float]
    event_time: datetime
    updated_time: datetime
    event_type: str
    alert_level: Optional[str]
    tsunami_flag: bool
    significance: Optional[int]
    review_status: Optional[str]
    felt_reports: Optional[int]
    source_network: Optional[str]
    bronze_batch_id: str

    def as_tuple(self) -> tuple:
        return (
            self.event_id, self.magnitude, self.mag_type, self.place_raw,
            self.region, self.country, self.latitude, self.longitude,
            self.depth_km, self.event_time, self.updated_time,
            self.event_type, self.alert_level, self.tsunami_flag,
            self.significance, self.review_status, self.felt_reports,
            self.source_network, self.bronze_batch_id,
        )


def feature_to_silver_row(feature: dict[str, Any], bronze_batch_id: str) -> Optional[SilverRow]:
    """Converts one raw GeoJSON feature (as stored in bronze.earthquakes_raw
    .raw_feature) into a SilverRow. Returns None (and lets the caller skip
    + log it) if the record is missing data essential for it to be usable
    at all -- e.g. no coordinates or no event time -- rather than crashing
    the whole batch over one bad record."""
    props = feature.get("properties") or {}
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates") or [None, None, None]

    event_id = feature.get("id")
    event_time = _epoch_ms_to_utc(props.get("time"))
    longitude, latitude = coords[0], coords[1]

    if not event_id or event_time is None or latitude is None or longitude is None:
        return None

    parsed_place = parse_place(props.get("place"))
    updated_time = _epoch_ms_to_utc(props.get("updated")) or event_time

    return SilverRow(
        event_id=event_id,
        magnitude=props.get("mag"),
        mag_type=props.get("magType"),
        place_raw=props.get("place"),
        region=parsed_place.region,
        country=parsed_place.country,
        latitude=float(latitude),
        longitude=float(longitude),
        depth_km=coords[2] if len(coords) > 2 else None,
        event_time=event_time,
        updated_time=updated_time,
        event_type=props.get("type") or "earthquake",
        alert_level=props.get("alert"),
        tsunami_flag=bool(props.get("tsunami")),
        significance=props.get("sig"),
        review_status=props.get("status"),
        felt_reports=props.get("felt"),
        source_network=props.get("net"),
        bronze_batch_id=bronze_batch_id,
    )


UPSERT_SILVER_SQL = """
INSERT INTO silver.earthquakes_clean (
    event_id, magnitude, mag_type, place_raw, region, country,
    latitude, longitude, depth_km, event_time, updated_time,
    event_type, alert_level, tsunami_flag, significance,
    review_status, felt_reports, source_network, bronze_batch_id
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (event_id) DO UPDATE SET
    magnitude       = EXCLUDED.magnitude,
    mag_type        = EXCLUDED.mag_type,
    place_raw       = EXCLUDED.place_raw,
    region          = EXCLUDED.region,
    country         = EXCLUDED.country,
    latitude        = EXCLUDED.latitude,
    longitude       = EXCLUDED.longitude,
    depth_km        = EXCLUDED.depth_km,
    event_time      = EXCLUDED.event_time,
    updated_time    = EXCLUDED.updated_time,
    event_type      = EXCLUDED.event_type,
    alert_level     = EXCLUDED.alert_level,
    tsunami_flag    = EXCLUDED.tsunami_flag,
    significance    = EXCLUDED.significance,
    review_status   = EXCLUDED.review_status,
    felt_reports    = EXCLUDED.felt_reports,
    source_network  = EXCLUDED.source_network,
    bronze_batch_id = EXCLUDED.bronze_batch_id,
    silver_loaded_at = now()
-- Idempotency / out-of-order guard: only overwrite an existing row if the
-- incoming record is a genuinely newer USGS revision. This makes it safe
-- to reprocess an old bronze batch (e.g. a manual backfill) without it
-- clobbering a more recent revision that arrived through a later run.
WHERE EXCLUDED.updated_time >= silver.earthquakes_clean.updated_time;
"""
