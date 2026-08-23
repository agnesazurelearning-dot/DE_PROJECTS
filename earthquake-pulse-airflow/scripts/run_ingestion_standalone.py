#!/usr/bin/env python3
"""
Standalone ingestion tester -- fetches a window of live USGS earthquake
data and prints it, with NO Airflow, NO database, and NO Docker required.

Two uses:
  1. Pre-flight check before a demo: confirm your network can actually
     reach earthquake.usgs.gov and see real data, in about 5 seconds,
     before you're on camera / on a call.
  2. Development: eyeball real payloads while iterating on
     include/transform.py's place-parsing logic.

Usage:
    python3 scripts/run_ingestion_standalone.py
    python3 scripts/run_ingestion_standalone.py --hours-ago 6 --min-magnitude 2.5
    python3 scripts/run_ingestion_standalone.py --save sample.json
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from include.transform import feature_to_silver_row  # noqa: E402
from include.usgs_client import build_request_url, fetch_earthquakes  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--hours-ago", type=float, default=1.0, help="Window size in hours, ending now (default: 1)")
    parser.add_argument("--min-magnitude", type=float, default=0.0)
    parser.add_argument("--save", type=str, default=None, help="Optional path to save the raw JSON response")
    args = parser.parse_args()

    window_end = datetime.now(timezone.utc)
    window_start = window_end - timedelta(hours=args.hours_ago)

    url = build_request_url(window_start, window_end, args.min_magnitude)
    print(f"Fetching: {url}\n")

    payload = fetch_earthquakes(window_start, window_end, min_magnitude=args.min_magnitude)
    features = payload.get("features", [])
    print(f"{len(features)} events in [{window_start.isoformat()}, {window_end.isoformat()})\n")

    skipped = 0
    for feature in features[:15]:
        row = feature_to_silver_row(feature, bronze_batch_id="manual-test")
        if row is None:
            skipped += 1
            print(f"  SKIPPED (malformed): {feature.get('id')}")
            continue
        mag = f"M{row.magnitude:.1f}" if row.magnitude is not None else "M?.?"
        print(f"  {mag:<6} {row.region:<38} {row.country:<28} sig={row.significance}")

    if len(features) > 15:
        print(f"  ... and {len(features) - 15} more")
    if skipped:
        print(f"\n{skipped} record(s) skipped as malformed (this is expected/normal, not an error)")

    if args.save:
        with open(args.save, "w") as fh:
            json.dump(payload, fh, indent=2)
        print(f"\nSaved raw response to {args.save}")


if __name__ == "__main__":
    main()
