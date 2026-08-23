"""
Unit tests for include/transform.py. These test pure Python logic only
(no Postgres, no Airflow, no network) so they run anywhere with just
`pytest` -- e.g. in CI, before ever standing up docker-compose.

Run: pytest tests/ -v
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from include.transform import parse_place, feature_to_silver_row  # noqa: E402


class TestParsePlace:
    def test_us_state_with_distance_prefix(self):
        result = parse_place("10km SSW of Ridgecrest, CA")
        assert result.country == "United States"
        assert "Ridgecrest, CA" in result.region

    def test_us_full_state_name(self):
        result = parse_place("35 km NNW of Sutton-Alpine, Alaska")
        assert result.country == "United States"
        assert "Sutton-Alpine, Alaska" in result.region

    def test_international_city_country(self):
        result = parse_place("88 km SSE of Kimbe, Papua New Guinea")
        assert result.country == "Papua New Guinea"
        assert "Kimbe" in result.region

    def test_region_only_no_comma(self):
        result = parse_place("Fiji region")
        assert result.region == "Fiji"
        assert result.country == "Fiji"

    def test_ambiguous_offshore_region(self):
        result = parse_place("South Sandwich Islands region")
        assert result.region == "South Sandwich Islands"
        # No reliable country signal in this shape -> falls back to itself,
        # not silently misclassified as a real country.
        assert result.country in ("South Sandwich Islands", "International Waters / Unclassified")

    def test_empty_place_never_raises(self):
        result = parse_place("")
        assert result.region == "Unknown"
        assert result.country == "Unclassified"

    def test_none_place_never_raises(self):
        result = parse_place(None)
        assert result.region == "Unknown"
        assert result.country == "Unclassified"

    def test_full_state_name_texas(self):
        # Regression test: found via live-data validation against the USGS
        # feed -- USGS reports some states abbreviated ("CA") and others
        # spelled out ("Texas"), both must map to United States.
        result = parse_place("32 km NNE of Mentone, Texas")
        assert result.country == "United States"

    def test_full_state_name_washington(self):
        result = parse_place("24 km ENE of Ashford, Washington")
        assert result.country == "United States"

    def test_oceanic_ridge_not_misclassified_as_country(self):
        result = parse_place("northern Mid-Atlantic Ridge")
        assert result.country == "International Waters / Unclassified"

    def test_named_sea_not_misclassified_as_country(self):
        result = parse_place("Scotia Sea")
        assert result.country == "International Waters / Unclassified"
        assert result.region == "Scotia Sea"

    def test_us_territory_puerto_rico(self):
        result = parse_place("2 km NW of Guayanilla, Puerto Rico")
        assert result.country == "United States"


class TestFeatureToSilverRow:
    def _sample_feature(self, **overrides):
        feature = {
            "type": "Feature",
            "id": "us7000abcd",
            "properties": {
                "mag": 4.7,
                "place": "112 km SSE of Kimbe, Papua New Guinea",
                "time": 1755000000000,
                "updated": 1755000600000,
                "type": "earthquake",
                "alert": None,
                "tsunami": 0,
                "sig": 340,
                "status": "reviewed",
                "felt": None,
                "net": "us",
                "magType": "mb",
            },
            "geometry": {"type": "Point", "coordinates": [151.9, -5.6, 35.0]},
        }
        feature["properties"].update(overrides)
        return feature

    def test_happy_path(self):
        row = feature_to_silver_row(self._sample_feature(), bronze_batch_id="batch-1")
        assert row is not None
        assert row.event_id == "us7000abcd"
        assert row.magnitude == 4.7
        assert row.country == "Papua New Guinea"
        assert row.latitude == -5.6
        assert row.longitude == 151.9
        assert row.depth_km == 35.0
        assert row.tsunami_flag is False
        assert row.bronze_batch_id == "batch-1"

    def test_missing_coordinates_returns_none(self):
        feature = self._sample_feature()
        feature["geometry"] = {"type": "Point", "coordinates": [None, None, None]}
        row = feature_to_silver_row(feature, bronze_batch_id="batch-1")
        assert row is None

    def test_missing_event_id_returns_none(self):
        feature = self._sample_feature()
        feature["id"] = None
        row = feature_to_silver_row(feature, bronze_batch_id="batch-1")
        assert row is None

    def test_missing_updated_falls_back_to_event_time(self):
        feature = self._sample_feature()
        del feature["properties"]["updated"]
        row = feature_to_silver_row(feature, bronze_batch_id="batch-1")
        assert row is not None
        assert row.updated_time == row.event_time

    def test_tsunami_flag_true(self):
        row = feature_to_silver_row(self._sample_feature(tsunami=1), bronze_batch_id="batch-1")
        assert row.tsunami_flag is True


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
